package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestRevisionLifecycle(t *testing.T) {
	t.Parallel()

	store := openTestRegistry(t)
	ctx := context.Background()

	r1, noChange, err := store.Prepare(ctx, "image.generate", KindService, []byte(`{"timeout":"1s"}`), "hash-1")
	if err != nil || noChange || r1.Number != 1 || r1.State != RevisionStarting {
		t.Fatalf("prepare r1 = %#v noChange=%v err=%v", r1, noChange, err)
	}
	if err := store.Activate(ctx, "image.generate", 1); err != nil {
		t.Fatalf("activate r1: %v", err)
	}

	same, noChange, err := store.Prepare(ctx, "image.generate", KindService, []byte(`{"timeout":"1s"}`), "hash-1")
	if err != nil || !noChange || same.Number != 1 {
		t.Fatalf("same config = %#v noChange=%v err=%v", same, noChange, err)
	}

	r2, noChange, err := store.Prepare(ctx, "image.generate", KindService, []byte(`{"timeout":"2s"}`), "hash-2")
	if err != nil || noChange || r2.Number != 2 {
		t.Fatalf("prepare r2 = %#v noChange=%v err=%v", r2, noChange, err)
	}
	beforeSwitch := getService(t, store, "image.generate")
	if beforeSwitch.CurrentRevision != 1 || beforeSwitch.Current.Hash != "hash-1" {
		t.Fatalf("current changed before activate: %#v", beforeSwitch)
	}
	starting, err := store.ListStarting(ctx)
	if err != nil || len(starting) != 1 || starting[0].Number != 2 {
		t.Fatalf("starting revisions = %#v err=%v", starting, err)
	}
	if err := store.MarkSwitchStarted(ctx, "image.generate", 2); err != nil {
		t.Fatalf("mark switch started: %v", err)
	}
	if switching := getRevision(t, store, "image.generate", 2); !switching.SwitchStarted {
		t.Fatalf("switch_started = false: %#v", switching)
	}
	services, err := store.List(ctx)
	if err != nil || len(services) != 1 || services[0].CurrentRevision != 1 {
		t.Fatalf("services = %#v err=%v", services, err)
	}
	if err := store.Activate(ctx, "image.generate", 2); err != nil {
		t.Fatalf("activate r2: %v", err)
	}
	old := getRevision(t, store, "image.generate", 1)
	if old.State != RevisionDraining {
		t.Fatalf("r1 state = %q, want draining", old.State)
	}

	r3, noChange, err := store.Prepare(ctx, "image.generate", KindTombstone, nil, "")
	if err != nil || noChange || r3.Number != 3 {
		t.Fatalf("prepare tombstone = %#v noChange=%v err=%v", r3, noChange, err)
	}
	if err := store.Activate(ctx, "image.generate", 3); err != nil {
		t.Fatalf("activate tombstone: %v", err)
	}
	deleted := getService(t, store, "image.generate")
	if deleted.State != ServiceDeleted || deleted.Current.Kind != KindTombstone {
		t.Fatalf("deleted service = %#v", deleted)
	}
	_, noChange, err = store.Prepare(ctx, "image.generate", KindTombstone, nil, "")
	if err != nil || !noChange {
		t.Fatalf("second delete noChange=%v err=%v", noChange, err)
	}
}

func TestFailedRevisionDoesNotReplaceCurrent(t *testing.T) {
	t.Parallel()

	store := openTestRegistry(t)
	ctx := context.Background()
	r1, _, err := store.Prepare(ctx, "service-a", KindService, []byte(`{"v":1}`), "hash-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Activate(ctx, "service-a", r1.Number); err != nil {
		t.Fatal(err)
	}
	r2, _, err := store.Prepare(ctx, "service-a", KindService, []byte(`{"v":2}`), "hash-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFailed(ctx, "service-a", r2.Number); err != nil {
		t.Fatal(err)
	}

	service := getService(t, store, "service-a")
	if service.CurrentRevision != 1 || service.State != ServiceActive {
		t.Fatalf("service after failed update = %#v", service)
	}
	if failed := getRevision(t, store, "service-a", 2); failed.State != RevisionFailed {
		t.Fatalf("failed revision state = %q", failed.State)
	}
}

func TestListJoinsCurrentAndLatestRevisions(t *testing.T) {
	t.Parallel()

	store := openTestRegistry(t)
	ctx := context.Background()
	current, _, err := store.Prepare(ctx, "service-a", KindService, []byte(`{"v":1}`), "hash-a1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Activate(ctx, "service-a", current.Number); err != nil {
		t.Fatal(err)
	}
	latest, _, err := store.Prepare(ctx, "service-a", KindService, []byte(`{"v":2}`), "hash-a2")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFailed(ctx, "service-a", latest.Number); err != nil {
		t.Fatal(err)
	}
	failed, _, err := store.Prepare(ctx, "service-b", KindService, []byte(`{"v":1}`), "hash-b1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFailed(ctx, "service-b", failed.Number); err != nil {
		t.Fatal(err)
	}

	services, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 2 {
		t.Fatalf("services = %#v", services)
	}
	if service := services[0]; service.ID != "service-a" || service.Current.Number != 1 || service.Current.Hash != "hash-a1" || service.Latest.Number != 2 || service.Latest.Hash != "hash-a2" {
		t.Fatalf("service-a = %#v", service)
	}
	if service := services[1]; service.ID != "service-b" || service.CurrentRevision != 0 || service.Latest.Number != 1 || service.Latest.State != RevisionFailed {
		t.Fatalf("service-b = %#v", service)
	}
}

func TestConcurrentListReturnsConsistentJoinedRevisionSnapshots(t *testing.T) {
	t.Parallel()

	store := openTestRegistry(t)
	const writers = 12
	start := make(chan struct{})
	var errorsMu sync.Mutex
	var errorsFound []error
	recordError := func(err error) {
		errorsMu.Lock()
		defer errorsMu.Unlock()
		errorsFound = append(errorsFound, err)
	}
	var writerGroup, readerGroup sync.WaitGroup
	writerGroup.Add(writers)
	for i := range writers {
		serviceID := fmt.Sprintf("service-%02d", i)
		go func() {
			defer writerGroup.Done()
			<-start
			ctx := context.Background()
			revision, _, err := store.Prepare(ctx, serviceID, KindService, []byte(`{"v":1}`), serviceID+"-r1")
			if err != nil {
				recordError(fmt.Errorf("prepare %s: %w", serviceID, err))
				return
			}
			if err := store.Activate(ctx, serviceID, revision.Number); err != nil {
				recordError(fmt.Errorf("activate %s: %w", serviceID, err))
				return
			}
			latest, _, err := store.Prepare(ctx, serviceID, KindService, []byte(`{"v":2}`), serviceID+"-r2")
			if err != nil {
				recordError(fmt.Errorf("prepare update %s: %w", serviceID, err))
				return
			}
			if err := store.MarkFailed(ctx, serviceID, latest.Number); err != nil {
				recordError(fmt.Errorf("fail update %s: %w", serviceID, err))
			}
		}()
	}
	for range 3 {
		readerGroup.Add(1)
		go func() {
			defer readerGroup.Done()
			<-start
			for range 40 {
				services, err := store.List(context.Background())
				if err != nil {
					recordError(fmt.Errorf("list: %w", err))
					continue
				}
				for _, service := range services {
					if service.CurrentRevision == 0 {
						if service.State != ServiceStarting || service.Latest.State != RevisionStarting {
							recordError(fmt.Errorf("inconsistent initial snapshot: %#v", service))
						}
						continue
					}
					if service.Current.Number != service.CurrentRevision || service.Current.State != RevisionActive || service.State != ServiceActive || service.Latest.Number < service.CurrentRevision {
						recordError(fmt.Errorf("inconsistent joined snapshot: %#v", service))
					}
				}
			}
		}()
	}
	close(start)
	writerGroup.Wait()
	readerGroup.Wait()
	for _, err := range errorsFound {
		t.Error(err)
	}

	services, err := store.List(context.Background())
	if err != nil || len(services) != writers {
		t.Fatalf("final List returned %d services, err=%v", len(services), err)
	}
	for i, service := range services {
		if service.ID != fmt.Sprintf("service-%02d", i) || service.State != ServiceActive || service.CurrentRevision != 1 || service.Latest.Number != 2 || service.Latest.State != RevisionFailed {
			t.Fatalf("final service %d = %#v", i, service)
		}
	}
}

func TestTombstoneMissingService(t *testing.T) {
	t.Parallel()

	store := openTestRegistry(t)
	_, _, err := store.Prepare(context.Background(), "missing", KindTombstone, nil, "")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

func TestInvalidRevisionTransitions(t *testing.T) {
	t.Parallel()

	store := openTestRegistry(t)
	ctx := context.Background()
	revision, _, err := store.Prepare(ctx, "service-a", KindService, []byte(`{}`), "hash")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFailed(ctx, "service-a", revision.Number); err != nil {
		t.Fatal(err)
	}
	failedService := getService(t, store, "service-a")
	if failedService.State != ServiceFailed || failedService.CurrentRevision != 0 || failedService.Latest.State != RevisionFailed {
		t.Fatalf("failed initial service = %#v", failedService)
	}
	if err := store.Activate(ctx, "service-a", revision.Number); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("activate failed revision error = %v", err)
	}
	if err := store.MarkSwitchStarted(ctx, "service-a", revision.Number); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("mark failed revision switching error = %v", err)
	}
	if _, err := store.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing error = %v", err)
	}
}

func TestMigrationTreatsExistingStartingRevisionAsSwitchAttempted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE services (
			id TEXT PRIMARY KEY, state TEXT NOT NULL, current_revision INTEGER,
			created_at TEXT NOT NULL, updated_at TEXT NOT NULL
		);
		CREATE TABLE service_revisions (
			service_id TEXT NOT NULL, revision INTEGER NOT NULL, kind TEXT NOT NULL,
			state TEXT NOT NULL, spec_json BLOB, config_hash TEXT NOT NULL,
			created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
			PRIMARY KEY (service_id, revision)
		);
		INSERT INTO services VALUES ('service-a', 'starting', NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
		INSERT INTO service_revisions VALUES ('service-a', 1, 'service', 'starting', '{}', 'hash', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
	`)
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if revision := getRevision(t, store, "service-a", 1); !revision.SwitchStarted {
		t.Fatalf("migrated revision = %#v", revision)
	}
}

func openTestRegistry(t *testing.T) *Registry {
	t.Helper()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func getService(t *testing.T, store *Registry, id string) Service {
	t.Helper()

	service, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	return service
}

func getRevision(t *testing.T, store *Registry, id string, revision int64) Revision {
	t.Helper()

	result, err := store.GetRevision(context.Background(), id, revision)
	if err != nil {
		t.Fatalf("get revision: %v", err)
	}
	return result
}
