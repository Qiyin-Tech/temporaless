package registry

import (
	"context"
	"errors"
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
	if _, err := store.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing error = %v", err)
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
