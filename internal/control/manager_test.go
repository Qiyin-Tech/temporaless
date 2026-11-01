package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"temporaless"
	"temporaless/internal/backend"
	"temporaless/internal/registry"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
)

func TestManagerPutUpdateDeleteLifecycle(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	runtime := &fakeRuntime{activities: make(map[string]Activity)}
	manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	t.Cleanup(manager.Close)

	first, err := manager.Put(context.Background(), "image-generate", serviceSpec("https://example.com/v1"))
	if err != nil {
		t.Fatalf("put r1: %v", err)
	}
	if first.CurrentRevision != 1 || first.State != registry.ServiceActive {
		t.Fatalf("r1 view = %#v", first)
	}

	if _, err := manager.Put(context.Background(), "image-generate", serviceSpec("https://example.com/v1")); err != nil {
		t.Fatalf("idempotent put: %v", err)
	}
	if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1", "current:r1"}) {
		t.Fatalf("events after no-op = %#v", got)
	}

	updated, err := manager.Put(context.Background(), "image-generate", serviceSpec("https://example.com/v2"))
	if err != nil {
		t.Fatalf("put r2: %v", err)
	}
	if updated.CurrentRevision != 2 {
		t.Fatalf("updated revision = %d", updated.CurrentRevision)
	}
	if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{
		"start:r1", "current:r1", "start:r2", "current:r2", "stop:r1",
	}) {
		t.Fatalf("update events = %#v", got)
	}

	deleted, err := manager.Delete(context.Background(), "image-generate")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if deleted.State != registry.ServiceDeleted || deleted.CurrentRevision != 3 {
		t.Fatalf("deleted view = %#v", deleted)
	}
	_, err = runtime.activity("r3")(context.Background(), temporaless.Request{Action: "invoke"})
	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) || applicationError.Type() != "ServiceDeleted" || !applicationError.NonRetryable() {
		t.Fatalf("tombstone error = %T %v", err, err)
	}
}

func TestManagerConcurrentMutationsRemainLinearizable(t *testing.T) {
	t.Parallel()

	t.Run("idempotent puts create one revision", func(t *testing.T) {
		store := openRegistry(t)
		runtime := &fakeRuntime{activities: make(map[string]Activity)}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
		t.Cleanup(manager.Close)

		const callers = 16
		start := make(chan struct{})
		results := make(chan struct {
			view ServiceView
			err  error
		}, callers)
		for range callers {
			go func() {
				<-start
				view, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v1"))
				results <- struct {
					view ServiceView
					err  error
				}{view, err}
			}()
		}
		close(start)
		for range callers {
			result := <-results
			if result.err != nil || result.view.CurrentRevision != 1 {
				t.Fatalf("put = %#v, %v", result.view, result.err)
			}
		}
		if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1", "current:r1"}) {
			t.Fatalf("events = %#v", got)
		}
	})

	t.Run("put and delete have a total order", func(t *testing.T) {
		store := openRegistry(t)
		runtime := &fakeRuntime{activities: make(map[string]Activity)}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
		t.Cleanup(manager.Close)
		if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v1")); err != nil {
			t.Fatal(err)
		}

		type result struct {
			operation string
			view      ServiceView
			err       error
		}
		start := make(chan struct{})
		results := make(chan result, 2)
		go func() {
			<-start
			view, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v2"))
			results <- result{"put", view, err}
		}()
		go func() {
			<-start
			view, err := manager.Delete(context.Background(), "service-a")
			results <- result{"delete", view, err}
		}()
		close(start)

		byRevision := make(map[int64]result, 2)
		for range 2 {
			result := <-results
			if result.err != nil {
				t.Fatalf("%s: %v", result.operation, result.err)
			}
			byRevision[result.view.CurrentRevision] = result
		}
		if len(byRevision) != 2 || byRevision[2].operation == "" || byRevision[3].operation == "" {
			t.Fatalf("results by revision = %#v", byRevision)
		}
		if byRevision[2].operation == "put" && byRevision[2].view.State != registry.ServiceActive {
			t.Fatalf("put state = %q", byRevision[2].view.State)
		}
		if byRevision[2].operation == "delete" && byRevision[2].view.State != registry.ServiceDeleted {
			t.Fatalf("delete state = %q", byRevision[2].view.State)
		}
		final := getStoredService(t, store, "service-a")
		if final.CurrentRevision != 3 || final.State != byRevision[3].view.State {
			t.Fatalf("final = %#v, last result = %#v", final, byRevision[3])
		}
	})
}

func TestManagerReadsRemainAvailableDuringWorkerSwitch(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	base := &fakeRuntime{activities: make(map[string]Activity)}
	runtime := &gatedSetCurrentRuntime{fakeRuntime: base, entered: make(chan struct{}), release: make(chan struct{})}
	manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	t.Cleanup(manager.Close)

	putResult := make(chan error, 1)
	go func() {
		_, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v1"))
		putResult <- err
	}()
	select {
	case <-runtime.entered:
	case <-time.After(time.Second):
		t.Fatal("PUT did not reach SetCurrent")
	}

	service, err := manager.Get(context.Background(), "service-a")
	if err != nil || service.State != registry.ServiceStarting || service.CurrentRevision != 0 || service.LatestRevision != 1 {
		t.Fatalf("GET during switch = %#v, %v", service, err)
	}
	services, err := manager.List(context.Background())
	if err != nil || len(services) != 1 || services[0].LatestRevisionState != registry.RevisionStarting {
		t.Fatalf("LIST during switch = %#v, %v", services, err)
	}
	if err := manager.Health(context.Background()); err != nil {
		t.Fatalf("health during switch: %v", err)
	}
	close(runtime.release)
	if err := <-putResult; err != nil {
		t.Fatalf("PUT after switch release: %v", err)
	}

	stored := getStoredService(t, store, "service-a")
	if stored.State != registry.ServiceActive || stored.CurrentRevision != 1 || stored.Current.State != registry.RevisionActive || stored.Latest.Number != 1 {
		t.Fatalf("final service after switch = %#v", stored)
	}
}

func TestManagerConcurrentDifferentServicesKeepRevisionStateIsolated(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	runtime := &fakeRuntime{activities: make(map[string]Activity)}
	manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	t.Cleanup(manager.Close)

	const services = 12
	start := make(chan struct{})
	results := make(chan error, services)
	for i := range services {
		serviceID := fmt.Sprintf("service-%02d", i)
		go func() {
			<-start
			_, err := manager.Put(context.Background(), serviceID, serviceSpec("https://example.com/"+serviceID))
			results <- err
		}()
	}
	close(start)
	for range services {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}

	listed, err := store.List(context.Background())
	if err != nil || len(listed) != services {
		t.Fatalf("services = %d, err=%v", len(listed), err)
	}
	for i, service := range listed {
		if service.ID != fmt.Sprintf("service-%02d", i) || service.CurrentRevision != 1 || service.Latest.Number != 1 || service.Current.Hash != service.Latest.Hash || service.State != registry.ServiceActive {
			t.Fatalf("service %d state crossed between concurrent operations: %#v", i, service)
		}
	}
}

func TestManagerRejectsCanceledMutationAfterWaitingForLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prepare   func(*testing.T, *Manager)
		mutate    func(context.Context, *Manager) error
		assertion func(*testing.T, *registry.Registry)
	}{
		{
			name:    "put",
			prepare: func(*testing.T, *Manager) {},
			mutate: func(ctx context.Context, manager *Manager) error {
				_, err := manager.Put(ctx, "service-a", serviceSpec("https://example.com/v1"))
				return err
			},
			assertion: func(t *testing.T, store *registry.Registry) {
				if _, err := store.Get(context.Background(), "service-a"); !errors.Is(err, registry.ErrNotFound) {
					t.Fatalf("stored service error = %v, want ErrNotFound", err)
				}
			},
		},
		{
			name: "delete",
			prepare: func(t *testing.T, manager *Manager) {
				if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v1")); err != nil {
					t.Fatal(err)
				}
			},
			mutate: func(ctx context.Context, manager *Manager) error {
				_, err := manager.Delete(ctx, "service-a")
				return err
			},
			assertion: func(t *testing.T, store *registry.Registry) {
				service := getStoredService(t, store, "service-a")
				if service.CurrentRevision != 1 || service.State != registry.ServiceActive {
					t.Fatalf("service after canceled delete = %#v", service)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openRegistry(t)
			manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
			t.Cleanup(manager.Close)
			test.prepare(t, manager)

			manager.opMu.Lock()
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- test.mutate(ctx, manager) }()
			cancel()
			manager.opMu.Unlock()

			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("mutation error = %v, want context canceled", err)
			}
			test.assertion(t, store)
		})
	}
}

func TestManagerStartFailureLeavesCurrentRevision(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	runtime := &fakeRuntime{activities: make(map[string]Activity)}
	manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	t.Cleanup(manager.Close)

	if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v1")); err != nil {
		t.Fatal(err)
	}
	runtime.failStart = "r2"
	if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v2")); err == nil {
		t.Fatal("put r2 succeeded, want start failure")
	}
	service, err := store.Get(context.Background(), "service-a")
	if err != nil {
		t.Fatal(err)
	}
	if service.CurrentRevision != 1 || service.Latest.State != registry.RevisionFailed {
		t.Fatalf("service after failure = %#v", service)
	}
}

func TestManagerReconcilesPersistedStartingRevision(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	compiled, err := backend.Compile(serviceSpec("https://example.com/v1"), http.DefaultClient, backend.DefaultMaxResponseBodyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Prepare(context.Background(), "service-a", registry.KindService, compiled.SpecJSON(), compiled.Hash()); err != nil {
		t.Fatal(err)
	}

	runtime := &fakeRuntime{activities: make(map[string]Activity)}
	manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	t.Cleanup(manager.Close)
	if err := manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	service, err := store.Get(context.Background(), "service-a")
	if err != nil {
		t.Fatal(err)
	}
	if service.CurrentRevision != 1 || service.State != registry.ServiceActive {
		t.Fatalf("reconciled service = %#v", service)
	}
}

func TestManagerRestoresCurrentRevision(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	firstRuntime := &fakeRuntime{activities: make(map[string]Activity)}
	firstManager := NewManager(store, firstRuntime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	if _, err := firstManager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v1")); err != nil {
		t.Fatal(err)
	}
	firstManager.Close()

	restartedRuntime := &fakeRuntime{activities: make(map[string]Activity)}
	restartedManager := NewManager(store, restartedRuntime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	t.Cleanup(restartedManager.Close)
	if err := restartedManager.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := restartedRuntime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1", "current:r1"}) {
		t.Fatalf("restart events = %#v", got)
	}
}

func TestManagerReconcilePreservesSafeRouting(t *testing.T) {
	t.Parallel()

	t.Run("invalid stored service is isolated", func(t *testing.T) {
		store := openRegistry(t)
		if _, _, err := store.Prepare(context.Background(), "service-a", registry.KindService, []byte(`{`), "invalid"); err != nil {
			t.Fatal(err)
		}
		runtime := &fakeRuntime{activities: make(map[string]Activity)}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
		t.Cleanup(manager.Close)

		if err := manager.Reconcile(context.Background()); err != nil {
			t.Fatalf("reconcile isolated service failure: %v", err)
		}
		service := getStoredService(t, store, "service-a")
		if service.State != registry.ServiceFailed || service.Latest.State != registry.RevisionFailed {
			t.Fatalf("service = %#v", service)
		}
		if got := runtime.eventsSnapshot(); len(got) != 0 {
			t.Fatalf("events = %#v", got)
		}
	})

	t.Run("initial unknown switch remains unresolved", func(t *testing.T) {
		store := openRegistry(t)
		compiled, err := backend.Compile(serviceSpec("https://example.com/v1"), http.DefaultClient, backend.DefaultMaxResponseBodyBytes)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.Prepare(context.Background(), "service-a", registry.KindService, compiled.SpecJSON(), compiled.Hash()); err != nil {
			t.Fatal(err)
		}
		runtime := &fakeRuntime{
			activities:   make(map[string]Activity),
			setError:     serviceerror.NewNotFound("response lost"),
			currentError: errors.New("Temporal unavailable"),
		}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: 10 * time.Millisecond, RetryInterval: time.Millisecond})
		t.Cleanup(manager.Close)

		if err := manager.Reconcile(context.Background()); err == nil {
			t.Fatal("reconcile succeeded with an unresolved initial switch")
		}
		service := getStoredService(t, store, "service-a")
		if service.CurrentRevision != 0 || service.Latest.State != registry.RevisionStarting {
			t.Fatalf("service = %#v", service)
		}
	})

	t.Run("unknown switch result does not roll back", func(t *testing.T) {
		store := activeServiceWithStartingUpdate(t)
		runtime := &fakeRuntime{
			activities:   make(map[string]Activity),
			setError:     serviceerror.NewNotFound("response lost"),
			currentError: errors.New("Temporal unavailable"),
		}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: 10 * time.Millisecond, RetryInterval: time.Millisecond})
		t.Cleanup(manager.Close)

		if err := manager.Reconcile(context.Background()); err == nil {
			t.Fatal("reconcile succeeded, want unknown switch result")
		}
		if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1", "start:r2"}) {
			t.Fatalf("events = %#v", got)
		}
		service := getStoredService(t, store, "service-a")
		if service.CurrentRevision != 1 || service.Latest.State != registry.RevisionStarting {
			t.Fatalf("service = %#v", service)
		}
	})

	t.Run("known start failure restores current", func(t *testing.T) {
		store := activeServiceWithStartingUpdate(t)
		runtime := &fakeRuntime{activities: make(map[string]Activity), failStart: "r2"}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
		t.Cleanup(manager.Close)

		if err := manager.Reconcile(context.Background()); err != nil {
			t.Fatalf("reconcile recovered a known revision failure: %v", err)
		}
		if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1", "current:r1"}) {
			t.Fatalf("events = %#v", got)
		}
	})

	t.Run("attempted switch start failure cannot restore old current", func(t *testing.T) {
		store := activeServiceWithStartingUpdate(t)
		if err := store.MarkSwitchStarted(context.Background(), "service-a", 2); err != nil {
			t.Fatal(err)
		}
		runtime := &fakeRuntime{activities: make(map[string]Activity), current: "r2", failStart: "r2"}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
		t.Cleanup(manager.Close)

		if err := manager.Reconcile(context.Background()); err == nil {
			t.Fatal("reconcile succeeded with an unserved current version")
		}
		if runtime.current != "r2" {
			t.Fatalf("current rolled back to %q", runtime.current)
		}
		service := getStoredService(t, store, "service-a")
		if service.CurrentRevision != 1 || service.Latest.State != registry.RevisionStarting {
			t.Fatalf("service = %#v", service)
		}
	})

	t.Run("recovered replacement clears obsolete prewarm failure", func(t *testing.T) {
		store := activeServiceWithStartingUpdate(t)
		runtime := &fakeRuntime{activities: make(map[string]Activity), current: "r1", failStart: "r1"}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
		t.Cleanup(manager.Close)

		if err := manager.Reconcile(context.Background()); err != nil {
			t.Fatalf("reconcile recovered r2: %v", err)
		}
		service := getStoredService(t, store, "service-a")
		if runtime.current != "r2" || service.CurrentRevision != 2 || service.State != registry.ServiceActive {
			t.Fatalf("current = %q, service = %#v", runtime.current, service)
		}
	})
}

func TestManagerRejectsInvalidServiceID(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{})
	t.Cleanup(manager.Close)
	if _, err := manager.Put(context.Background(), "contains.dot", serviceSpec("https://example.com")); err == nil {
		t.Fatal("Put succeeded, want invalid service id")
	}
}

func TestRetryableSwitchErrorRecognizesTemporalStatus(t *testing.T) {
	t.Parallel()

	if !retryableSwitchError(serviceerror.NewNotFound("poller not registered")) {
		t.Fatal("Temporal NotFound should be retried")
	}
	if !retryableSwitchError(serviceerror.NewFailedPrecondition("poller not ready")) {
		t.Fatal("Temporal FailedPrecondition should be retried")
	}
	if retryableSwitchError(serviceerror.NewInvalidArgument("bad deployment")) {
		t.Fatal("Temporal InvalidArgument should not be retried")
	}
}

func TestManagerRetriesWorkerRegistrationRace(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	runtime := &fakeRuntime{
		activities: make(map[string]Activity),
		setErrors:  []error{serviceerror.NewNotFound("not registered")},
	}
	manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second, RetryInterval: time.Millisecond})
	t.Cleanup(manager.Close)
	if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com")); err != nil {
		t.Fatalf("put after registration race: %v", err)
	}
	if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1", "current:r1"}) {
		t.Fatalf("events = %#v", got)
	}
}

func TestManagerClassifiesSwitchTimeout(t *testing.T) {
	t.Parallel()

	t.Run("known rejection marks revision failed", func(t *testing.T) {
		store := openRegistry(t)
		runtime := &fakeRuntime{activities: make(map[string]Activity), setError: serviceerror.NewNotFound("not registered")}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: 10 * time.Millisecond, RetryInterval: time.Millisecond})
		t.Cleanup(manager.Close)
		if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com")); err == nil {
			t.Fatal("put succeeded")
		}
		service := getStoredService(t, store, "service-a")
		if service.Latest.State != registry.RevisionFailed {
			t.Fatalf("latest state = %q", service.Latest.State)
		}
	})

	t.Run("unknown result remains recoverable", func(t *testing.T) {
		store := openRegistry(t)
		runtime := &fakeRuntime{
			activities:   make(map[string]Activity),
			setError:     serviceerror.NewNotFound("not registered"),
			currentError: errors.New("Temporal unavailable"),
		}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: 10 * time.Millisecond, RetryInterval: time.Millisecond})
		t.Cleanup(manager.Close)
		if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com")); err == nil {
			t.Fatal("put succeeded")
		}
		service := getStoredService(t, store, "service-a")
		if service.Latest.State != registry.RevisionStarting {
			t.Fatalf("latest state = %q", service.Latest.State)
		}

		runtime.setError = nil
		runtime.currentError = nil
		if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com")); err != nil {
			t.Fatalf("resume starting revision: %v", err)
		}
		if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1", "current:r1"}) {
			t.Fatalf("resume events = %#v", got)
		}
	})

	t.Run("observed current version completes unknown response", func(t *testing.T) {
		store := openRegistry(t)
		runtime := &fakeRuntime{
			activities: make(map[string]Activity),
			setError:   serviceerror.NewNotFound("response lost"),
			current:    "r1",
		}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: 10 * time.Millisecond, RetryInterval: time.Millisecond})
		t.Cleanup(manager.Close)
		service, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com"))
		if err != nil || service.CurrentRevision != 1 {
			t.Fatalf("put = %#v, %v", service, err)
		}
	})

	t.Run("later rejection still observes an earlier unknown success", func(t *testing.T) {
		store := openRegistry(t)
		runtime := &fakeRuntime{
			activities:   make(map[string]Activity),
			setError:     serviceerror.NewNotFound("response lost"),
			currentError: errors.New("Temporal unavailable"),
		}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: 10 * time.Millisecond, RetryInterval: time.Millisecond})
		t.Cleanup(manager.Close)
		if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com")); err == nil {
			t.Fatal("first put succeeded")
		}

		runtime.setError = serviceerror.NewInvalidArgument("already applied before validation changed")
		runtime.currentError = nil
		runtime.current = "r1"
		service, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com"))
		if err != nil || service.CurrentRevision != 1 || service.State != registry.ServiceActive {
			t.Fatalf("resumed put = %#v, %v", service, err)
		}
	})

	t.Run("old observation cannot reject a late switch", func(t *testing.T) {
		store := activeServiceWithStartingUpdate(t)
		runtime := newDelayedSwitchRuntime()
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: 10 * time.Millisecond, RetryInterval: time.Millisecond})
		t.Cleanup(manager.Close)

		if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v2")); err == nil {
			t.Fatal("put succeeded before the server applied the switch")
		}
		service := getStoredService(t, store, "service-a")
		if service.Latest.State != registry.RevisionStarting || !service.Latest.SwitchStarted {
			t.Fatalf("service = %#v", service)
		}
		if runtime.workerStopped("r2") {
			t.Fatal("r2 Worker stopped while switch result was unknown")
		}
		close(runtime.release)
		<-runtime.applied
		if current, _ := runtime.Current(context.Background(), "service-a"); current != "r2" {
			t.Fatalf("late current = %q", current)
		}
	})

	t.Run("later nonretryable rejection does not erase unknown outcome", func(t *testing.T) {
		store := openRegistry(t)
		runtime := &fakeRuntime{
			activities: make(map[string]Activity),
			setErrors:  []error{serviceerror.NewDeadlineExceeded("response lost")},
			setError:   serviceerror.NewInvalidArgument("rejected retry"),
		}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second, RetryInterval: time.Millisecond})
		t.Cleanup(manager.Close)

		if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com")); err == nil {
			t.Fatal("put succeeded")
		}
		service := getStoredService(t, store, "service-a")
		if service.Latest.State != registry.RevisionStarting || !service.Latest.SwitchStarted {
			t.Fatalf("service = %#v", service)
		}
		if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1"}) {
			t.Fatalf("events = %#v", got)
		}
	})

	t.Run("later retryable rejection does not erase unknown outcome", func(t *testing.T) {
		store := openRegistry(t)
		runtime := &fakeRuntime{
			activities: make(map[string]Activity),
			setErrors:  []error{serviceerror.NewDeadlineExceeded("response lost")},
			setError:   serviceerror.NewNotFound("rejected retry"),
		}
		manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: 10 * time.Millisecond, RetryInterval: time.Millisecond})
		t.Cleanup(manager.Close)

		if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com")); err == nil {
			t.Fatal("put succeeded")
		}
		service := getStoredService(t, store, "service-a")
		if service.Latest.State != registry.RevisionStarting || !service.Latest.SwitchStarted {
			t.Fatalf("service = %#v", service)
		}
		if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1"}) {
			t.Fatalf("events = %#v", got)
		}
	})
}

func TestManagerReconcileHonorsParentDeadline(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	first := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	if _, err := first.Put(context.Background(), "service-a", serviceSpec("https://example.com/v1")); err != nil {
		t.Fatal(err)
	}
	first.Close()

	manager := NewManager(store, deadlineRuntime{}, http.DefaultClient, ManagerOptions{SwitchTimeout: 500 * time.Millisecond, RetryInterval: time.Millisecond})
	t.Cleanup(manager.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := manager.Reconcile(ctx); err == nil {
		t.Fatal("reconcile succeeded")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("reconcile ignored parent deadline: %v", elapsed)
	}
}

func TestManagerCloseStopsWorkersConcurrently(t *testing.T) {
	t.Parallel()

	manager := NewManager(openRegistry(t), &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{})
	first := newBlockingWorker()
	second := newBlockingWorker()
	manager.workers[workerKey{serviceID: "service-a", revision: 1}] = first
	manager.workers[workerKey{serviceID: "service-b", revision: 1}] = second
	done := make(chan struct{})
	go func() {
		manager.Close()
		close(done)
	}()
	release := func(worker *blockingWorker) {
		select {
		case <-worker.release:
		default:
			close(worker.release)
		}
	}
	defer release(first)
	defer release(second)
	for name, entered := range map[string]<-chan struct{}{"first": first.entered, "second": second.entered} {
		select {
		case <-entered:
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("%s Worker was not told to stop concurrently", name)
		}
	}
	release(first)
	release(second)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
}

func TestConcurrentManagerCloseStopsEachWorkerOnce(t *testing.T) {
	t.Parallel()

	runtime := &fakeRuntime{activities: make(map[string]Activity)}
	manager := NewManager(openRegistry(t), runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v1")); err != nil {
		t.Fatal(err)
	}

	const closers = 8
	start := make(chan struct{})
	var closeGroup sync.WaitGroup
	closeGroup.Add(closers)
	for range closers {
		go func() {
			defer closeGroup.Done()
			<-start
			manager.Close()
		}()
	}
	close(start)
	closeGroup.Wait()

	if got := runtime.eventsSnapshot(); !reflect.DeepEqual(got, []string{"start:r1", "current:r1", "stop:r1"}) {
		t.Fatalf("events after concurrent Close = %#v", got)
	}
}

func TestManagerRejectsOperationsAfterClose(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{})
	manager.Close()
	if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com")); err == nil {
		t.Fatal("Put succeeded after Close")
	}
	if err := manager.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile succeeded after Close")
	}
}

func serviceSpec(endpoint string) temporaless.ServiceSpec {
	return temporaless.ServiceSpec{
		Timeout: "5s",
		Actions: map[string]temporaless.ActionSpec{
			"invoke": {Method: http.MethodPost, Endpoint: endpoint, RequestSchema: json.RawMessage(`{"type":"object"}`)},
		},
	}
}

func openRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	store, err := registry.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func activeServiceWithStartingUpdate(t *testing.T) *registry.Registry {
	t.Helper()
	store := openRegistry(t)
	manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	if _, err := manager.Put(context.Background(), "service-a", serviceSpec("https://example.com/v1")); err != nil {
		t.Fatal(err)
	}
	manager.Close()

	compiled, err := backend.Compile(serviceSpec("https://example.com/v2"), http.DefaultClient, backend.DefaultMaxResponseBodyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Prepare(context.Background(), "service-a", registry.KindService, compiled.SpecJSON(), compiled.Hash()); err != nil {
		t.Fatal(err)
	}
	return store
}

func getStoredService(t *testing.T, store *registry.Registry, id string) registry.Service {
	t.Helper()
	service, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type fakeRuntime struct {
	mu           sync.Mutex
	events       []string
	activities   map[string]Activity
	current      string
	failStart    string
	setErrors    []error
	setError     error
	currentError error
}

type gatedSetCurrentRuntime struct {
	*fakeRuntime
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *gatedSetCurrentRuntime) SetCurrent(ctx context.Context, serviceID, buildID string) error {
	r.once.Do(func() { close(r.entered) })
	select {
	case <-r.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return r.fakeRuntime.SetCurrent(ctx, serviceID, buildID)
}

func (r *fakeRuntime) Start(_ string, buildID string, activity Activity) (Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failStart == buildID {
		return nil, errors.New("start failed")
	}
	r.events = append(r.events, "start:"+buildID)
	r.activities[buildID] = activity
	return &fakeWorker{stop: func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.events = append(r.events, "stop:"+buildID)
	}}, nil
}

func (r *fakeRuntime) SetCurrent(_ context.Context, _ string, buildID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.setErrors) > 0 {
		err := r.setErrors[0]
		r.setErrors = r.setErrors[1:]
		return err
	}
	if r.setError != nil {
		return r.setError
	}
	r.events = append(r.events, "current:"+buildID)
	r.current = buildID
	return nil
}

func (r *fakeRuntime) Current(context.Context, string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current, r.currentError
}

func (r *fakeRuntime) Ping(context.Context) error { return nil }

func (r *fakeRuntime) eventsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *fakeRuntime) activity(buildID string) Activity {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.activities[buildID]
}

type fakeWorker struct {
	once sync.Once
	stop func()
}

func (w *fakeWorker) Stop() { w.once.Do(w.stop) }

type delayedSwitchRuntime struct {
	mu      sync.Mutex
	current string
	stopped map[string]bool
	release chan struct{}
	applied chan struct{}
	once    sync.Once
}

func newDelayedSwitchRuntime() *delayedSwitchRuntime {
	return &delayedSwitchRuntime{
		current: "r1",
		stopped: make(map[string]bool),
		release: make(chan struct{}),
		applied: make(chan struct{}),
	}
}

func (r *delayedSwitchRuntime) Start(_ string, buildID string, _ Activity) (Worker, error) {
	return &fakeWorker{stop: func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.stopped[buildID] = true
	}}, nil
}

func (r *delayedSwitchRuntime) SetCurrent(ctx context.Context, _ string, buildID string) error {
	if buildID != "r2" {
		r.mu.Lock()
		r.current = buildID
		r.mu.Unlock()
		return nil
	}
	r.once.Do(func() {
		go func() {
			<-r.release
			r.mu.Lock()
			r.current = "r2"
			r.mu.Unlock()
			close(r.applied)
		}()
	})
	<-ctx.Done()
	return serviceerror.NewDeadlineExceeded("response timed out while switch continued")
}

func (r *delayedSwitchRuntime) Current(context.Context, string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current, nil
}

func (r *delayedSwitchRuntime) Ping(context.Context) error { return nil }

func (r *delayedSwitchRuntime) workerStopped(buildID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped[buildID]
}

type deadlineRuntime struct{}

func (deadlineRuntime) Start(string, string, Activity) (Worker, error) {
	return &fakeWorker{stop: func() {}}, nil
}
func (deadlineRuntime) SetCurrent(ctx context.Context, _, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}
func (deadlineRuntime) Current(context.Context, string) (string, error) {
	return "", errors.New("Temporal unavailable")
}
func (deadlineRuntime) Ping(context.Context) error { return nil }

type blockingWorker struct {
	entered chan struct{}
	release chan struct{}
}

func newBlockingWorker() *blockingWorker {
	return &blockingWorker{entered: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingWorker) Stop() {
	close(w.entered)
	<-w.release
}
