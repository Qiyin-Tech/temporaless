package control

import (
	"context"
	"encoding/json"
	"errors"
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
	compiled, err := backend.Compile(serviceSpec("https://example.com/v1"), http.DefaultClient)
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
		compiled, err := backend.Compile(serviceSpec("https://example.com/v1"), http.DefaultClient)
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

	compiled, err := backend.Compile(serviceSpec("https://example.com/v2"), http.DefaultClient)
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
