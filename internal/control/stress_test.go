package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"temporaless/internal/registry"
)

func TestSustainedControlPlaneStress(t *testing.T) {
	if os.Getenv("TEMPORALESS_STRESS") != "1" {
		t.Skip("set TEMPORALESS_STRESS=1 to run the 30-second control-plane stress test")
	}

	const (
		duration     = 30 * time.Second
		serviceCount = 8
		writers      = 4
		readers      = 8
	)
	store := openRegistry(t)
	runtime := &stressRuntime{current: make(map[string]string)}
	manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{SwitchTimeout: time.Second})
	t.Cleanup(manager.Close)
	handler := NewAPI(manager, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for index := range serviceCount {
		serviceID := fmt.Sprintf("stress-%02d", index)
		status, _ := stressRequest(handler, context.Background(), http.MethodPut, "/services/"+serviceID, stressSpec(serviceID, 0))
		if status != http.StatusOK {
			t.Fatalf("initialize %s: status %d", serviceID, status)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	var firstError error
	var failOnce sync.Once
	fail := func(err error) {
		failOnce.Do(func() {
			firstError = err
			cancel()
		})
	}
	var readOperations, writeOperations atomic.Uint64
	var group sync.WaitGroup

	group.Add(writers)
	for writer := range writers {
		go func() {
			defer group.Done()
			for sequence := 1; ; sequence++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				serviceID := fmt.Sprintf("stress-%02d", (writer+sequence)%serviceCount)
				method, body := http.MethodPut, stressSpec(serviceID, writer*1_000_000+sequence)
				if sequence%5 == 0 {
					method, body = http.MethodDelete, nil
				}
				status, response := stressRequest(handler, ctx, method, "/services/"+serviceID, body)
				if ctx.Err() != nil {
					return
				}
				if status != http.StatusOK {
					fail(fmt.Errorf("%s %s: status %d: %s", method, serviceID, status, response))
					return
				}
				writeOperations.Add(1)
			}
		}()
	}

	group.Add(readers)
	for reader := range readers {
		go func() {
			defer group.Done()
			for sequence := 0; ; sequence++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				var path string
				switch sequence % 3 {
				case 0:
					path = "/services"
				case 1:
					path = fmt.Sprintf("/services/stress-%02d", (reader+sequence)%serviceCount)
				default:
					path = "/healthz"
				}
				status, response := stressRequest(handler, ctx, http.MethodGet, path, nil)
				if ctx.Err() != nil {
					return
				}
				if status != http.StatusOK {
					fail(fmt.Errorf("GET %s: status %d: %s", path, status, response))
					return
				}
				if path != "/healthz" {
					if err := validateStressResponse(path, response); err != nil {
						fail(err)
						return
					}
				}
				readOperations.Add(1)
			}
		}()
	}

	group.Wait()
	if firstError != nil {
		t.Fatal(firstError)
	}
	if readOperations.Load() == 0 || writeOperations.Load() == 0 {
		t.Fatalf("stress did no useful work: reads=%d writes=%d", readOperations.Load(), writeOperations.Load())
	}

	services, err := store.List(context.Background())
	if err != nil || len(services) != serviceCount {
		t.Fatalf("final services=%d, err=%v", len(services), err)
	}
	for _, service := range services {
		if service.CurrentRevision == 0 || service.Latest.Number != service.CurrentRevision || service.Current.State != registry.RevisionActive || service.Latest.State != registry.RevisionActive {
			t.Fatalf("incomplete final revision: %#v", service)
		}
		if service.State == registry.ServiceActive && service.Current.Kind != registry.KindService || service.State == registry.ServiceDeleted && service.Current.Kind != registry.KindTombstone {
			t.Fatalf("service state and revision kind disagree: %#v", service)
		}
		current, err := runtime.Current(context.Background(), service.ID)
		if err != nil || current != buildID(service.CurrentRevision) {
			t.Fatalf("runtime current for %s=%q, want %q, err=%v", service.ID, current, buildID(service.CurrentRevision), err)
		}
	}
	if len(manager.workers) != serviceCount {
		t.Fatalf("live workers=%d, want %d", len(manager.workers), serviceCount)
	}
	t.Logf("completed %d reads and %d writes", readOperations.Load(), writeOperations.Load())
}

func stressSpec(serviceID string, revision int) []byte {
	return fmt.Appendf(nil, `{"actions":{"invoke":{"method":"POST","endpoint":"https://example.com/%s/%d"}},"timeout":"1s"}`, serviceID, revision)
}

func stressRequest(handler http.Handler, ctx context.Context, method, path string, body []byte) (int, []byte) {
	request := httptest.NewRequest(method, path, bytes.NewReader(body)).WithContext(ctx)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response.Code, bytes.Clone(response.Body.Bytes())
}

func validateStressResponse(path string, response []byte) error {
	if path == "/services" {
		var services []ServiceView
		if err := json.Unmarshal(response, &services); err != nil {
			return fmt.Errorf("decode service list: %w", err)
		}
		for _, service := range services {
			if err := validateStressView(service); err != nil {
				return err
			}
		}
		return nil
	}
	var service ServiceView
	if err := json.Unmarshal(response, &service); err != nil {
		return fmt.Errorf("decode service: %w", err)
	}
	return validateStressView(service)
}

func validateStressView(service ServiceView) error {
	if service.CurrentRevision == 0 || service.LatestRevision < service.CurrentRevision || service.RevisionState != registry.RevisionActive {
		return fmt.Errorf("inconsistent service view: %#v", service)
	}
	if service.State == registry.ServiceActive && service.RevisionKind != registry.KindService || service.State == registry.ServiceDeleted && service.RevisionKind != registry.KindTombstone {
		return fmt.Errorf("service state and revision kind disagree: %#v", service)
	}
	if service.State != registry.ServiceActive && service.State != registry.ServiceDeleted {
		return fmt.Errorf("unexpected initialized service state: %#v", service)
	}
	return nil
}

type stressRuntime struct {
	mu      sync.Mutex
	current map[string]string
}

func (*stressRuntime) Start(string, string, Activity) (Worker, error) {
	return stressWorker{}, nil
}

func (r *stressRuntime) SetCurrent(_ context.Context, serviceID, buildID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current[serviceID] = buildID
	return nil
}

func (r *stressRuntime) Current(_ context.Context, serviceID string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current[serviceID], nil
}

func (*stressRuntime) Ping(context.Context) error { return nil }

type stressWorker struct{}

func (stressWorker) Stop() {}
