package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"temporaless"
	"temporaless/internal/registry"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type integrationWorkflowArgs struct {
	TaskQueue    string
	ActivityType string
	Request      temporaless.Request
}

func integrationWorkflow(ctx workflow.Context, args integrationWorkflowArgs) (temporaless.Response, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              args.TaskQueue,
		ScheduleToCloseTimeout: 30 * time.Second,
		StartToCloseTimeout:    10 * time.Second,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	var response temporaless.Response
	err := workflow.ExecuteActivity(ctx, args.ActivityType, args.Request).Get(ctx, &response)
	return response, err
}

func TestManagerWithTemporal(t *testing.T) {
	if os.Getenv("TEMPORAL_SMOKE") != "1" {
		t.Skip("set TEMPORAL_SMOKE=1 to run against a local Temporal server")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	temporalClient, err := client.Dial(client.Options{HostPort: temporalAddress()})
	if err != nil {
		t.Fatalf("dial Temporal: %v", err)
	}
	defer temporalClient.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	serviceID := "integration-" + suffix
	workflowQueue := "integration-workflow-" + suffix
	workflowType := "integration-workflow-" + suffix
	workflowWorker := worker.New(temporalClient, workflowQueue, worker.Options{})
	workflowWorker.RegisterWorkflowWithOptions(integrationWorkflow, workflow.RegisterOptions{Name: workflowType})
	if err := workflowWorker.Start(); err != nil {
		t.Fatalf("start workflow worker: %v", err)
	}
	defer workflowWorker.Stop()

	const concurrentCalls = 4
	blockedStarted := make(chan struct{}, concurrentCalls)
	blockedRelease := make(chan struct{})
	var blockV1 atomic.Bool
	var releaseOnce sync.Once
	releaseBlocked := func() { releaseOnce.Do(func() { close(blockedRelease) }) }
	defer releaseBlocked()
	backendServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		if request.URL.Path == "/v1" && blockV1.Load() {
			blockedStarted <- struct{}{}
			select {
			case <-blockedRelease:
			case <-request.Context().Done():
				return
			}
		}
		response.Header().Set("Content-Type", "application/octet-stream")
		_, _ = fmt.Fprintf(response, "%s:%s", request.URL.Path, body)
	}))
	defer backendServer.Close()

	store, err := registry.Open(":memory:")
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	defer store.Close()
	runtime := NewTemporalRuntime(temporalClient, 5*time.Second)
	manager := NewManager(store, runtime, backendServer.Client(), ManagerOptions{SwitchTimeout: 15 * time.Second})
	defer manager.Close()

	if _, err := manager.Put(ctx, serviceID, integrationSpec(backendServer.URL+"/v1")); err != nil {
		t.Fatalf("put r1: %v", err)
	}
	assertIntegrationCall(t, ctx, temporalClient, workflowQueue, workflowType, serviceID, suffix+"-r1", "/v1:payload")

	blockV1.Store(true)
	blockedRuns := make([]client.WorkflowRun, 0, concurrentCalls)
	for index := range concurrentCalls {
		blockedRuns = append(blockedRuns, startIntegrationCall(t, ctx, temporalClient, workflowQueue, workflowType, serviceID, fmt.Sprintf("%s-inflight-%d", suffix, index)))
	}
	for range concurrentCalls {
		select {
		case <-blockedStarted:
		case <-ctx.Done():
			t.Fatalf("wait for concurrent r1 calls: %v", ctx.Err())
		}
	}
	updateDone := make(chan error, 1)
	go func() {
		_, err := manager.Put(ctx, serviceID, integrationSpec(backendServer.URL+"/v2"))
		updateDone <- err
	}()
	waitForCurrentBuildID(t, ctx, runtime, serviceID, "r2")
	select {
	case err := <-updateDone:
		releaseBlocked()
		t.Fatalf("put r2 returned before r1 Activities drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseBlocked()
	for _, run := range blockedRuns {
		var response temporaless.Response
		if err := run.Get(ctx, &response); err != nil || string(response.Body) != "/v1:payload" {
			t.Fatalf("in-flight r1 response = %q, %v", response.Body, err)
		}
	}
	if err := <-updateDone; err != nil {
		t.Fatalf("put r2: %v", err)
	}
	assertIntegrationCall(t, ctx, temporalClient, workflowQueue, workflowType, serviceID, suffix+"-r2", "/v2:payload")

	if _, err := manager.Delete(ctx, serviceID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	run := startIntegrationCall(t, ctx, temporalClient, workflowQueue, workflowType, serviceID, suffix+"-deleted")
	var response temporaless.Response
	err = run.Get(ctx, &response)
	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) || applicationError.Type() != "ServiceDeleted" {
		t.Fatalf("deleted call error = %T %v", err, err)
	}
}

func waitForCurrentBuildID(t *testing.T, ctx context.Context, runtime *TemporalRuntime, serviceID, want string) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := runtime.Current(ctx, serviceID)
		if err == nil && current == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("current build ID = %q, want %q: %v", current, want, errors.Join(err, ctx.Err()))
		case <-ticker.C:
		}
	}
}

func integrationSpec(endpoint string) temporaless.ServiceSpec {
	return temporaless.ServiceSpec{
		Timeout: "5s",
		Actions: map[string]temporaless.ActionSpec{
			"invoke": {Method: http.MethodPost, Endpoint: endpoint},
		},
	}
}

func assertIntegrationCall(
	t *testing.T,
	ctx context.Context,
	temporalClient client.Client,
	workflowQueue string,
	workflowType string,
	serviceID string,
	workflowID string,
	want string,
) {
	t.Helper()
	run := startIntegrationCall(t, ctx, temporalClient, workflowQueue, workflowType, serviceID, workflowID)
	var response temporaless.Response
	if err := run.Get(ctx, &response); err != nil {
		t.Fatalf("get workflow result: %v", err)
	}
	if string(response.Body) != want {
		t.Fatalf("response body = %q, want %q", response.Body, want)
	}
}

func startIntegrationCall(
	t *testing.T,
	ctx context.Context,
	temporalClient client.Client,
	workflowQueue string,
	workflowType string,
	serviceID string,
	workflowID string,
) client.WorkflowRun {
	t.Helper()
	run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: workflowID, TaskQueue: workflowQueue}, workflowType, integrationWorkflowArgs{
		TaskQueue:    serviceID,
		ActivityType: serviceID + "/invoke",
		Request: temporaless.Request{
			Action: "invoke",
			Body:   []byte("payload"),
		},
	})
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}
	return run
}

func temporalAddress() string {
	if address := strings.TrimSpace(os.Getenv("TEMPORAL_ADDRESS")); address != "" {
		return address
	}
	return client.DefaultHostPort
}
