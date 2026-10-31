package smoke

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type invokeArgs struct {
	ActivityQueue string
	ActivityType  string
	Block         bool
}

const concurrentActivities = 4

func invokeWorkflow(ctx workflow.Context, args invokeArgs) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              args.ActivityQueue,
		ScheduleToCloseTimeout: 2 * time.Minute,
		StartToCloseTimeout:    20 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	})

	var version string
	err := workflow.ExecuteActivity(ctx, args.ActivityType, args.Block).Get(ctx, &version)
	return version, err
}

func TestActivityOnlyWorkerDeploymentSwitch(t *testing.T) {
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
	workflowQueue := "smoke-workflow-" + suffix
	activityQueue := "smoke-service-" + suffix
	activityType := "smoke-service-" + suffix + "/invoke"
	workflowType := "smoke-workflow-" + suffix
	deployment := "smoke-service-" + suffix

	workflowWorker := worker.New(temporalClient, workflowQueue, worker.Options{})
	workflowWorker.RegisterWorkflowWithOptions(invokeWorkflow, workflow.RegisterOptions{Name: workflowType})
	if err := workflowWorker.Start(); err != nil {
		t.Fatalf("start workflow worker: %v", err)
	}
	defer workflowWorker.Stop()

	r1 := startVersionWorker(t, temporalClient, activityQueue, activityType, deployment, "r1", func(context.Context, bool) (string, error) {
		return "r1", nil
	})
	defer r1.Stop()
	setCurrentEventually(t, ctx, temporalClient, deployment, "r1")
	r1.Stop()

	queuedRun := executeWorkflow(t, ctx, temporalClient, workflowQueue, workflowType, suffix+"-queued", invokeArgs{
		ActivityQueue: activityQueue,
		ActivityType:  activityType,
	})
	if err := waitForActivityBacklog(ctx, temporalClient, deployment, activityQueue, "r1", 1); err != nil {
		t.Fatalf("wait for r1 backlog: %v", err)
	}

	started := make(chan struct{}, concurrentActivities)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBlocked := func() { releaseOnce.Do(func() { close(release) }) }

	r2 := startVersionWorker(t, temporalClient, activityQueue, activityType, deployment, "r2", func(ctx context.Context, block bool) (string, error) {
		if block {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return "r2", nil
	})
	defer r2.Stop()
	defer releaseBlocked()
	setCurrentEventually(t, ctx, temporalClient, deployment, "r2")
	assertWorkflowResult(t, ctx, queuedRun, "r2")
	if err := waitForActivityBacklog(ctx, temporalClient, deployment, activityQueue, "r1", 0); err != nil {
		t.Fatalf("wait for r1 to drain: %v", err)
	}

	blockedRuns := make([]client.WorkflowRun, 0, concurrentActivities)
	for index := range concurrentActivities {
		blockedRuns = append(blockedRuns, executeWorkflow(t, ctx, temporalClient, workflowQueue, workflowType, fmt.Sprintf("%s-blocked-%d", suffix, index), invokeArgs{
			ActivityQueue: activityQueue,
			ActivityType:  activityType,
			Block:         true,
		}))
	}
	for range concurrentActivities {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatalf("wait for concurrent r2 activities to start: %v", ctx.Err())
		}
	}

	tombstone := startVersionWorker(t, temporalClient, activityQueue, activityType, deployment, "r3", func(context.Context, bool) (string, error) {
		return "", temporal.NewNonRetryableApplicationError("service deleted", "ServiceDeleted", nil)
	})
	defer tombstone.Stop()
	setCurrentEventually(t, ctx, temporalClient, deployment, "r3")

	deletedRun := executeWorkflow(t, ctx, temporalClient, workflowQueue, workflowType, suffix+"-deleted", invokeArgs{
		ActivityQueue: activityQueue,
		ActivityType:  activityType,
	})
	var result string
	err = deletedRun.Get(ctx, &result)
	if err == nil || !strings.Contains(err.Error(), "ServiceDeleted") {
		t.Fatalf("deleted service error = %v, want ServiceDeleted", err)
	}

	releaseBlocked()
	for _, run := range blockedRuns {
		assertWorkflowResult(t, ctx, run, "r2")
	}
}

func startVersionWorker(
	t *testing.T,
	temporalClient client.Client,
	taskQueue string,
	activityType string,
	deployment string,
	buildID string,
	activityFn func(context.Context, bool) (string, error),
) worker.Worker {
	t.Helper()

	w := worker.New(temporalClient, taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: concurrentActivities,
		WorkerStopTimeout:                  2 * time.Second,
		DisableWorkflowWorker:              true,
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning: true,
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: deployment,
				BuildID:        buildID,
			},
		},
	})
	w.RegisterActivityWithOptions(activityFn, activity.RegisterOptions{Name: activityType})
	if err := w.Start(); err != nil {
		t.Fatalf("start activity worker %s: %v", buildID, err)
	}
	return w
}

func setCurrentEventually(t *testing.T, ctx context.Context, temporalClient client.Client, deployment, buildID string) {
	t.Helper()

	handle := temporalClient.WorkerDeploymentClient().GetHandle(deployment)
	var lastErr error
	for ctx.Err() == nil {
		_, lastErr = handle.SetCurrentVersion(ctx, client.WorkerDeploymentSetCurrentVersionOptions{BuildID: buildID})
		if lastErr == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("set %s/%s current: %v", deployment, buildID, errors.Join(lastErr, ctx.Err()))
}

func executeWorkflow(
	t *testing.T,
	ctx context.Context,
	temporalClient client.Client,
	taskQueue string,
	workflowType string,
	workflowID string,
	args invokeArgs,
) client.WorkflowRun {
	t.Helper()

	run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, workflowType, args)
	if err != nil {
		t.Fatalf("start workflow %s: %v", workflowID, err)
	}
	return run
}

func assertWorkflowResult(t *testing.T, ctx context.Context, run client.WorkflowRun, want string) {
	t.Helper()

	var got string
	if err := run.Get(ctx, &got); err != nil {
		t.Fatalf("get workflow result: %v", err)
	}
	if got != want {
		t.Fatalf("workflow result = %q, want %q", got, want)
	}
}

func waitForActivityBacklog(ctx context.Context, temporalClient client.Client, deployment, taskQueue, buildID string, want int64) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastCount int64 = -1
	for {
		description, err := temporalClient.WorkflowService().DescribeWorkerDeploymentVersion(ctx, &workflowservicepb.DescribeWorkerDeploymentVersionRequest{
			Namespace: "default",
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: deployment,
				BuildId:        buildID,
			},
			ReportTaskQueueStats: true,
		})
		if err == nil {
			for _, queue := range description.GetVersionTaskQueues() {
				if queue.GetName() == taskQueue && queue.GetType() == enumspb.TASK_QUEUE_TYPE_ACTIVITY && queue.GetStats() != nil {
					lastCount = queue.GetStats().GetApproximateBacklogCount()
				}
			}
			if (want == 0 && lastCount == 0) || (want > 0 && lastCount >= want) {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("build %s backlog = %d, want %d: %w", buildID, lastCount, want, errors.Join(err, ctx.Err()))
		case <-ticker.C:
		}
	}
}

func temporalAddress() string {
	if address := os.Getenv("TEMPORAL_ADDRESS"); address != "" {
		return address
	}
	return client.DefaultHostPort
}
