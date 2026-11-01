package control

import (
	"context"
	"time"

	"temporaless"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const activityHeartbeatInterval = time.Second

type TemporalRuntime struct {
	client      client.Client
	stopTimeout time.Duration
}

func NewTemporalRuntime(temporalClient client.Client, stopTimeout time.Duration) *TemporalRuntime {
	if stopTimeout <= 0 {
		stopTimeout = 30 * time.Second
	}
	return &TemporalRuntime{client: temporalClient, stopTimeout: stopTimeout}
}

func (r *TemporalRuntime) Start(serviceID string, buildID string, activityFn Activity) (Worker, error) {
	activityWorker := worker.New(r.client, serviceID, worker.Options{
		DisableWorkflowWorker: true,
		WorkerStopTimeout:     r.stopTimeout,
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning: true,
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: serviceID,
				BuildID:        buildID,
			},
		},
	})
	activityWorker.RegisterActivityWithOptions(withActivityHeartbeat(activityFn), activity.RegisterOptions{Name: serviceID + "/invoke"})
	if err := activityWorker.Start(); err != nil {
		return nil, err
	}
	return activityWorker, nil
}

func withActivityHeartbeat(activityFn Activity) Activity {
	return func(ctx context.Context, request temporaless.Request) (temporaless.Response, error) {
		heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
		heartbeatDone := make(chan struct{})
		interval := heartbeatInterval(activity.GetInfo(ctx).HeartbeatTimeout)
		go func() {
			defer close(heartbeatDone)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-heartbeatCtx.Done():
					return
				case <-ticker.C:
					activity.RecordHeartbeat(heartbeatCtx)
				}
			}
		}()
		defer func() {
			stopHeartbeat()
			<-heartbeatDone
		}()

		return activityFn(ctx, request)
	}
}

func heartbeatInterval(timeout time.Duration) time.Duration {
	if timeout > 0 && timeout/2 < activityHeartbeatInterval {
		if interval := timeout / 2; interval > 0 {
			return interval
		}
		return timeout
	}
	return activityHeartbeatInterval
}

func (r *TemporalRuntime) SetCurrent(ctx context.Context, serviceID string, buildID string) error {
	_, err := r.client.WorkerDeploymentClient().GetHandle(serviceID).SetCurrentVersion(ctx, client.WorkerDeploymentSetCurrentVersionOptions{
		BuildID: buildID,
	})
	return err
}

func (r *TemporalRuntime) Current(ctx context.Context, serviceID string) (string, error) {
	description, err := r.client.WorkerDeploymentClient().GetHandle(serviceID).Describe(ctx, client.WorkerDeploymentDescribeOptions{})
	if err != nil {
		return "", err
	}
	return currentBuildID(description), nil
}

func currentBuildID(description client.WorkerDeploymentDescribeResponse) string {
	if description.Info.RoutingConfig.CurrentVersion == nil {
		return ""
	}
	return description.Info.RoutingConfig.CurrentVersion.BuildID
}

func (r *TemporalRuntime) Ping(ctx context.Context) error {
	_, err := r.client.CheckHealth(ctx, nil)
	return err
}
