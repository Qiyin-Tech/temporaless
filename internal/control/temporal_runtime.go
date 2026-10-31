package control

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

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
	activityWorker.RegisterActivityWithOptions(activityFn, activity.RegisterOptions{Name: serviceID + "/invoke"})
	if err := activityWorker.Start(); err != nil {
		return nil, err
	}
	return activityWorker, nil
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
