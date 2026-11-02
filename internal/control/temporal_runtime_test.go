package control

import (
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func TestHeartbeatInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout time.Duration
		want    time.Duration
	}{
		{name: "unset", want: time.Second},
		{name: "long", timeout: 10 * time.Second, want: time.Second},
		{name: "short", timeout: time.Second, want: 500 * time.Millisecond},
		{name: "minimum", timeout: time.Nanosecond, want: time.Nanosecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := heartbeatInterval(test.timeout); got != test.want {
				t.Fatalf("heartbeat interval = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTemporalRuntimeDefaultsActivityConcurrency(t *testing.T) {
	t.Parallel()

	runtime := NewTemporalRuntime(nil, 0, 0, 0)
	if got := runtime.maxConcurrentActivities; got != defaultMaxConcurrentActivities {
		t.Fatalf("max concurrent Activities = %d, want %d", got, defaultMaxConcurrentActivities)
	}
	if runtime.maxActivityTaskPollers != 0 {
		t.Fatalf("max Activity task pollers = %d, want SDK default", runtime.maxActivityTaskPollers)
	}
}

func TestTemporalRuntimeUsesConfiguredActivityTaskPollers(t *testing.T) {
	t.Parallel()

	runtime := NewTemporalRuntime(nil, time.Second, 32, 16)
	if runtime.maxActivityTaskPollers != 16 {
		t.Fatalf("max Activity task pollers = %d, want 16", runtime.maxActivityTaskPollers)
	}
}

func TestCurrentBuildID(t *testing.T) {
	t.Parallel()

	if got := currentBuildID(client.WorkerDeploymentDescribeResponse{}); got != "" {
		t.Fatalf("unversioned build ID = %q", got)
	}
	description := client.WorkerDeploymentDescribeResponse{Info: client.WorkerDeploymentInfo{
		RoutingConfig: client.WorkerDeploymentRoutingConfig{
			CurrentVersion: &worker.WorkerDeploymentVersion{DeploymentName: "service-a", BuildID: "r3"},
		},
	}}
	if got := currentBuildID(description); got != "r3" {
		t.Fatalf("build ID = %q, want r3", got)
	}
}
