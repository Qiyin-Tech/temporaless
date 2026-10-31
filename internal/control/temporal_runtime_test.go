package control

import (
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

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
