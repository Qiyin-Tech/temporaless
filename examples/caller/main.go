package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	callerTaskQueue = "temporaless-quickstart-caller"
	workflowType    = "temporaless-quickstart-invoke"
)

type invokeInput struct {
	ServiceID string
	Request   gatewayRequest
}

// gatewayRequest is the language-neutral JSON payload accepted by the Gateway Activity.
type gatewayRequest struct {
	Action  string      `json:"action"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`
	JobID   string      `json:"job_id,omitempty"`
}

// gatewayResponse is the language-neutral JSON result returned by the Gateway Activity.
type gatewayResponse struct {
	Status  int         `json:"status"`
	Message string      `json:"message"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`
}

func invokeWorkflow(ctx workflow.Context, input invokeInput) (gatewayResponse, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              input.ServiceID,
		ScheduleToCloseTimeout: 30 * time.Second,
		StartToCloseTimeout:    15 * time.Second,
		HeartbeatTimeout:       2 * time.Second,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumAttempts:    3,
		},
	})
	var response gatewayResponse
	err := workflow.ExecuteActivity(ctx, input.ServiceID+"/invoke", input.Request).Get(ctx, &response)
	return response, err
}

func main() {
	temporalClient, err := client.Dial(client.Options{HostPort: client.DefaultHostPort, Namespace: "default"})
	if err != nil {
		log.Fatal(err)
	}
	defer temporalClient.Close()

	workflowWorker := worker.New(temporalClient, callerTaskQueue, worker.Options{})
	workflowWorker.RegisterWorkflowWithOptions(invokeWorkflow, workflow.RegisterOptions{Name: workflowType})
	if err := workflowWorker.Start(); err != nil {
		log.Fatal(err)
	}
	defer workflowWorker.Stop()

	operationID := fmt.Sprintf("quickstart-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        operationID,
		TaskQueue: callerTaskQueue,
	}, workflowType, invokeInput{
		ServiceID: "demo-service",
		Request: gatewayRequest{
			Action: "invoke",
			Headers: http.Header{
				"Authorization":   {"Bearer demo-token"},
				"Content-Type":    {"application/json"},
				"Idempotency-Key": {operationID},
			},
			Body: []byte(`{"prompt":"a small robot playing piano"}`),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	var response gatewayResponse
	if err := run.Get(ctx, &response); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("workflow_id=%s run_id=%s\n", run.GetID(), run.GetRunID())
	fmt.Printf("status=%d message=%q content_type=%q\n", response.Status, response.Message, response.Headers.Get("Content-Type"))
	fmt.Printf("body=%s\n", response.Body)
}
