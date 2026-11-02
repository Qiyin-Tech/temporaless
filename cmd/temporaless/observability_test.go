package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	temporallog "go.temporal.io/sdk/log"
)

func TestLoggerAndTemporalSDKUseJSON(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(&output)
	logger.Info("application message", "key", "value")
	temporallog.NewStructuredLogger(logger.With("component", "temporal_sdk")).Info("SDK message", "task_queue", "demo")

	scanner := bufio.NewScanner(&output)
	for _, message := range []string{"application message", "SDK message"} {
		if !scanner.Scan() {
			t.Fatalf("missing log for %q", message)
		}
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("log is not JSON: %v", err)
		}
		if record["msg"] != message || record["service"] != "temporaless" {
			t.Fatalf("log = %#v", record)
		}
	}
}

func TestMetricsEndpointExportsTemporalMetrics(t *testing.T) {
	endpoint, err := newMetricsEndpoint(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(endpoint.shutdown)
	endpoint.temporalHandler.WithTags(map[string]string{"task_queue": "demo"}).Counter("temporaless_test_counter").Inc(1)
	endpoint.temporalHandler.Timer("temporaless_test_latency").Record(100 * time.Millisecond)

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	endpoint.httpHandler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	body := response.Body.String()
	if !strings.Contains(body, "temporaless_test_counter_total") || !strings.Contains(body, `task_queue="demo"`) {
		t.Fatalf("metrics missing Temporal counter and tags:\n%s", body)
	}
	if !strings.Contains(body, "temporaless_test_latency_seconds_bucket") || !strings.Contains(body, `le="0.1"} 1`) {
		t.Fatalf("metrics missing sub-second latency bucket:\n%s", body)
	}

	request = httptest.NewRequest(http.MethodGet, "/", nil)
	response = httptest.NewRecorder()
	endpoint.httpHandler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unexpected non-metrics status = %d", response.Code)
	}
}
