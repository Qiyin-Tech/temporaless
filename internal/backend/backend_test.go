package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"temporaless"

	"go.temporal.io/sdk/temporal"
)

func TestRevisionInvokePreservesHTTPEnvelope(t *testing.T) {
	t.Parallel()

	requestBody := []byte("{\n  \"name\": \"Ada\"\n}")
	responseBody := []byte("{ \"ok\": true }")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.EscapedPath() != "/jobs/a%2Fb" {
			t.Errorf("path = %q, want /jobs/a%%2Fb", r.URL.EscapedPath())
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("X-Hop"); got != "" {
			t.Errorf("hop-by-hop request header forwarded: %q", got)
		}
		gotBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		if string(gotBody) != string(requestBody) {
			t.Errorf("body = %q, want %q", gotBody, requestBody)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Connection", "X-Hop")
		w.Header().Set("X-Hop", "remove me")
		w.Header().Set("X-Keep", "yes")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write(responseBody)
	}))
	defer server.Close()

	revision, err := Compile(temporaless.ServiceSpec{
		Timeout: "5s",
		Actions: map[string]temporaless.ActionSpec{
			"status": {
				Method:        http.MethodPost,
				Endpoint:      server.URL + "/jobs/{job_id}",
				RequestSchema: []byte(`{"type":"object","required":["name"]}`),
				ResponseSchemas: map[string]json.RawMessage{
					"418": json.RawMessage(`{"type":"object","required":["ok"]}`),
				},
			},
		},
	}, server.Client())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	response, err := revision.Invoke(context.Background(), temporaless.Request{
		Action: "status",
		JobID:  "a/b",
		Headers: http.Header{
			"Authorization": {"Bearer secret"},
			"content-type":  {"application/json"},
			"connection":    {"x-hop"},
			"x-hop":         {"remove me"},
		},
		Body: requestBody,
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if response.Status != http.StatusTeapot || response.Message != "I'm a teapot" {
		t.Fatalf("status/message = %d/%q", response.Status, response.Message)
	}
	if string(response.Body) != string(responseBody) {
		t.Fatalf("response body = %q, want %q", response.Body, responseBody)
	}
	if response.Headers.Get("X-Keep") != "yes" || response.Headers.Get("X-Hop") != "" {
		t.Fatalf("response headers = %#v", response.Headers)
	}
}

func TestRevisionInvokeIsolatedAcrossConcurrentRequests(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		_, _ = fmt.Fprintf(response, "%s|%s|%s", request.URL.EscapedPath(), request.Header.Get("X-Request-ID"), body)
	}))
	defer server.Close()
	revision, err := Compile(temporaless.ServiceSpec{
		Timeout: "5s",
		Actions: map[string]temporaless.ActionSpec{
			"status": {Method: http.MethodPost, Endpoint: server.URL + "/jobs/{job_id}"},
		},
	}, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	const callers = 32
	start := make(chan struct{})
	errors := make(chan error, callers)
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for index := range callers {
		go func() {
			defer callersDone.Done()
			<-start
			id := fmt.Sprintf("request-%d", index)
			body := []byte("body-" + id)
			response, err := revision.Invoke(context.Background(), temporaless.Request{
				Action:  "status",
				JobID:   "job/" + id,
				Headers: http.Header{"X-Request-ID": {id}},
				Body:    body,
			})
			if err != nil {
				errors <- err
				return
			}
			want := fmt.Sprintf("/jobs/job%%2F%s|%s|%s", id, id, body)
			if string(response.Body) != want {
				errors <- fmt.Errorf("response = %q, want %q", response.Body, want)
			}
		}()
	}
	close(start)
	callersDone.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func TestRevisionInvokeRejectsInvalidInputBeforeHTTP(t *testing.T) {
	t.Parallel()

	called := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	defer server.Close()

	revision, err := Compile(temporaless.ServiceSpec{
		Timeout: "5s",
		Actions: map[string]temporaless.ActionSpec{
			"invoke": {
				Method:        http.MethodPost,
				Endpoint:      server.URL,
				RequestSchema: []byte(`{"type":"object","required":["name"]}`),
			},
		},
	}, server.Client())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = revision.Invoke(context.Background(), temporaless.Request{
		Action:  "invoke",
		Headers: http.Header{"content-type": {"application/json"}},
		Body:    []byte(`{}`),
	})
	assertApplicationError(t, err, "RequestSchemaMismatch")
	if called {
		t.Fatal("backend was called for invalid input")
	}

	_, err = revision.Invoke(context.Background(), temporaless.Request{Action: "missing"})
	assertApplicationError(t, err, "ActionNotFound")

	withJobID, err := Compile(temporaless.ServiceSpec{
		Timeout: "5s",
		Actions: map[string]temporaless.ActionSpec{
			"status": {Method: http.MethodGet, Endpoint: server.URL + "/{job_id}"},
		},
	}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = withJobID.Invoke(context.Background(), temporaless.Request{Action: "status"})
	assertApplicationError(t, err, "InvalidRequest")
}

func TestRevisionInvokeRejectsInvalidResponseSchema(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	revision, err := Compile(temporaless.ServiceSpec{
		Timeout: "5s",
		Actions: map[string]temporaless.ActionSpec{
			"invoke": {
				Method:   http.MethodPost,
				Endpoint: server.URL,
				ResponseSchemas: map[string]json.RawMessage{
					"200": json.RawMessage(`{"type":"object","required":["ok"]}`),
				},
			},
		},
	}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = revision.Invoke(context.Background(), temporaless.Request{Action: "invoke"})
	assertApplicationError(t, err, "ResponseSchemaMismatch")
}

func TestCompileRejectsInvalidServiceSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec temporaless.ServiceSpec
	}{
		{name: "no actions", spec: temporaless.ServiceSpec{Timeout: "1s"}},
		{name: "zero timeout", spec: temporaless.ServiceSpec{Timeout: "0s", Actions: validActions()}},
		{name: "negative timeout", spec: temporaless.ServiceSpec{Timeout: "-1s", Actions: validActions()}},
		{name: "bad timeout", spec: temporaless.ServiceSpec{Timeout: "later", Actions: validActions()}},
		{name: "non http endpoint", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "POST", Endpoint: "file:///tmp/x"}}}},
		{name: "relative endpoint", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "POST", Endpoint: "/run"}}}},
		{name: "missing host", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "POST", Endpoint: "https:///run"}}}},
		{name: "unknown placeholder", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "GET", Endpoint: "https://example.com/{other}"}}}},
		{name: "placeholder in query", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "GET", Endpoint: "https://example.com/?id={job_id}"}}}},
		{name: "bad response status", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "GET", Endpoint: "https://example.com", ResponseSchemas: map[string]json.RawMessage{"2xx": json.RawMessage(`{}`)}}}}},
		{name: "low response status", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "GET", Endpoint: "https://example.com", ResponseSchemas: map[string]json.RawMessage{"99": json.RawMessage(`{}`)}}}}},
		{name: "high response status", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "GET", Endpoint: "https://example.com", ResponseSchemas: map[string]json.RawMessage{"600": json.RawMessage(`{}`)}}}}},
		{name: "non canonical response status", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "GET", Endpoint: "https://example.com", ResponseSchemas: map[string]json.RawMessage{"0200": json.RawMessage(`{}`)}}}}},
		{name: "multiple schema values", spec: temporaless.ServiceSpec{Timeout: "1s", Actions: map[string]temporaless.ActionSpec{"invoke": {Method: "GET", Endpoint: "https://example.com", RequestSchema: json.RawMessage(`{} {}`)}}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Compile(test.spec, http.DefaultClient); err == nil {
				t.Fatal("Compile succeeded, want error")
			}
		})
	}
}

func TestCompileCanonicalizesEquivalentSpecs(t *testing.T) {
	t.Parallel()

	first, err := Compile(temporaless.ServiceSpec{
		Timeout: "60s",
		Actions: map[string]temporaless.ActionSpec{
			"invoke": {Method: " post ", Endpoint: " https://example.com/run ", RequestSchema: json.RawMessage(`{ "type": "object", "properties": {"x": {"type":"string"}} }`)},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Compile(temporaless.ServiceSpec{
		Timeout: "1m",
		Actions: map[string]temporaless.ActionSpec{
			"invoke": {Method: "POST", Endpoint: "https://example.com/run", RequestSchema: json.RawMessage(`{"properties":{"x":{"type":"string"}},"type":"object"}`)},
		},
	}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if first.Hash() != second.Hash() || string(first.SpecJSON()) != string(second.SpecJSON()) {
		t.Fatalf("canonical specs differ:\n%s\n%s", first.SpecJSON(), second.SpecJSON())
	}
	if first.client != http.DefaultClient {
		t.Fatal("nil client did not use http.DefaultClient")
	}
}

func validActions() map[string]temporaless.ActionSpec {
	return map[string]temporaless.ActionSpec{
		"invoke": {Method: http.MethodPost, Endpoint: "https://example.com"},
	}
}

func assertApplicationError(t *testing.T, err error, wantType string) {
	t.Helper()

	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) {
		t.Fatalf("error = %T %v, want ApplicationError", err, err)
	}
	if applicationError.Type() != wantType || !applicationError.NonRetryable() {
		t.Fatalf("application error = %q nonRetryable=%v", applicationError.Type(), applicationError.NonRetryable())
	}
}

func TestRevisionInvokeHonorsTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(time.Second):
		case <-r.Context().Done():
		}
	}))
	defer server.Close()

	revision, err := Compile(temporaless.ServiceSpec{
		Timeout: "20ms",
		Actions: map[string]temporaless.ActionSpec{
			"invoke": {Method: http.MethodPost, Endpoint: server.URL},
		},
	}, server.Client())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, err := revision.Invoke(context.Background(), temporaless.Request{Action: "invoke"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
}

func TestRevisionInvokePropagatesCancellation(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	revision, err := Compile(temporaless.ServiceSpec{
		Timeout: "5s",
		Actions: map[string]temporaless.ActionSpec{
			"invoke": {Method: http.MethodPost, Endpoint: server.URL},
		},
	}, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := revision.Invoke(ctx, temporaless.Request{Action: "invoke"})
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
}
