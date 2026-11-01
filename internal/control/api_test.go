package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"temporaless/internal/registry"

	"gopkg.in/yaml.v3"
)

func TestAPIServiceLifecycle(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	runtime := &fakeRuntime{activities: make(map[string]Activity)}
	manager := NewManager(store, runtime, http.DefaultClient, ManagerOptions{})
	t.Cleanup(manager.Close)
	handler := NewAPI(manager, slog.New(slog.NewTextHandler(io.Discard, nil)))

	put := request(t, handler, http.MethodPut, "/services/image-generate", `{
		"actions":{"invoke":{"method":"POST","endpoint":"https://example.com/run"}},
		"timeout":"5s"
	}`)
	if put.Code != http.StatusOK {
		t.Fatalf("PUT status = %d body=%s", put.Code, put.Body.String())
	}
	var created ServiceView
	decodeResponse(t, put, &created)
	if created.ID != "image-generate" || created.CurrentRevision != 1 {
		t.Fatalf("created = %#v", created)
	}

	get := request(t, handler, http.MethodGet, "/services/image-generate", "")
	if get.Code != http.StatusOK {
		t.Fatalf("GET status = %d body=%s", get.Code, get.Body.String())
	}
	health := request(t, handler, http.MethodGet, "/services/image-generate/health", "")
	var healthView ServiceHealthView
	decodeResponse(t, health, &healthView)
	if health.Code != http.StatusOK || healthView.Health != "not_configured" {
		t.Fatalf("health without config = %d %#v", health.Code, healthView)
	}

	list := request(t, handler, http.MethodGet, "/services", "")
	var services []ServiceView
	decodeResponse(t, list, &services)
	if len(services) != 1 || services[0].ID != "image-generate" {
		t.Fatalf("services = %#v", services)
	}

	deleted := request(t, handler, http.MethodDelete, "/services/image-generate", "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("DELETE status = %d body=%s", deleted.Code, deleted.Body.String())
	}
	var deletedView ServiceView
	decodeResponse(t, deleted, &deletedView)
	if deletedView.State != registry.ServiceDeleted {
		t.Fatalf("deleted = %#v", deletedView)
	}
}

func TestAPIRejectsInvalidInputAndMissingService(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{})
	t.Cleanup(manager.Close)
	handler := NewAPI(manager, slog.New(slog.NewTextHandler(io.Discard, nil)))

	invalid := request(t, handler, http.MethodPut, "/services/service-a", `{
		"actions":{"invoke":{"method":"POST","endpoint":"https://example.com"}},
		"timeout":"5s",
		"unknown":true
	}`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d body=%s", invalid.Code, invalid.Body.String())
	}

	missing := request(t, handler, http.MethodGet, "/services/missing", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d body=%s", missing.Code, missing.Body.String())
	}
}

func TestAPIPutValidatesContentTypeAndJSONDocument(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{})
	t.Cleanup(manager.Close)
	handler := NewAPI(manager, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, test := range []struct {
		name, contentType, body string
		wantCode                int
		wantMessage             string
	}{
		{name: "missing content type", body: `{}`, wantCode: http.StatusUnsupportedMediaType},
		{name: "structured json media type", contentType: "application/problem+json", body: `{"actions":{"invoke":{"method":"POST","endpoint":"https://example.com"}},"timeout":"1s"}`, wantCode: http.StatusOK},
		{name: "malformed json", contentType: "application/json", body: `{`, wantCode: http.StatusBadRequest},
		{name: "multiple documents", contentType: "application/json", body: `{} {}`, wantCode: http.StatusBadRequest, wantMessage: "invalid JSON: multiple JSON values"},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/services/service-a", bytes.NewBufferString(test.body))
			if test.contentType != "" {
				req.Header.Set("Content-Type", test.contentType)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if got := response.Code; got != test.wantCode {
				t.Fatalf("status = %d, want %d: %s", got, test.wantCode, response.Body.String())
			}
			if test.wantCode == http.StatusOK {
				return
			}
			var payload struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatalf("error payload %q: %v", response.Body.String(), err)
			}
			want := "invalid_request"
			if test.wantCode == http.StatusUnsupportedMediaType {
				want = "unsupported_media_type"
			}
			if payload.Error.Code != want {
				t.Fatalf("error code = %q, want %q", payload.Error.Code, want)
			}
			if test.wantCode == http.StatusBadRequest && !strings.HasPrefix(payload.Error.Message, "invalid JSON:") {
				t.Fatalf("error message = %q, want JSON decoding error", payload.Error.Message)
			}
			if test.wantMessage != "" && payload.Error.Message != test.wantMessage {
				t.Fatalf("error message = %q, want %q", payload.Error.Message, test.wantMessage)
			}
		})
	}
}

func TestAPIPutRejectsOversizedSpec(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{})
	t.Cleanup(manager.Close)
	handler := NewAPI(manager, slog.New(slog.NewTextHandler(io.Discard, nil)))
	body := bytes.Repeat([]byte(" "), maxServiceSpecBytes+1)
	req := httptest.NewRequest(http.MethodPut, "/services/service-a", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
	if _, err := store.Get(context.Background(), "service-a"); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("oversized payload created service: %v", err)
	}
}

func TestAPIHealth(t *testing.T) {
	t.Parallel()

	store := openRegistry(t)
	manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{})
	t.Cleanup(manager.Close)
	handler := NewAPI(manager, slog.New(slog.NewTextHandler(io.Discard, nil)))
	response := request(t, handler, http.MethodGet, "/healthz", "")
	if response.Code != http.StatusOK || response.Body.String() != "{\"status\":\"ok\"}\n" {
		t.Fatalf("health = %d %q", response.Code, response.Body.String())
	}
}

func TestAPIDocumentation(t *testing.T) {
	t.Parallel()

	handler := NewAPI(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	openAPI := request(t, handler, http.MethodGet, "/openapi.yaml", "")
	if openAPI.Code != http.StatusOK || openAPI.Header().Get("Content-Type") != "application/yaml" || !strings.Contains(openAPI.Body.String(), "openapi: 3.1.0") {
		t.Fatalf("OpenAPI response = %d %q", openAPI.Code, openAPI.Body.String())
	}
	var document map[string]any
	if err := yaml.Unmarshal(openAPI.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode OpenAPI: %v", err)
	}
	paths, ok := document["paths"].(map[string]any)
	if !ok {
		t.Fatalf("OpenAPI paths = %#v", document["paths"])
	}
	for _, path := range []string{"/services", "/services/{id}", "/services/{id}/health", "/healthz"} {
		if _, ok := paths[path]; !ok {
			t.Errorf("OpenAPI missing path %s", path)
		}
	}
	swagger := request(t, handler, http.MethodGet, "/docs", "")
	if swagger.Code != http.StatusOK || !strings.Contains(swagger.Body.String(), "SwaggerUIBundle") {
		t.Fatalf("Swagger response = %d %q", swagger.Code, swagger.Body.String())
	}
}

func TestAPIServiceHealth(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		backend    int
		wantStatus int
		wantHealth string
	}{
		{name: "healthy", backend: http.StatusNoContent, wantStatus: http.StatusOK, wantHealth: "healthy"},
		{name: "unhealthy", backend: http.StatusTeapot, wantStatus: http.StatusServiceUnavailable, wantHealth: "unhealthy"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var method string
			backendServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				method = request.Method
				response.WriteHeader(test.backend)
			}))
			defer backendServer.Close()

			store := openRegistry(t)
			manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, backendServer.Client(), ManagerOptions{})
			t.Cleanup(manager.Close)
			var logs bytes.Buffer
			handler := NewAPI(manager, slog.New(slog.NewTextHandler(&logs, nil)))
			put := request(t, handler, http.MethodPut, "/services/service-a", fmt.Sprintf(`{
				"actions":{"invoke":{"method":"POST","endpoint":"https://example.com"}},
				"health":{"method":"post","endpoint":%q,"timeout":"2s"},
				"timeout":"5s"
			}`, backendServer.URL))
			if put.Code != http.StatusOK {
				t.Fatalf("PUT status = %d body=%s", put.Code, put.Body.String())
			}

			response := request(t, handler, http.MethodGet, "/services/service-a/health", "")
			if response.Code != test.wantStatus {
				t.Fatalf("health status = %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
			var health ServiceHealthView
			decodeResponse(t, response, &health)
			if health.Health != test.wantHealth || health.HTTPStatus != test.backend || health.LatencyMS <= 0 {
				t.Fatalf("health = %#v", health)
			}
			if method != http.MethodPost {
				t.Fatalf("health method = %q, want POST", method)
			}
			if !strings.Contains(logs.String(), "latency_ms=") {
				t.Fatalf("health log missing latency: %s", logs.String())
			}
		})
	}
}

func request(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewBufferString(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}
