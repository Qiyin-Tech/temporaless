package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"temporaless/internal/registry"
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
