package control

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"temporaless/internal/registry"
)

func FuzzControlAPIRejectsInvalidStateWithoutPanicking(f *testing.F) {
	for _, seed := range []struct {
		method, path, contentType, header, body string
	}{
		{http.MethodPut, "service-a", "application/json", "", `{"actions":{"invoke":{"method":"POST","endpoint":"https://example.com"}},"timeout":"1s"}`},
		{http.MethodPut, "contains.dot", "application/json", "x", `{}`},
		{http.MethodPut, "service-a", "text/plain", "", `{}`},
		{http.MethodGet, "missing", "", "a\rb", ""},
		{http.MethodDelete, "service-a/child", "", "", ""},
		{http.MethodOptions, "service-a", "application/json", "", "{}"},
	} {
		f.Add(seed.method, seed.path, seed.contentType, seed.header, seed.body)
	}

	store, err := registry.Open(":memory:")
	if err != nil {
		f.Fatalf("open registry: %v", err)
	}
	defer store.Close()
	manager := NewManager(store, &fakeRuntime{activities: make(map[string]Activity)}, http.DefaultClient, ManagerOptions{})
	defer manager.Close()
	handler := NewAPI(manager, slog.New(slog.NewTextHandler(io.Discard, nil)))

	f.Fuzz(func(t *testing.T, method, path, contentType, headerValue, body string) {
		if len(method)+len(path)+len(contentType)+len(headerValue)+len(body) > 8192 {
			t.Skip()
		}
		switch method {
		case http.MethodPut, http.MethodGet, http.MethodDelete, http.MethodHead, http.MethodOptions:
		default:
			method = http.MethodGet
		}
		// Keep long fuzz runs bounded while still fuzzing arbitrary paths on read-only requests.
		if method == http.MethodPut && path != "service-a" {
			method = http.MethodGet
		}
		req := &http.Request{
			Method: method,
			URL:    &url.URL{Path: "/services/" + path},
			Header: http.Header{"Content-Type": []string{contentType}, "X-Fuzz": []string{strings.ToValidUTF8(headerValue, "?")}},
			Body:   io.NopCloser(bytes.NewReader([]byte(body))),
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code < 200 || response.Code > 599 {
			t.Fatalf("invalid HTTP status %d", response.Code)
		}
		services, err := manager.List(context.Background())
		if err != nil {
			t.Fatalf("list after request: %v", err)
		}
		for _, service := range services {
			if err := validateServiceID(service.ID); err != nil {
				t.Fatalf("invalid service persisted: %q", service.ID)
			}
			switch service.State {
			case registry.ServiceActive:
				if service.CurrentRevision == 0 || service.RevisionKind != registry.KindService || service.RevisionState != registry.RevisionActive {
					t.Fatalf("inconsistent active service: %#v", service)
				}
			case registry.ServiceDeleted:
				if service.CurrentRevision == 0 || service.RevisionKind != registry.KindTombstone || service.RevisionState != registry.RevisionActive {
					t.Fatalf("inconsistent deleted service: %#v", service)
				}
			case registry.ServiceFailed, registry.ServiceStarting:
			default:
				t.Fatalf("unknown persisted state: %#v", service)
			}
		}
	})
}
