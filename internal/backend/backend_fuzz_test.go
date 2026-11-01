package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"

	"temporaless"
	"temporaless/internal/httpx"
)

func FuzzCompileAndJobIDReplacementPreserveURLAuthority(f *testing.F) {
	for _, seed := range [][4]string{
		{"POST", "https://example.com/jobs/{job_id}", "5s", "job/one"},
		{" get ", "http://localhost:8080/{job_id}?fixed=1", "1m", "a b?c"},
		{"GET", "https://example.com/{unknown}", "0s", ""},
		{"", "not a URL", "later", ""},
	} {
		f.Add(seed[0], seed[1], seed[2], seed[3], `{"type":"object"}`, "application/json; charset=utf-8")
	}
	f.Fuzz(func(t *testing.T, method, endpoint, timeout, jobID, schema, contentType string) {
		if len(method)+len(endpoint)+len(timeout)+len(jobID)+len(schema)+len(contentType) > 8192 {
			t.Skip()
		}
		if !utf8.ValidString(method) || !utf8.ValidString(endpoint) || !utf8.ValidString(timeout) || !utf8.ValidString(schema) {
			return
		}
		_ = httpx.IsJSONContentType(contentType)
		revision, err := Compile(temporaless.ServiceSpec{
			Timeout: timeout,
			Actions: map[string]temporaless.ActionSpec{
				"invoke": {Method: method, Endpoint: endpoint, RequestSchema: json.RawMessage(schema)},
			},
		}, http.DefaultClient, DefaultMaxResponseBodyBytes)
		if err != nil {
			return
		}

		var normalized temporaless.ServiceSpec
		if err := json.Unmarshal(revision.SpecJSON(), &normalized); err != nil {
			t.Fatalf("normalized spec is invalid JSON: %v", err)
		}
		again, err := Compile(normalized, http.DefaultClient, DefaultMaxResponseBodyBytes)
		if err != nil || again.Hash() != revision.Hash() {
			t.Fatalf("normalized config did not round trip: compile err=%v", err)
		}

		action := revision.actions["invoke"]
		original, err := url.Parse(action.endpoint)
		if err != nil {
			t.Fatalf("normalized endpoint is invalid: %v", err)
		}
		replaced, err := action.endpointFor(jobID)
		if err != nil {
			return
		}
		parsed, err := url.Parse(replaced)
		if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
			t.Fatalf("accepted endpoint replacement is not absolute HTTP URL: %q, err=%v", replaced, err)
		}
		if parsed.Scheme != original.Scheme || parsed.Host != original.Host || parsed.User.String() != original.User.String() || parsed.RawQuery != original.RawQuery || parsed.Fragment != original.Fragment {
			t.Fatalf("job replacement changed URL outside path: before=%q after=%q", action.endpoint, replaced)
		}
		if action.hasJobID && jobID != "" && strings.Contains(replaced, "{job_id}") {
			t.Fatalf("job placeholder remains after replacement: %q", replaced)
		}
	})
}

func FuzzFilterHeadersRemovesConnectionNominatedHeaders(f *testing.F) {
	f.Add("X-Hop", "value", "keep")
	f.Add("x-custom", "", "")
	f.Add("", "Connection", "")
	f.Fuzz(func(t *testing.T, nominated, value, keep string) {
		if len(nominated)+len(value)+len(keep) > 4096 {
			t.Skip()
		}
		source := make(http.Header)
		source.Set("Connection", nominated)
		source.Set("X-Keep", keep)
		source.Set("X-Hop", value)
		filtered := filterHeaders(source)
		if filtered.Get("Connection") != "" {
			t.Fatal("Connection header was forwarded")
		}
		nominatesHop, nominatesKeep := false, false
		for token := range strings.SplitSeq(nominated, ",") {
			nominatesHop = nominatesHop || strings.EqualFold(strings.TrimSpace(token), "X-Hop")
			nominatesKeep = nominatesKeep || strings.EqualFold(strings.TrimSpace(token), "X-Keep")
		}
		if nominatesHop && filtered.Get("X-Hop") != "" {
			t.Fatal("Connection-nominated X-Hop header was forwarded")
		}
		if !nominatesKeep && filtered.Get("X-Keep") != keep {
			t.Fatalf("unrelated header = %q, want %q", filtered.Get("X-Keep"), keep)
		}
	})
}

func FuzzInvokePreservesHTTPResponseEnvelope(f *testing.F) {
	f.Add(200, "application/octet-stream", "response")
	f.Add(418, "application/problem+json", "{\"teapot\":true}")
	f.Add(503, "text/plain; charset=utf-8", "unavailable")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := 200
		if _, err := fmt.Sscanf(r.URL.Query().Get("status"), "%d", &status); err != nil || status < 200 || status > 599 {
			status = 200
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Envelope", "preserved")
		w.WriteHeader(status)
		if status != http.StatusNoContent && status != http.StatusResetContent && status != http.StatusNotModified {
			_, _ = w.Write(body)
		}
	}))
	f.Cleanup(server.Close)
	f.Fuzz(func(t *testing.T, status int, contentType, body string) {
		if len(contentType)+len(body) > 8192 {
			t.Skip()
		}
		if status < 200 || status > 599 {
			status = 200
		}
		endpoint := fmt.Sprintf("%s/echo?status=%d", server.URL, status)
		revision, err := Compile(temporaless.ServiceSpec{
			Timeout: "5s",
			Actions: map[string]temporaless.ActionSpec{
				"invoke": {Method: http.MethodPost, Endpoint: endpoint},
			},
		}, server.Client(), DefaultMaxResponseBodyBytes)
		if err != nil {
			t.Fatalf("compile echo action: %v", err)
		}
		requestBody := []byte(body)
		response, err := revision.Invoke(context.Background(), temporaless.Request{
			Action:  "invoke",
			Headers: http.Header{"X-Envelope": {"request"}, "Content-Type": {contentType}},
			Body:    requestBody,
		})
		if err != nil {
			return
		}
		wantBody := requestBody
		if status == http.StatusNoContent || status == http.StatusResetContent || status == http.StatusNotModified {
			wantBody = nil
		}
		if response.Status != status || response.Message != http.StatusText(status) || !bytes.Equal(response.Body, wantBody) || response.Headers.Get("X-Envelope") != "preserved" {
			t.Fatalf("response envelope status=%d/%d message=%q/%q header=%q body_len=%d/%d body_equal=%v", response.Status, status, response.Message, http.StatusText(status), response.Headers.Get("X-Envelope"), len(response.Body), len(wantBody), bytes.Equal(response.Body, wantBody))
		}
	})
}
