package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"temporaless"
	"temporaless/internal/httpx"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.temporal.io/sdk/temporal"
)

// DefaultMaxResponseBodyBytes is the default in-memory backend response limit.
const DefaultMaxResponseBodyBytes int64 = 1 << 20

var actionNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// Revision is one compiled, immutable service configuration.
type Revision struct {
	actions              map[string]action
	timeout              time.Duration
	client               *http.Client
	maxResponseBodyBytes int64
	specJSON             []byte
	hash                 string
}

// HealthProbe executes one compiled direct HTTP backend health check.
type HealthProbe struct {
	method   string
	endpoint string
	timeout  time.Duration
	client   *http.Client
}

// HealthProbeResult reports the backend status and observed request latency.
type HealthProbeResult struct {
	Status  int
	Message string
	Latency time.Duration
}

type action struct {
	method          string
	endpoint        string
	hasJobID        bool
	requestSchema   *jsonschema.Schema
	responseSchemas map[int]*jsonschema.Schema
}

// Compile validates and normalizes a ServiceSpec into an immutable Revision.
func Compile(spec temporaless.ServiceSpec, client *http.Client, maxResponseBodyBytes int64) (*Revision, error) {
	if maxResponseBodyBytes <= 0 {
		return nil, errors.New("max response body bytes must be positive")
	}
	timeout, err := time.ParseDuration(spec.Timeout)
	if err != nil || timeout <= 0 {
		return nil, fmt.Errorf("timeout must be a positive Go duration")
	}
	if len(spec.Actions) == 0 {
		return nil, errors.New("at least one action is required")
	}

	normalized := temporaless.ServiceSpec{Actions: make(map[string]temporaless.ActionSpec, len(spec.Actions)), Timeout: timeout.String()}
	actions := make(map[string]action, len(spec.Actions))
	for name, actionSpec := range spec.Actions {
		compiled, normalizedSpec, err := compileAction(name, actionSpec)
		if err != nil {
			return nil, fmt.Errorf("action %q: %w", name, err)
		}
		actions[name] = compiled
		normalized.Actions[name] = normalizedSpec
	}
	health, err := normalizeHealthSpec(spec.Health)
	if err != nil {
		return nil, fmt.Errorf("health: %w", err)
	}
	normalized.Health = health

	specJSON, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("encode normalized spec: %w", err)
	}
	sum := sha256.Sum256(specJSON)
	if client == nil {
		client = http.DefaultClient
	}
	return &Revision{
		actions:              actions,
		timeout:              timeout,
		client:               client,
		maxResponseBodyBytes: maxResponseBodyBytes,
		specJSON:             specJSON,
		hash:                 hex.EncodeToString(sum[:]),
	}, nil
}

// SpecJSON returns a copy of the normalized ServiceSpec JSON.
func (r *Revision) SpecJSON() []byte {
	return bytes.Clone(r.specJSON)
}

// Hash returns the normalized ServiceSpec content hash.
func (r *Revision) Hash() string {
	return r.hash
}

// Invoke validates and forwards one Activity request to its configured HTTP action.
func (r *Revision) Invoke(ctx context.Context, request temporaless.Request) (temporaless.Response, error) {
	action, ok := r.actions[request.Action]
	if !ok {
		return temporaless.Response{}, temporal.NewNonRetryableApplicationError("action not found", "ActionNotFound", nil)
	}
	endpoint, err := action.endpointFor(request.JobID)
	if err != nil {
		return temporaless.Response{}, err
	}
	requestHeaders := filterHeaders(request.Headers)
	if err := validateBody(action.requestSchema, requestHeaders.Get("Content-Type"), request.Body); err != nil {
		return temporaless.Response{}, temporal.NewNonRetryableApplicationError("request body does not match schema", "RequestSchemaMismatch", nil)
	}

	requestContext, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestContext, action.method, endpoint, bytes.NewReader(request.Body))
	if err != nil {
		return temporaless.Response{}, fmt.Errorf("create backend request: %w", err)
	}
	httpRequest.Header = requestHeaders

	httpResponse, err := r.client.Do(httpRequest)
	if err != nil {
		return temporaless.Response{}, fmt.Errorf("call backend: %w", err)
	}
	defer httpResponse.Body.Close()
	body, err := io.ReadAll(io.LimitReader(httpResponse.Body, r.maxResponseBodyBytes+1))
	if err != nil {
		return temporaless.Response{}, fmt.Errorf("read backend response: %w", err)
	}
	if int64(len(body)) > r.maxResponseBodyBytes {
		return temporaless.Response{}, temporal.NewNonRetryableApplicationError(fmt.Sprintf("backend response body exceeds %d bytes", r.maxResponseBodyBytes), "ResponseTooLarge", nil)
	}
	if err := validateBody(action.responseSchemas[httpResponse.StatusCode], httpResponse.Header.Get("Content-Type"), body); err != nil {
		return temporaless.Response{}, temporal.NewNonRetryableApplicationError("response body does not match schema", "ResponseSchemaMismatch", nil)
	}
	return temporaless.Response{
		Status:  httpResponse.StatusCode,
		Message: http.StatusText(httpResponse.StatusCode),
		Headers: filterHeaders(httpResponse.Header),
		Body:    body,
	}, nil
}

func compileAction(name string, spec temporaless.ActionSpec) (action, temporaless.ActionSpec, error) {
	if !actionNamePattern.MatchString(name) {
		return action{}, temporaless.ActionSpec{}, errors.New("name must match ^[a-z0-9][a-z0-9._-]{0,127}$")
	}
	method, err := normalizeMethod(spec.Method)
	if err != nil {
		return action{}, temporaless.ActionSpec{}, err
	}
	endpointText, hasJobID, err := normalizeEndpoint(spec.Endpoint)
	if err != nil {
		return action{}, temporaless.ActionSpec{}, err
	}

	requestSchema, normalizedRequest, err := compileSchema(spec.RequestSchema, "request-schema.json")
	if err != nil {
		return action{}, temporaless.ActionSpec{}, fmt.Errorf("request_schema: %w", err)
	}
	responseSchemas, normalizedResponses, err := compileResponseSchemas(spec.ResponseSchemas)
	if err != nil {
		return action{}, temporaless.ActionSpec{}, err
	}

	return action{
			method:          method,
			endpoint:        endpointText,
			hasJobID:        hasJobID,
			requestSchema:   requestSchema,
			responseSchemas: responseSchemas,
		}, temporaless.ActionSpec{
			Method:          method,
			Endpoint:        endpointText,
			RequestSchema:   normalizedRequest,
			ResponseSchemas: normalizedResponses,
		}, nil
}

// NewHealthProbe validates HealthSpec and returns an executable probe.
func NewHealthProbe(spec temporaless.HealthSpec, client *http.Client) (*HealthProbe, error) {
	normalized, err := normalizeHealthSpec(&spec)
	if err != nil {
		return nil, err
	}
	timeout, _ := time.ParseDuration(normalized.Timeout)
	if client == nil {
		client = http.DefaultClient
	}
	return &HealthProbe{
		method:   normalized.Method,
		endpoint: normalized.Endpoint,
		timeout:  timeout,
		client:   client,
	}, nil
}

// Check calls the configured endpoint and returns its final HTTP status and latency.
func (p *HealthProbe) Check(ctx context.Context) (HealthProbeResult, error) {
	started := time.Now()
	requestCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, p.method, p.endpoint, nil)
	if err != nil {
		return HealthProbeResult{Latency: time.Since(started)}, err
	}
	response, err := p.client.Do(request)
	latency := time.Since(started)
	if err != nil {
		return HealthProbeResult{Latency: latency}, err
	}
	defer response.Body.Close()
	return HealthProbeResult{
		Status:  response.StatusCode,
		Message: http.StatusText(response.StatusCode),
		Latency: latency,
	}, nil
}

func normalizeHealthSpec(spec *temporaless.HealthSpec) (*temporaless.HealthSpec, error) {
	if spec == nil {
		return nil, nil
	}
	method, err := normalizeMethod(spec.Method)
	if err != nil {
		return nil, err
	}
	endpoint, hasJobID, err := normalizeEndpoint(spec.Endpoint)
	if err != nil {
		return nil, err
	}
	if hasJobID {
		return nil, errors.New("{job_id} is not allowed in health endpoint")
	}
	timeout, err := time.ParseDuration(spec.Timeout)
	if err != nil || timeout <= 0 {
		return nil, errors.New("timeout must be a positive Go duration")
	}
	return &temporaless.HealthSpec{Method: method, Endpoint: endpoint, Timeout: timeout.String()}, nil
}

func normalizeMethod(raw string) (string, error) {
	method := strings.ToUpper(strings.TrimSpace(raw))
	if method == "" {
		return "", errors.New("method is required")
	}
	if _, err := http.NewRequest(method, "http://example.com", nil); err != nil {
		return "", fmt.Errorf("invalid method: %w", err)
	}
	return method, nil
}

func normalizeEndpoint(raw string) (string, bool, error) {
	endpointText := strings.TrimSpace(raw)
	endpoint, err := url.Parse(endpointText)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
		return "", false, errors.New("endpoint must be an absolute http or https URL")
	}
	withoutJobID := strings.ReplaceAll(endpointText, "{job_id}", "")
	if strings.ContainsAny(withoutJobID, "{}") {
		return "", false, errors.New("endpoint contains an unsupported placeholder")
	}
	pathPlaceholders := strings.Count(endpoint.Path, "{job_id}")
	if strings.Count(endpointText, "{job_id}") != pathPlaceholders {
		return "", false, errors.New("{job_id} is only allowed in the URL path")
	}
	return endpointText, pathPlaceholders > 0, nil
}

func compileResponseSchemas(rawSchemas map[string]json.RawMessage) (map[int]*jsonschema.Schema, map[string]json.RawMessage, error) {
	compiled := make(map[int]*jsonschema.Schema, len(rawSchemas))
	normalized := make(map[string]json.RawMessage, len(rawSchemas))
	for statusText, rawSchema := range rawSchemas {
		status, err := strconv.Atoi(statusText)
		if err != nil || status < 100 || status > 599 || strconv.Itoa(status) != statusText {
			return nil, nil, fmt.Errorf("response status %q must be a decimal HTTP status", statusText)
		}
		schema, normalizedSchema, err := compileSchema(rawSchema, "response-"+statusText+"-schema.json")
		if err != nil {
			return nil, nil, fmt.Errorf("response_schemas[%s]: %w", statusText, err)
		}
		compiled[status] = schema
		normalized[statusText] = normalizedSchema
	}
	if len(normalized) == 0 {
		normalized = nil
	}
	return compiled, normalized, nil
}

func (a action) endpointFor(jobID string) (string, error) {
	if !a.hasJobID {
		return a.endpoint, nil
	}
	if jobID == "" {
		return "", temporal.NewNonRetryableApplicationError("job_id is required", "InvalidRequest", nil)
	}
	return strings.ReplaceAll(a.endpoint, "{job_id}", url.PathEscape(jobID)), nil
}

func compileSchema(raw json.RawMessage, resource string) (*jsonschema.Schema, json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil, nil
	}
	value, err := decodeJSON(raw)
	if err != nil {
		return nil, nil, err
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(jsonschema.SchemeURLLoader{})
	if err := compiler.AddResource(resource, value); err != nil {
		return nil, nil, err
	}
	schema, err := compiler.Compile(resource)
	if err != nil {
		return nil, nil, err
	}
	return schema, normalized, nil
}

func validateBody(schema *jsonschema.Schema, contentType string, body []byte) error {
	if schema == nil || !httpx.IsJSONContentType(contentType) {
		return nil
	}
	value, err := decodeJSON(body)
	if err != nil {
		return err
	}
	return schema.Validate(value)
}

func decodeJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

var hopByHopHeaders = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
	"Content-Length":      {},
	"Host":                {},
}

func filterHeaders(source http.Header) http.Header {
	blocked := make(map[string]struct{}, len(hopByHopHeaders))
	for name := range hopByHopHeaders {
		blocked[http.CanonicalHeaderKey(name)] = struct{}{}
	}
	for name, values := range source {
		if strings.EqualFold(name, "Connection") {
			for _, value := range values {
				for token := range strings.SplitSeq(value, ",") {
					blocked[http.CanonicalHeaderKey(strings.TrimSpace(token))] = struct{}{}
				}
			}
		}
	}

	filtered := make(http.Header, len(source))
	for name, values := range source {
		name = http.CanonicalHeaderKey(name)
		if _, remove := blocked[name]; remove {
			continue
		}
		filtered[name] = append([]string(nil), values...)
	}
	return filtered
}
