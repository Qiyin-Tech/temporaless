package temporaless

import (
	"encoding/json"
	"net/http"
)

// ServiceSpec is the immutable HTTP configuration captured by one Worker revision.
type ServiceSpec struct {
	Actions map[string]ActionSpec `json:"actions"`
	Health  *HealthSpec           `json:"health,omitempty"`
	Timeout string                `json:"timeout"`
}

// HealthSpec configures the optional direct HTTP backend health probe.
type HealthSpec struct {
	Method   string `json:"method"`
	Endpoint string `json:"endpoint"`
	Timeout  string `json:"timeout"`
}

// ActionSpec maps one logical Activity action to an HTTP request and optional schemas.
type ActionSpec struct {
	Method          string                     `json:"method"`
	Endpoint        string                     `json:"endpoint"`
	RequestSchema   json.RawMessage            `json:"request_schema,omitempty"`
	ResponseSchemas map[string]json.RawMessage `json:"response_schemas,omitempty"`
}

// Request is the stable, language-neutral JSON input contract for a Gateway Activity.
type Request struct {
	Action  string      `json:"action"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`
	JobID   string      `json:"job_id,omitempty"`
}

// Response is the stable, language-neutral JSON result contract for a Gateway Activity.
type Response struct {
	Status  int         `json:"status"`
	Message string      `json:"message"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`
}
