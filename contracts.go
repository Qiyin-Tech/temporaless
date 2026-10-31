package temporaless

import (
	"encoding/json"
	"net/http"
)

// ServiceSpec is the immutable HTTP configuration captured by one Worker revision.
type ServiceSpec struct {
	Actions map[string]ActionSpec `json:"actions"`
	Timeout string                `json:"timeout"`
}

type ActionSpec struct {
	Method          string                     `json:"method"`
	Endpoint        string                     `json:"endpoint"`
	RequestSchema   json.RawMessage            `json:"request_schema,omitempty"`
	ResponseSchemas map[string]json.RawMessage `json:"response_schemas,omitempty"`
}

// Request and Response are the stable Temporal Activity payload contract.
type Request struct {
	Action  string      `json:"action"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`
	JobID   string      `json:"job_id,omitempty"`
}

type Response struct {
	Status  int         `json:"status"`
	Message string      `json:"message"`
	Headers http.Header `json:"headers,omitempty"`
	Body    []byte      `json:"body,omitempty"`
}
