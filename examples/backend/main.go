package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type invokeRequest struct {
	Prompt string `json:"prompt"`
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /runsync", invoke)
	mux.HandleFunc("GET /jobs/{id}", status)

	server := &http.Server{
		Addr:              "127.0.0.1:18080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("demo backend listening on http://%s", server.Addr)
	log.Fatal(server.ListenAndServe())
}

func invoke(response http.ResponseWriter, request *http.Request) {
	if request.Header.Get("Authorization") == "" || request.Header.Get("Idempotency-Key") == "" {
		writeJSON(response, http.StatusUnauthorized, map[string]string{"error": "Authorization and Idempotency-Key are required"})
		return
	}
	var input invokeRequest
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || strings.TrimSpace(input.Prompt) == "" {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "prompt is required"})
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "request must contain one JSON document"})
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{
		"job_id": request.Header.Get("Idempotency-Key"),
		"status": "completed",
		"output": "generated: " + input.Prompt,
	})
}

func status(response http.ResponseWriter, request *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{
		"job_id": request.PathValue("id"),
		"status": "completed",
	})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
