package control

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"temporaless"
	"temporaless/internal/httpx"
	"temporaless/internal/registry"
)

const maxServiceSpecBytes = 4 << 20

//go:embed openapi.yaml
var openAPISpec []byte

const swaggerUI = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>temporaless Control API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#swagger-ui"})</script>
</body>
</html>`

// API exposes the REST control plane, OpenAPI document, and Swagger UI.
type API struct {
	manager *Manager
	logger  *slog.Logger
	mux     *http.ServeMux
}

// NewAPI builds the complete REST control-plane handler.
func NewAPI(manager *Manager, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	api := &API{manager: manager, logger: logger, mux: http.NewServeMux()}
	api.mux.HandleFunc("PUT /services/{id}", api.putService)
	api.mux.HandleFunc("GET /services", api.listServices)
	api.mux.HandleFunc("GET /services/{id}/health", api.getServiceHealth)
	api.mux.HandleFunc("GET /services/{id}", api.getService)
	api.mux.HandleFunc("DELETE /services/{id}", api.deleteService)
	api.mux.HandleFunc("GET /healthz", api.health)
	api.mux.HandleFunc("GET /openapi.yaml", api.openAPI)
	api.mux.HandleFunc("GET /docs", api.swagger)
	api.mux.HandleFunc("GET /docs/", api.swagger)
	return api
}

func (a *API) openAPI(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "application/yaml")
	_, _ = response.Write(openAPISpec)
}

func (a *API) swagger(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(response, swaggerUI)
}

// ServeHTTP dispatches a control-plane request through the method-aware mux.
func (a *API) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	a.mux.ServeHTTP(response, request)
}

func (a *API) putService(response http.ResponseWriter, request *http.Request) {
	if !httpx.IsJSONContentType(request.Header.Get("Content-Type")) {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	var spec temporaless.ServiceSpec
	if err := decodeRequestJSON(response, request, &spec); err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	service, err := a.manager.Put(request.Context(), request.PathValue("id"), spec)
	if err != nil {
		a.handleError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, service)
}

func (a *API) listServices(response http.ResponseWriter, request *http.Request) {
	services, err := a.manager.List(request.Context())
	if err != nil {
		a.handleError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, services)
}

func (a *API) getService(response http.ResponseWriter, request *http.Request) {
	service, err := a.manager.Get(request.Context(), request.PathValue("id"))
	if err != nil {
		a.handleError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, service)
}

func (a *API) getServiceHealth(response http.ResponseWriter, request *http.Request) {
	health, err := a.manager.ServiceHealth(request.Context(), request.PathValue("id"))
	if err != nil {
		a.handleError(response, request, err)
		return
	}
	a.logger.InfoContext(request.Context(), "service health checked",
		"service_id", health.ID,
		"revision", health.Revision,
		"registry_state", health.RegistryState,
		"health", health.Health,
		"http_status", health.HTTPStatus,
		"latency_ms", health.LatencyMS,
		"message", health.Message,
	)
	status := http.StatusOK
	if health.Health == "unhealthy" || health.Health == "not_ready" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(response, status, health)
}

func (a *API) deleteService(response http.ResponseWriter, request *http.Request) {
	service, err := a.manager.Delete(request.Context(), request.PathValue("id"))
	if err != nil {
		a.handleError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, service)
}

func (a *API) health(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	if err := a.manager.Health(ctx); err != nil {
		a.logger.Error("health check failed", "error", err)
		writeError(response, http.StatusServiceUnavailable, "unavailable", "service is unavailable")
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleError(response http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, ErrInvalidArgument):
		writeError(response, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, registry.ErrNotFound):
		writeError(response, http.StatusNotFound, "not_found", "service not found")
	default:
		a.logger.Error("control API request failed", "method", request.Method, "path", request.URL.Path, "error", err)
		writeError(response, http.StatusInternalServerError, "internal", "internal server error")
	}
}

func decodeRequestJSON(response http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(response, request.Body, maxServiceSpecBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

func writeError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
