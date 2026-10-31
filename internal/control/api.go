package control

import (
	"context"
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

type API struct {
	manager *Manager
	logger  *slog.Logger
	mux     *http.ServeMux
}

func NewAPI(manager *Manager, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	api := &API{manager: manager, logger: logger, mux: http.NewServeMux()}
	api.mux.HandleFunc("PUT /services/{id}", api.putService)
	api.mux.HandleFunc("GET /services", api.listServices)
	api.mux.HandleFunc("GET /services/{id}", api.getService)
	api.mux.HandleFunc("DELETE /services/{id}", api.deleteService)
	api.mux.HandleFunc("GET /healthz", api.health)
	return api
}

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
