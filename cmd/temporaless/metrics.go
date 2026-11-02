package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
)

type metricsEndpoint struct {
	temporalHandler client.MetricsHandler
	httpHandler     http.Handler
	provider        *sdkmetric.MeterProvider
}

func newMetricsEndpoint(logger *slog.Logger) (*metricsEndpoint, error) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	exporter, err := otelprometheus.New(otelprometheus.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("create Prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		// Temporal records Go SDK histograms in seconds; OTel's unit-agnostic defaults start at 5.
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram, Unit: "s"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
			}},
		)),
	)
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	return &metricsEndpoint{
		temporalHandler: temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{
			Meter:                provider.Meter("temporal-sdk-go"),
			UseMonotonicCounters: true,
			OnError: func(err error) {
				logger.Error("Temporal metric error", "error", err)
			},
		}),
		httpHandler: mux,
		provider:    provider,
	}, nil
}

func (m *metricsEndpoint) shutdown() {
	if err := m.provider.Shutdown(context.Background()); err != nil {
		slog.Error("metrics provider shutdown failed", "error", err)
	}
}
