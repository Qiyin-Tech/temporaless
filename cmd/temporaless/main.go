package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"temporaless/internal/control"
	"temporaless/internal/registry"

	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
)

func main() {
	slog.SetDefault(newLogger(os.Stdout))
	if err := run(); err != nil {
		slog.Error("temporaless stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "YAML config file; defaults are used when omitted")
	flag.Parse()
	configuration, err := loadConfig(*configPath)
	if err != nil {
		return err
	}

	store, err := registry.Open(configuration.Registry.Path)
	if err != nil {
		return err
	}
	defer store.Close()

	var metrics *metricsEndpoint
	if configuration.Observability.Metrics.Listen != "" {
		metrics, err = newMetricsEndpoint(slog.Default())
		if err != nil {
			return err
		}
		defer metrics.shutdown()
	}

	temporalOptions := client.Options{
		HostPort:  configuration.Temporal.Address,
		Namespace: configuration.Temporal.Namespace,
		Logger:    temporallog.NewStructuredLogger(slog.Default().With("component", "temporal_sdk")),
	}
	if metrics != nil {
		temporalOptions.MetricsHandler = metrics.temporalHandler
	}
	temporalClient, err := client.Dial(temporalOptions)
	if err != nil {
		return err
	}
	defer temporalClient.Close()

	runtime := control.NewTemporalRuntime(
		temporalClient,
		configuration.Worker.StopTimeout,
		configuration.Worker.MaxConcurrentActivitiesPerService,
		configuration.Worker.MaxConcurrentActivityTaskPollers,
	)
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.MaxIdleConns = max(httpTransport.MaxIdleConns, configuration.HTTP.MaxIdleConnectionsPerHost)
	httpTransport.MaxIdleConnsPerHost = configuration.HTTP.MaxIdleConnectionsPerHost
	backendClient := &http.Client{Transport: httpTransport}
	manager := control.NewManager(store, runtime, backendClient, control.ManagerOptions{
		SwitchTimeout:               configuration.Worker.SwitchTimeout,
		MaxBackendResponseBodyBytes: configuration.HTTP.MaxBackendResponseBytes,
	})
	defer manager.Close()
	startupCtx, startupCancel := context.WithTimeout(context.Background(), configuration.Temporal.StartupTimeout)
	err = manager.Reconcile(startupCtx)
	startupCancel()
	if err != nil {
		return err
	}

	servers := []*http.Server{{
		Addr:              configuration.Server.Listen,
		Handler:           control.NewAPI(manager, slog.Default()),
		ReadHeaderTimeout: configuration.Server.ReadHeaderTimeout,
		ReadTimeout:       configuration.Server.ReadTimeout,
		IdleTimeout:       configuration.Server.IdleTimeout,
	}}
	if metrics != nil {
		servers = append(servers, &http.Server{
			Addr:              configuration.Observability.Metrics.Listen,
			Handler:           metrics.httpHandler,
			ReadHeaderTimeout: configuration.Server.ReadHeaderTimeout,
			ReadTimeout:       configuration.Server.ReadTimeout,
			IdleTimeout:       configuration.Server.IdleTimeout,
		})
	}
	serverErrors := make(chan error, len(servers))
	for index, server := range servers {
		server := server
		name := "control API"
		if index == 1 {
			name = "metrics"
		}
		go func() {
			slog.Info(name+" listening", "address", server.Addr)
			serverErrors <- server.ListenAndServe()
		}()
	}

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	var runErr error
	select {
	case <-signalCtx.Done():
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = err
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), configuration.Server.ShutdownTimeout)
	defer shutdownCancel()
	for _, server := range servers {
		runErr = errors.Join(runErr, server.Shutdown(shutdownCtx))
	}
	return runErr
}

func newLogger(output io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(output, nil)).With("service", "temporaless")
}
