package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"temporaless/internal/control"
	"temporaless/internal/registry"

	"go.temporal.io/sdk/client"
)

func main() {
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

	temporalClient, err := client.Dial(client.Options{HostPort: configuration.Temporal.Address, Namespace: configuration.Temporal.Namespace})
	if err != nil {
		return err
	}
	defer temporalClient.Close()

	runtime := control.NewTemporalRuntime(temporalClient, configuration.Worker.StopTimeout, configuration.Worker.MaxConcurrentActivitiesPerService)
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

	server := &http.Server{
		Addr:              configuration.Server.Listen,
		Handler:           control.NewAPI(manager, slog.Default()),
		ReadHeaderTimeout: configuration.Server.ReadHeaderTimeout,
		ReadTimeout:       configuration.Server.ReadTimeout,
		IdleTimeout:       configuration.Server.IdleTimeout,
	}
	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("control API listening", "address", configuration.Server.Listen)
		serverErrors <- server.ListenAndServe()
	}()

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	select {
	case <-signalCtx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), configuration.Server.ShutdownTimeout)
		defer shutdownCancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
