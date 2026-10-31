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
	"time"

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
	listenAddress := flag.String("listen", "127.0.0.1:8080", "control API listen address")
	databasePath := flag.String("db", "temporaless.db", "SQLite registry path")
	temporalAddress := flag.String("temporal-address", client.DefaultHostPort, "Temporal frontend address")
	temporalNamespace := flag.String("temporal-namespace", "default", "Temporal namespace")
	switchTimeout := flag.Duration("switch-timeout", 30*time.Second, "maximum time to make a Worker revision current")
	workerStopTimeout := flag.Duration("worker-stop-timeout", 30*time.Second, "grace period for in-flight Activities")
	flag.Parse()

	store, err := registry.Open(*databasePath)
	if err != nil {
		return err
	}
	defer store.Close()

	temporalClient, err := client.Dial(client.Options{HostPort: *temporalAddress, Namespace: *temporalNamespace})
	if err != nil {
		return err
	}
	defer temporalClient.Close()

	runtime := control.NewTemporalRuntime(temporalClient, *workerStopTimeout)
	manager := control.NewManager(store, runtime, http.DefaultClient, control.ManagerOptions{SwitchTimeout: *switchTimeout})
	defer manager.Close()
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	err = manager.Reconcile(startupCtx)
	startupCancel()
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:              *listenAddress,
		Handler:           control.NewAPI(manager, slog.Default()),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       time.Minute,
	}
	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("control API listening", "address", *listenAddress)
		serverErrors <- server.ListenAndServe()
	}()

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	select {
	case <-signalCtx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
