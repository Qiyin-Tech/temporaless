package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"temporaless/internal/backend"

	"go.temporal.io/sdk/client"
	"gopkg.in/yaml.v3"
)

type config struct {
	Server   serverConfig   `yaml:"server"`
	Registry registryConfig `yaml:"registry"`
	Temporal temporalConfig `yaml:"temporal"`
	Worker   workerConfig   `yaml:"worker"`
	HTTP     httpConfig     `yaml:"http"`
}

type serverConfig struct {
	Listen            string        `yaml:"listen"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
}

type registryConfig struct {
	Path string `yaml:"path"`
}

type temporalConfig struct {
	Address        string        `yaml:"address"`
	Namespace      string        `yaml:"namespace"`
	StartupTimeout time.Duration `yaml:"startup_timeout"`
}

type workerConfig struct {
	SwitchTimeout                     time.Duration `yaml:"switch_timeout"`
	StopTimeout                       time.Duration `yaml:"stop_timeout"`
	MaxConcurrentActivitiesPerService int           `yaml:"max_concurrent_activities_per_service"`
}

type httpConfig struct {
	MaxBackendResponseBytes   int64 `yaml:"max_backend_response_bytes"`
	MaxIdleConnectionsPerHost int   `yaml:"max_idle_connections_per_host"`
}

func defaultConfig() config {
	return config{
		Server: serverConfig{
			Listen:            "127.0.0.1:8080",
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			IdleTimeout:       time.Minute,
			ShutdownTimeout:   10 * time.Second,
		},
		Registry: registryConfig{Path: "temporaless.db"},
		Temporal: temporalConfig{
			Address:        client.DefaultHostPort,
			Namespace:      "default",
			StartupTimeout: 2 * time.Minute,
		},
		Worker: workerConfig{
			SwitchTimeout:                     30 * time.Second,
			StopTimeout:                       30 * time.Second,
			MaxConcurrentActivitiesPerService: 32,
		},
		HTTP: httpConfig{
			MaxBackendResponseBytes:   backend.DefaultMaxResponseBodyBytes,
			MaxIdleConnectionsPerHost: 32,
		},
	}
}

func loadConfig(path string) (config, error) {
	result := defaultConfig()
	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			return config{}, fmt.Errorf("open config: %w", err)
		}
		defer file.Close()
		decoder := yaml.NewDecoder(file)
		decoder.KnownFields(true)
		if err := decoder.Decode(&result); err != nil {
			return config{}, fmt.Errorf("decode config: %w", err)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			if err == nil {
				err = errors.New("multiple YAML documents")
			}
			return config{}, fmt.Errorf("decode config: %w", err)
		}
	}
	if err := result.validate(); err != nil {
		return config{}, err
	}
	return result, nil
}

func (c config) validate() error {
	for name, value := range map[string]string{
		"server.listen":      c.Server.Listen,
		"registry.path":      c.Registry.Path,
		"temporal.address":   c.Temporal.Address,
		"temporal.namespace": c.Temporal.Namespace,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not be empty", name)
		}
	}
	for name, value := range map[string]time.Duration{
		"server.read_header_timeout": c.Server.ReadHeaderTimeout,
		"server.read_timeout":        c.Server.ReadTimeout,
		"server.idle_timeout":        c.Server.IdleTimeout,
		"server.shutdown_timeout":    c.Server.ShutdownTimeout,
		"temporal.startup_timeout":   c.Temporal.StartupTimeout,
		"worker.switch_timeout":      c.Worker.SwitchTimeout,
		"worker.stop_timeout":        c.Worker.StopTimeout,
	} {
		if value <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.Worker.MaxConcurrentActivitiesPerService <= 0 {
		return errors.New("worker.max_concurrent_activities_per_service must be positive")
	}
	if c.HTTP.MaxBackendResponseBytes <= 0 {
		return errors.New("http.max_backend_response_bytes must be positive")
	}
	if c.HTTP.MaxIdleConnectionsPerHost <= 0 {
		return errors.New("http.max_idle_connections_per_host must be positive")
	}
	return nil
}
