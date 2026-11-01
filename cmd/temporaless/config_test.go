package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigMergesYAMLWithDefaults(t *testing.T) {
	t.Parallel()

	path := writeConfig(t, `
server:
  listen: 0.0.0.0:9090
registry:
  path: /tmp/test.db
worker:
  switch_timeout: 45s
http:
  max_backend_response_bytes: 2097152
`)
	configuration, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Server.Listen != "0.0.0.0:9090" || configuration.Server.ReadTimeout != 15*time.Second {
		t.Fatalf("server config = %#v", configuration.Server)
	}
	if configuration.Worker.SwitchTimeout != 45*time.Second || configuration.Worker.StopTimeout != 30*time.Second {
		t.Fatalf("worker config = %#v", configuration.Worker)
	}
	if configuration.HTTP.MaxBackendResponseBytes != 2<<20 {
		t.Fatalf("HTTP config = %#v", configuration.HTTP)
	}
}

func TestLoadConfigRejectsInvalidYAML(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, yaml, want string
	}{
		{name: "unknown field", yaml: "server:\n  unknown: true\n", want: "field unknown not found"},
		{name: "non-positive duration", yaml: "server:\n  read_timeout: 0s\n", want: "server.read_timeout must be positive"},
		{name: "multiple documents", yaml: "server: {}\n---\nserver: {}\n", want: "multiple YAML documents"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadConfig(writeConfig(t, test.yaml))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "temporaless.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
