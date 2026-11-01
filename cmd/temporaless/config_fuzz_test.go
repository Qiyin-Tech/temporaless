package main

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzLoadConfigRejectsInvalidOrNormalizesValidYAML(f *testing.F) {
	for _, seed := range []string{
		"",
		"server: {}\n",
		"server:\n  unknown: true\n",
		"server: {}\n---\nworker: {}\n",
		"worker:\n  max_concurrent_activities_per_service: 9223372036854775807\n",
		"server:\n  read_timeout: -9223372036854775808ns\n",
		"http:\n  max_backend_response_bytes: 0\n",
		"temporal:\n  startup_timeout: 1h30m\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			t.Skip()
		}
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
			t.Fatal(err)
		}
		configuration, err := loadConfig(path)
		if err != nil {
			return
		}
		if err := configuration.validate(); err != nil {
			t.Fatalf("loadConfig accepted config that fails validate: %v", err)
		}
		if configuration.Worker.MaxConcurrentActivitiesPerService <= 0 || configuration.HTTP.MaxBackendResponseBytes <= 0 {
			t.Fatalf("accepted invalid limits: worker=%d response=%d", configuration.Worker.MaxConcurrentActivitiesPerService, configuration.HTTP.MaxBackendResponseBytes)
		}
	})
}
