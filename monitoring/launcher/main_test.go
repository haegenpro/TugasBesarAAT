package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func configured(service string) map[string]string {
	values := map[string]string{}
	for _, key := range required[service] {
		switch {
		case strings.HasSuffix(key, "_URL"):
			values[key] = "http://example:9090/metrics"
		case strings.HasSuffix(key, "RETENTION_DAYS"):
			values[key] = "30"
		case key == "GRAFANA_BIND_ADDRESS":
			values[key] = "0.0.0.0"
		case key == "LOKI_GRPC_PORT":
			values[key] = "19095"
		case strings.HasSuffix(key, "_PORT"):
			values[key] = "19080"
		default:
			values[key] = "configured-secret"
		}
	}
	return values
}

func TestRequiredConfiguration(t *testing.T) {
	for service, keys := range required {
		t.Run(service, func(t *testing.T) {
			for _, key := range keys {
				for _, blank := range []string{"", " \t\n"} {
					v := configured(service)
					v[key] = blank
					if _, err := validate(service, func(k string) string { return v[k] }); err == nil || !strings.Contains(err.Error(), key) {
						t.Fatalf("%s must be reported: %v", key, err)
					}
				}
			}
			_, err := validate(service, func(string) string { return "" })
			if err == nil {
				t.Fatal("missing config accepted")
			}
			for _, key := range keys {
				if !strings.Contains(err.Error(), key) {
					t.Errorf("missing name %s", key)
				}
			}
			if _, err := validate(service, func(k string) string { return configured(service)[k] }); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInvalidConfigurationDoesNotExposeValues(t *testing.T) {
	cases := []struct{ service, key, value string }{
		{"grafana", "GRAFANA_BIND_ADDRESS", "not-an-address"},
		{"grafana", "DLQ_STREAM_NAME", "HAZARDS\"} or vector(0)"},
		{"nats-exporter", "NATS_MONITOR_URL", "nats://broker:4222"},
		{"prometheus", "NATS_EXPORTER_METRICS_URL", "http://exporter:99999/metrics"},
		{"grafana", "GRAFANA_HOST_PORT", "65536"},
		{"grafana", "GRAFANA_HOST_PORT", "-1"},
		{"alloy", "ALLOY_PORT", "0"},
		{"nats-exporter", "NATS_EXPORTER_PORT", "invalid"},
		{"loki", "LOKI_GRPC_PORT", "19080"},
		{"grafana", "PROMETHEUS_URL", "http://secret-user:secret-pass@example"},
		{"grafana", "LOKI_URL", "ftp://example"},
		{"grafana", "LOKI_URL", "http://example:99999"},
		{"loki", "LOG_RETENTION_DAYS", "0"},
		{"loki", "LOG_RETENTION_DAYS", "-3"},
		{"prometheus", "METRICS_RETENTION_DAYS", "bad-retention-secret"},
	}
	for _, c := range cases {
		t.Run(c.key+"/"+c.value, func(t *testing.T) {
			v := configured(c.service)
			v[c.key] = c.value
			_, err := validate(c.service, func(k string) string { return v[k] })
			if err == nil || !strings.Contains(err.Error(), c.key) {
				t.Fatalf("invalid config accepted: %v", err)
			}
			if len(c.value) > 8 && strings.Contains(err.Error(), c.value) {
				t.Fatal("value exposed")
			}
		})
	}
}

func TestRenderConfigurations(t *testing.T) {
	source := "../templates"
	for service := range required {
		t.Run(service, func(t *testing.T) {
			dest := t.TempDir()
			if err := prepare(service, configured(service), source, dest); err != nil {
				t.Fatal(err)
			}
			if service == "grafana" {
				b, err := os.ReadFile(filepath.Join(dest, "dashboards", "monitoring.json"))
				if err != nil {
					t.Fatal(err)
				}
				if !json.Valid(b) {
					t.Fatal("invalid dashboard JSON")
				}
				for _, kind := range []string{"datasources", "dashboards", "alerting"} {
					if _, err := os.Stat(filepath.Join(dest, "provisioning", kind, "monitoring.yml")); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestTemplateEscaping(t *testing.T) {
	v := configured("alloy")
	v["COMPOSE_PROJECT_NAME"] = "project\"\nwith-newline"
	dest := t.TempDir()
	if err := render("alloy", v, "../templates", dest); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dest, "config.alloy"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "project\"\nwith-newline") {
		t.Fatal("configuration injection")
	}
}

func TestNonDefaultPortsReachGeneratedConfiguration(t *testing.T) {
	for _, c := range []struct {
		service, path string
		expected      []string
	}{
		{"loki", "loki.yml", []string{"http_listen_port: 19080", "grpc_listen_port: 19095"}},
		{"prometheus", "prometheus.yml", []string{"localhost:19080"}},
		{"grafana", "provisioning/alerting/monitoring.yml", []string{"http://127.0.0.1:19080/api/health"}},
	} {
		t.Run(c.service, func(t *testing.T) {
			dest := t.TempDir()
			if err := prepare(c.service, configured(c.service), "../templates", dest); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(dest, c.path))
			if err != nil {
				t.Fatal(err)
			}
			for _, expected := range c.expected {
				if !strings.Contains(string(data), expected) {
					t.Fatalf("configured port missing from %s", c.path)
				}
			}
		})
	}
}
