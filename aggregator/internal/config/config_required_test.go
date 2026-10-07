package config

import (
	"strings"
	"testing"
)

func TestConfigurationRequiresExplicitValues(t *testing.T) {
	values := map[string]string{
		"AGGREGATOR_PORT":        "8080",
		"BMKG_BASE_URL":          "http://bmkg-mock:8081",
		"BMKG_API_KEY":           "changeme-bmkg-key",
		"PVMBG_BASE_URL":         "http://pvmbg-mock:8082",
		"PVMBG_TOKEN":            "changeme-pvmbg-token",
		"POLL_INTERVAL":          "3",
		"SOURCE_RETRY_MAX_SEC":   "60",
		"BMKG_FRESHNESS_SEC":     "60",
		"PVMBG_FRESHNESS_SEC":    "1800",
		"DATABASE_URL":           "postgres://postgres:changeme-postgres-password@postgres:5432/hazards?sslmode=disable",
		"NATS_URL":               "nats://broker:4222",
		"NATS_STREAM":            "HAZARDS",
		"NATS_SUBJECT":           "hazard.events",
		"SCHEMA_REGISTRY_URL":    "http://schema-registry:8080/apis/ccompat/v7",
		"NATS_LIFECYCLE_STREAM":  "REPORT_LIFECYCLE",
		"NATS_LIFECYCLE_SUBJECT": "report.lifecycle",
	}
	for key, value := range values {
		t.Setenv(key, value)
	}
	if _, err := Load(); err != nil {
		t.Fatalf("explicit configuration rejected: %v", err)
	}
	for key := range values {
		for _, missing := range []string{"", "   "} {
			t.Run(key+"/"+missing, func(t *testing.T) {
				t.Setenv(key, missing)
				if _, err := Load(); err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("missing %s must fail and name that setting: %v", key, err)
				}
			})
		}
	}
	for key, invalid := range map[string]string{
		"AGGREGATOR_PORT":      "invalid",
		"POLL_INTERVAL":        "invalid",
		"SOURCE_RETRY_MAX_SEC": "invalid",
		"BMKG_FRESHNESS_SEC":   "invalid",
		"PVMBG_FRESHNESS_SEC":  "invalid",
		"BMKG_BASE_URL":        "invalid",
		"PVMBG_BASE_URL":       "invalid",
		"DATABASE_URL":         "invalid",
		"NATS_URL":             "invalid",
		"SCHEMA_REGISTRY_URL":  "invalid",
	} {
		t.Run(key+"/invalid", func(t *testing.T) {
			t.Setenv(key, invalid)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("invalid %s must fail and name that setting: %v", key, err)
			}
		})
	}
	t.Run("all missing", func(t *testing.T) {
		for key := range values {
			t.Setenv(key, "")
		}
		_, err := Load()
		if err == nil {
			t.Fatal("missing configuration accepted")
		}
		for key := range values {
			if !strings.Contains(err.Error(), key) {
				t.Fatalf("missing %s not reported: %v", key, err)
			}
		}
	})
}
