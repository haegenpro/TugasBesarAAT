package main

import (
	"strings"
	"testing"
)

func TestConfigurationRequiresExplicitValues(t *testing.T) {
	t.Setenv("AUTH_BASE_URL", "")
	t.Setenv("AUTH_INTERNAL_KEY", "")
	values := map[string]string{
		"CONSUMER_ROLE":          "dashboard",
		"CONSUMER_PORT":          "8091",
		"CONSUMER_DB_PATH":       "/data/dashboard.db",
		"NATS_URL":               "nats://broker:4222",
		"NATS_STREAM":            "HAZARDS",
		"NATS_SUBJECT":           "hazard.events",
		"NATS_DURABLE":           "dashboard-v1",
		"SCHEMA_REGISTRY_URL":    "http://schema-registry:8080/apis/ccompat/v7",
		"NATS_DLQ_STREAM":        "HAZARDS_DLQ",
		"NATS_DLQ_SUBJECT":       "hazard.dlq.dashboard",
		"NATS_LIFECYCLE_STREAM":  "REPORT_LIFECYCLE",
		"NATS_LIFECYCLE_SUBJECT": "report.lifecycle",
	}
	for key, value := range values {
		t.Setenv(key, value)
	}
	if _, err := loadConfig(); err != nil {
		t.Fatalf("explicit configuration rejected: %v", err)
	}
	for key := range values {
		for _, missing := range []string{"", "   "} {
			t.Run(key+"/"+missing, func(t *testing.T) {
				t.Setenv(key, missing)
				if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("missing %s must fail and name that setting: %v", key, err)
				}
			})
		}
	}
	for key, invalid := range map[string]string{
		"CONSUMER_PORT":       "invalid",
		"NATS_URL":            "invalid",
		"SCHEMA_REGISTRY_URL": "invalid",
	} {
		t.Run(key+"/invalid", func(t *testing.T) {
			t.Setenv(key, invalid)
			if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("invalid %s must fail and name that setting: %v", key, err)
			}
		})
	}
	t.Run("all missing", func(t *testing.T) {
		for key := range values {
			t.Setenv(key, "")
		}
		_, err := loadConfig()
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
