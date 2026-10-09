package main

import (
	"strings"
	"testing"
)

func TestConfigurationRequiresExplicitValues(t *testing.T) {
	values := map[string]string{
		"NATS_PORT":               "4222",
		"NATS_MONITOR_PORT":       "8222",
		"NATS_URL":                "nats://broker:4222",
		"NATS_STREAM":             "HAZARDS",
		"NATS_SUBJECT":            "hazard.events",
		"NATS_STREAM_MAX_AGE":     "24h",
		"NATS_DLQ_STREAM":         "HAZARDS_DLQ",
		"NATS_DLQ_SUBJECT_PREFIX": "hazard.dlq",
		"NATS_DLQ_MAX_AGE":        "168h",
		"NATS_LIFECYCLE_STREAM":   "REPORT_LIFECYCLE",
		"NATS_LIFECYCLE_SUBJECT":  "report.lifecycle",
		"NATS_LIFECYCLE_MAX_AGE":  "24h",
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
		"NATS_URL":          "invalid",
		"NATS_PORT":         "0",
		"NATS_MONITOR_PORT": "65536",
	} {
		t.Run(key+"/invalid", func(t *testing.T) {
			t.Setenv(key, invalid)
			if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("invalid %s must fail and name that setting: %v", key, err)
			}
		})
	}
	t.Run("listener collision", func(t *testing.T) {
		t.Setenv("NATS_MONITOR_PORT", values["NATS_PORT"])
		if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "must be different") {
			t.Fatalf("overlapping listeners accepted: %v", err)
		}
	})
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
