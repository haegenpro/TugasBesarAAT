package config

import (
	"strings"
	"testing"
)

func TestConfigurationRequiresExplicitValues(t *testing.T) {
	values := map[string]string{
		"BMKG_PORT":              "8081",
		"BMKG_API_KEY":           "changeme-bmkg-key",
		"BMKG_DELAY_MS":          "100",
		"BMKG_EVENT_INTERVAL_MS": "10000",
		"SCHEMA_REGISTRY_URL":    "http://schema-registry:8080/apis/ccompat/v7",
		"SOURCE_ARCHIVE_PATH":    "/data/history.db",
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
		"BMKG_PORT":              "invalid",
		"BMKG_DELAY_MS":          "invalid",
		"BMKG_EVENT_INTERVAL_MS": "invalid",
		"SCHEMA_REGISTRY_URL":    "invalid",
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
