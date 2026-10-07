package main

import (
	"strings"
	"testing"
)

func TestConfigurationRequiresExplicitValues(t *testing.T) {
	values := map[string]string{
		"CLIENT_API_PORT":         "8080",
		"AUTH_BASE_URL":           "http://auth-service:8083",
		"AGGREGATOR_BASE_URL":     "http://aggregator:8080",
		"AUTH_INTERNAL_KEY":       "replace-with-random-internal-key",
		"CLIENT_API_MAX_INFLIGHT": "128",
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
		"CLIENT_API_PORT":         "invalid",
		"CLIENT_API_MAX_INFLIGHT": "invalid",
		"AUTH_BASE_URL":           "invalid",
		"AGGREGATOR_BASE_URL":     "invalid",
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
