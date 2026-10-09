package main

import (
	"strconv"
	"strings"
	"testing"
)

func TestConfigurationRequiresExplicitValues(t *testing.T) {
	values := map[string]string{
		"CLIENT_API_PORT":                    "8080",
		"AUTH_BASE_URL":                      "http://auth-service:8083",
		"AGGREGATOR_BASE_URL":                "http://aggregator:8080",
		"AUTH_INTERNAL_KEY":                  "replace-with-random-internal-key",
		"CLIENT_API_MAX_INFLIGHT":            "128",
		"CLIENT_API_MAX_INFLIGHT_PER_CLIENT": "32",
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
		"CLIENT_API_PORT":                    "invalid",
		"CLIENT_API_MAX_INFLIGHT":            "invalid",
		"CLIENT_API_MAX_INFLIGHT_PER_CLIENT": "invalid",
		"AUTH_BASE_URL":                      "invalid",
		"AGGREGATOR_BASE_URL":                "invalid",
	} {
		t.Run(key+"/invalid", func(t *testing.T) {
			t.Setenv(key, invalid)
			if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("invalid %s must fail and name that setting: %v", key, err)
			}
		})
	}
	for _, value := range []string{"0", "1", "32", "256", "-1", "1.5"} {
		t.Run("per-client/"+value, func(t *testing.T) {
			t.Setenv("CLIENT_API_MAX_INFLIGHT_PER_CLIENT", value)
			cfg, err := loadConfig()
			if value == "-1" || value == "1.5" {
				if err == nil || !strings.Contains(err.Error(), "CLIENT_API_MAX_INFLIGHT_PER_CLIENT") {
					t.Fatalf("invalid per-client limit accepted: %q, %v", value, err)
				}
				return
			}
			want, _ := strconv.Atoi(value)
			if err != nil || cfg.MaxInFlightPerClient != want {
				t.Fatalf("valid per-client limit rejected: %q, %v", value, err)
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
