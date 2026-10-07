package main

import (
	"strings"
	"testing"
)

func TestConfigurationRequiresExplicitValues(t *testing.T) {
	values := map[string]string{
		"AUTH_PORT":              "8083",
		"AUTH_DB_PATH":           "/data/auth.db",
		"AUTH_INTERNAL_KEY":      "replace-with-random-internal-key",
		"ACCESS_TOKEN_TTL_SEC":   "60",
		"REFRESH_TOKEN_TTL_SEC":  "86400",
		"MEDIA_CLIENT_ID":        "media-demo",
		"MEDIA_CLIENT_SECRET":    "media-secret",
		"FIELD_CLIENT_ID":        "field-demo",
		"FIELD_CLIENT_SECRET":    "field-secret",
		"INTERNAL_CLIENT_ID":     "internal-demo",
		"INTERNAL_CLIENT_SECRET": "internal-secret",
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
		"AUTH_PORT":             "invalid",
		"ACCESS_TOKEN_TTL_SEC":  "invalid",
		"REFRESH_TOKEN_TTL_SEC": "invalid",
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
