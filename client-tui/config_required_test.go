package main

import (
	"strings"
	"testing"
)

func TestConfigurationRequiresExplicitValues(t *testing.T) {
	values := map[string]string{
		"BNPB_API_URL":       "http://client-api:8080",
		"MEDIA_CLIENT_ID":    "media-demo",
		"FIELD_CLIENT_ID":    "field-demo",
		"INTERNAL_CLIENT_ID": "internal-demo",
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
		"BNPB_API_URL": "invalid",
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
