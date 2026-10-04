package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	Port             string
	Token            string
	AdminToken       string
	MinDelayMs       int
	MaxDelayMs       int
	ReportIntervalMs int
	RegistryURL      string
}

func Load() (Config, error) {
	token := os.Getenv("PVMBG_TOKEN")
	adminToken := os.Getenv("PVMBG_ADMIN_TOKEN")
	if token == "" || adminToken == "" || token == adminToken {
		return Config{}, errors.New("PVMBG_TOKEN and PVMBG_ADMIN_TOKEN must be distinct and nonempty")
	}
	// The contract requires at least one new report every 10 seconds.
	interval := getEnvInt("PVMBG_REPORT_INTERVAL_MS", 10000)
	if interval < 1 || interval > 10000 {
		return Config{}, errors.New("PVMBG_REPORT_INTERVAL_MS must be between 1 and 10000")
	}
	return Config{
		Port:             getEnv("PVMBG_PORT", "8082"),
		Token:            token,
		AdminToken:       adminToken,
		MinDelayMs:       getEnvInt("PVMBG_MIN_DELAY_MS", 500),
		MaxDelayMs:       getEnvInt("PVMBG_MAX_DELAY_MS", 3000),
		ReportIntervalMs: interval,
		RegistryURL:      getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8080/apis/ccompat/v7"),
	}, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
