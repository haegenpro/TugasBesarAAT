package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	Port            string
	APIKey          string
	DelayMs         int
	EventIntervalMs int
	RegistryURL     string
}

func Load() (Config, error) {
	key := os.Getenv("BMKG_API_KEY")
	if key == "" {
		return Config{}, errors.New("BMKG_API_KEY is required")
	}
	// The contract requires at least one new event every 10 seconds.
	interval := getEnvInt("BMKG_EVENT_INTERVAL_MS", 10000)
	if interval < 1 || interval > 10000 {
		return Config{}, errors.New("BMKG_EVENT_INTERVAL_MS must be between 1 and 10000")
	}
	return Config{
		Port:            getEnv("BMKG_PORT", "8081"),
		APIKey:          key,
		DelayMs:         getEnvInt("BMKG_DELAY_MS", 100),
		EventIntervalMs: interval,
		RegistryURL:     getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8080/apis/ccompat/v7"),
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
