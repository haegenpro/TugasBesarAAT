package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port         string
	Token        string
	AdminToken   string
	MinDelayMs   int
	MaxDelayMs   int
}

func Load() Config {
	return Config{
		Port:       getEnv("PVMBG_PORT", "8082"),
		Token:      getEnv("PVMBG_TOKEN", "changeme-pvmbg-token"),
		AdminToken: getEnv("PVMBG_ADMIN_TOKEN", "changeme-pvmbg-admin-token"),
		MinDelayMs: getEnvInt("PVMBG_MIN_DELAY_MS", 500),
		MaxDelayMs: getEnvInt("PVMBG_MAX_DELAY_MS", 3000),
	}
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
