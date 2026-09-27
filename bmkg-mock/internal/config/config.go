package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port      string
	APIKey    string
	DelayMs   int
}

func Load() Config {
	return Config{
		Port:    getEnv("BMKG_PORT", "8081"),
		APIKey:  getEnv("BMKG_API_KEY", "changeme-bmkg-key"),
		DelayMs: getEnvInt("BMKG_DELAY_MS", 100),
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
