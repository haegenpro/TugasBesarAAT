package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	Port    string
	APIKey  string
	DelayMs int
}

func Load() (Config, error) {
	key := os.Getenv("BMKG_API_KEY")
	if key == "" {
		return Config{}, errors.New("BMKG_API_KEY is required")
	}
	return Config{
		Port:    getEnv("BMKG_PORT", "8081"),
		APIKey:  key,
		DelayMs: getEnvInt("BMKG_DELAY_MS", 100),
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
