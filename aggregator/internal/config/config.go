package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port string

	BMKGBaseURL  string
	BMKGAPIKey   string
	PVMBGBaseURL string
	PVMBGToken   string

	PollInterval time.Duration

	DatabaseURL string

	NATSURL     string
	NATSStream  string
	NATSSubject string
}

func Load() Config {
	return Config{
		Port: getEnv("AGGREGATOR_PORT", "8080"),

		BMKGBaseURL:  getEnv("BMKG_BASE_URL", "http://localhost:8081"),
		BMKGAPIKey:   getEnv("BMKG_API_KEY", "changeme-bmkg-key"),
		PVMBGBaseURL: getEnv("PVMBG_BASE_URL", "http://localhost:8082"),
		PVMBGToken:   getEnv("PVMBG_TOKEN", "changeme-pvmbg-token"),

		PollInterval: time.Duration(getEnvInt("POLL_INTERVAL", 3)) * time.Second,

		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/hazards?sslmode=disable"),

		NATSURL:     getEnv("NATS_URL", "nats://localhost:4222"),
		NATSStream:  getEnv("NATS_STREAM", "HAZARDS"),
		NATSSubject: getEnv("NATS_SUBJECT", "hazard.events"),
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
