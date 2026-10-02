package config

import (
	"errors"
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

	NATSURL           string
	NATSStream        string
	NATSSubject       string
	SchemaRegistryURL string
}

func Load() (Config, error) {
	bmkgKey := os.Getenv("BMKG_API_KEY")
	pvmbgToken := os.Getenv("PVMBG_TOKEN")
	databaseURL := os.Getenv("DATABASE_URL")
	if bmkgKey == "" || pvmbgToken == "" || databaseURL == "" || os.Getenv("NATS_STREAM") == "" || os.Getenv("NATS_SUBJECT") == "" || os.Getenv("SCHEMA_REGISTRY_URL") == "" {
		return Config{}, errors.New("BMKG_API_KEY, PVMBG_TOKEN, DATABASE_URL, NATS_STREAM, NATS_SUBJECT, and SCHEMA_REGISTRY_URL are required")
	}
	if bmkgKey == pvmbgToken {
		return Config{}, errors.New("BMKG and PVMBG credentials must be distinct")
	}
	return Config{
		Port: getEnv("AGGREGATOR_PORT", "8080"),

		BMKGBaseURL:  getEnv("BMKG_BASE_URL", "http://localhost:8081"),
		BMKGAPIKey:   bmkgKey,
		PVMBGBaseURL: getEnv("PVMBG_BASE_URL", "http://localhost:8082"),
		PVMBGToken:   pvmbgToken,

		PollInterval: time.Duration(getEnvInt("POLL_INTERVAL", 3)) * time.Second,

		DatabaseURL: databaseURL,

		NATSURL:           getEnv("NATS_URL", "nats://localhost:4222"),
		NATSStream:        os.Getenv("NATS_STREAM"),
		NATSSubject:       os.Getenv("NATS_SUBJECT"),
		SchemaRegistryURL: os.Getenv("SCHEMA_REGISTRY_URL"),
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
