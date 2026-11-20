package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type identity struct {
	ID     string
	Secret string
	Role   string
}

type config struct {
	Port            string
	DatabaseURL     string
	InternalKey     string
	AccessTTL       time.Duration
	RefreshTTL      time.Duration
	Identities      map[string]identity
	PriorityEnabled bool
	PriorityKey     string
	MaxInFlight     int
	FieldReserved   int
}

func loadConfig() (config, error) {
	v, err := requiredEnv("AUTH_PORT", "DATABASE_URL", "AUTH_INTERNAL_KEY", "ACCESS_TOKEN_TTL_SEC", "REFRESH_TOKEN_TTL_SEC", "MEDIA_CLIENT_ID", "MEDIA_CLIENT_SECRET", "FIELD_CLIENT_ID", "FIELD_CLIENT_SECRET", "INTERNAL_CLIENT_ID", "INTERNAL_CLIENT_SECRET")
	if err != nil {
		return config{}, err
	}
	if _, err = envInt(v, "AUTH_PORT", 1, 65535); err != nil {
		return config{}, err
	}
	accessTTL, err := durationSeconds(v, "ACCESS_TOKEN_TTL_SEC")
	if err != nil {
		return config{}, err
	}
	refreshTTL, err := durationSeconds(v, "REFRESH_TOKEN_TTL_SEC")
	if err != nil {
		return config{}, err
	}
	if refreshTTL <= accessTTL {
		return config{}, errors.New("refresh token TTL must exceed access token TTL")
	}
	internalKey := v["AUTH_INTERNAL_KEY"]
	if internalKey == "" {
		return config{}, errors.New("AUTH_INTERNAL_KEY is required")
	}
	identities := make(map[string]identity, 3)
	secrets := make(map[string]bool, 3)
	for _, item := range []struct{ prefix, role string }{
		{"MEDIA", "media"},
		{"FIELD", "field"},
		{"INTERNAL", "internal"},
	} {
		id := v[item.prefix+"_CLIENT_ID"]
		secret := v[item.prefix+"_CLIENT_SECRET"]
		if id == "" || secret == "" {
			return config{}, errors.New(item.prefix + " credentials are required")
		}
		if _, exists := identities[id]; exists || secrets[secret] {
			return config{}, errors.New("client IDs and secrets must be distinct")
		}
		identities[id] = identity{ID: id, Secret: secret, Role: item.role}
		secrets[secret] = true
	}
	if err = envURL(v, "DATABASE_URL", "postgres", "postgresql"); err != nil {
		return config{}, err
	}
	port, path := v["AUTH_PORT"], v["DATABASE_URL"]
	cfg := config{Port: port, DatabaseURL: path, InternalKey: internalKey, AccessTTL: accessTTL, RefreshTTL: refreshTTL, Identities: identities}
	raw := os.Getenv("AUTH_PRIORITY_ENABLED")
	if raw != "" && raw != "true" && raw != "false" {
		return config{}, errors.New("invalid AUTH_PRIORITY_ENABLED")
	}
	cfg.PriorityEnabled = raw == "true"
	if cfg.PriorityEnabled {
		cfg.PriorityKey = os.Getenv("AUTH_PRIORITY_KEY")
		if len(cfg.PriorityKey) < 32 || cfg.PriorityKey == internalKey {
			return config{}, errors.New("distinct AUTH_PRIORITY_KEY of at least 32 bytes required")
		}
		settings, e := requiredEnv("AUTH_MAX_INFLIGHT", "AUTH_FIELD_RESERVED")
		if e != nil {
			return config{}, e
		}
		cfg.MaxInFlight, e = envInt(settings, "AUTH_MAX_INFLIGHT", 2, 65536)
		if e != nil {
			return config{}, e
		}
		cfg.FieldReserved, e = envInt(settings, "AUTH_FIELD_RESERVED", 1, cfg.MaxInFlight-1)
		if e != nil {
			return config{}, e
		}
	}
	return cfg, nil
}

func durationSeconds(values map[string]string, name string) (time.Duration, error) {
	value := values[name]
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 1 || int64(seconds) > (1<<63-1)/int64(time.Second) {
		return 0, fmt.Errorf("%s must be positive integer seconds within the supported duration range", name)
	}
	return time.Duration(seconds) * time.Second, nil
}
