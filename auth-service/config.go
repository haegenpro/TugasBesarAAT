package main

import (
	"errors"
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
	Port        string
	DBPath      string
	InternalKey string
	AccessTTL   time.Duration
	RefreshTTL  time.Duration
	Identities  map[string]identity
}

func loadConfig() (config, error) {
	accessTTL, err := durationSeconds("ACCESS_TOKEN_TTL_SEC", 60)
	if err != nil {
		return config{}, err
	}
	refreshTTL, err := durationSeconds("REFRESH_TOKEN_TTL_SEC", 86400)
	if err != nil {
		return config{}, err
	}
	if refreshTTL <= accessTTL {
		return config{}, errors.New("refresh token TTL must exceed access token TTL")
	}
	internalKey := os.Getenv("AUTH_INTERNAL_KEY")
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
		id := os.Getenv(item.prefix + "_CLIENT_ID")
		secret := os.Getenv(item.prefix + "_CLIENT_SECRET")
		if id == "" || secret == "" {
			return config{}, errors.New(item.prefix + " credentials are required")
		}
		if _, exists := identities[id]; exists || secrets[secret] {
			return config{}, errors.New("client IDs and secrets must be distinct")
		}
		identities[id] = identity{ID: id, Secret: secret, Role: item.role}
		secrets[secret] = true
	}
	port := os.Getenv("AUTH_PORT")
	if port == "" {
		port = "8083"
	}
	path := os.Getenv("AUTH_DB_PATH")
	if path == "" {
		path = "/data/auth.db"
	}
	return config{Port: port, DBPath: path, InternalKey: internalKey, AccessTTL: accessTTL, RefreshTTL: refreshTTL, Identities: identities}, nil
}

func durationSeconds(name string, fallback int) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return time.Duration(fallback) * time.Second, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 1 {
		return 0, errors.New(name + " must be a positive integer")
	}
	return time.Duration(seconds) * time.Second, nil
}
