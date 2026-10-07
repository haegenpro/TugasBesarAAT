package config

import (
	"errors"
)

type Config struct {
	Port             string
	Token            string
	AdminToken       string
	MinDelayMs       int
	MaxDelayMs       int
	ReportIntervalMs int
	RegistryURL      string
	ArchivePath      string
}

func Load() (Config, error) {
	v, err := requiredEnv("PVMBG_PORT", "PVMBG_TOKEN", "PVMBG_ADMIN_TOKEN", "PVMBG_MIN_DELAY_MS", "PVMBG_MAX_DELAY_MS", "PVMBG_REPORT_INTERVAL_MS", "SCHEMA_REGISTRY_URL", "SOURCE_ARCHIVE_PATH")
	if err != nil {
		return Config{}, err
	}
	if v["PVMBG_TOKEN"] == v["PVMBG_ADMIN_TOKEN"] {
		return Config{}, errors.New("PVMBG_TOKEN and PVMBG_ADMIN_TOKEN must be distinct")
	}
	if _, err = envInt(v, "PVMBG_PORT", 1, 65535); err != nil {
		return Config{}, err
	}
	minDelay, err := envInt(v, "PVMBG_MIN_DELAY_MS", 0, 0)
	if err != nil {
		return Config{}, err
	}
	maxDelay, err := envInt(v, "PVMBG_MAX_DELAY_MS", minDelay, 0)
	if err != nil {
		return Config{}, err
	}
	interval, err := envInt(v, "PVMBG_REPORT_INTERVAL_MS", 1, 10000)
	if err != nil {
		return Config{}, err
	}
	if err = envURL(v, "SCHEMA_REGISTRY_URL", "http", "https"); err != nil {
		return Config{}, err
	}
	return Config{Port: v["PVMBG_PORT"], Token: v["PVMBG_TOKEN"], AdminToken: v["PVMBG_ADMIN_TOKEN"], MinDelayMs: minDelay, MaxDelayMs: maxDelay, ReportIntervalMs: interval, RegistryURL: v["SCHEMA_REGISTRY_URL"], ArchivePath: v["SOURCE_ARCHIVE_PATH"]}, nil
}
