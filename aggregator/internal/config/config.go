package config

import (
	"errors"
	"time"
)

type Config struct {
	Port       string
	AdminToken string

	BMKGBaseURL  string
	BMKGAPIKey   string
	PVMBGBaseURL string
	PVMBGToken   string

	PollInterval     time.Duration
	MaxBackoff       time.Duration
	BMKGFreshness    time.Duration
	PVMBGFreshness   time.Duration
	LifecycleStream  string
	LifecycleSubject string

	DatabaseURL string

	NATSURL                 string
	NATSStream              string
	NATSSubject             string
	SchemaRegistryURL       string
	BMKGSeismicReaderSchema string
	BMKGTsunamiReaderSchema string
	PVMBGReaderSchema       string
}

func Load() (Config, error) {
	v, err := requiredEnv("AGGREGATOR_ADMIN_TOKEN", "AGGREGATOR_PORT", "BMKG_BASE_URL", "BMKG_API_KEY", "PVMBG_BASE_URL", "PVMBG_TOKEN", "POLL_INTERVAL", "SOURCE_RETRY_MAX_SEC", "BMKG_FRESHNESS_SEC", "PVMBG_FRESHNESS_SEC", "DATABASE_URL", "NATS_URL", "NATS_STREAM", "NATS_SUBJECT", "SCHEMA_REGISTRY_URL", "NATS_LIFECYCLE_STREAM", "NATS_LIFECYCLE_SUBJECT", "BMKG_SEISMIC_READER_SCHEMA", "BMKG_TSUNAMI_READER_SCHEMA", "PVMBG_READER_SCHEMA")
	if err != nil {
		return Config{}, err
	}
	if v["BMKG_API_KEY"] == v["PVMBG_TOKEN"] {
		return Config{}, errors.New("BMKG and PVMBG credentials must be distinct")
	}
	if v["AGGREGATOR_ADMIN_TOKEN"] == v["BMKG_API_KEY"] || v["AGGREGATOR_ADMIN_TOKEN"] == v["PVMBG_TOKEN"] {
		return Config{}, errors.New("AGGREGATOR_ADMIN_TOKEN must differ from source data credentials")
	}
	if _, err = envInt(v, "AGGREGATOR_PORT", 1, 65535); err != nil {
		return Config{}, err
	}
	for _, key := range []string{"BMKG_BASE_URL", "PVMBG_BASE_URL", "SCHEMA_REGISTRY_URL"} {
		if err = envURL(v, key, "http", "https"); err != nil {
			return Config{}, err
		}
	}
	if err = envURL(v, "DATABASE_URL", "postgres", "postgresql"); err != nil {
		return Config{}, err
	}
	if err = envURL(v, "NATS_URL", "nats", "tls", "ws", "wss"); err != nil {
		return Config{}, err
	}
	durations := map[string]time.Duration{}
	for _, key := range []string{"POLL_INTERVAL", "SOURCE_RETRY_MAX_SEC", "BMKG_FRESHNESS_SEC", "PVMBG_FRESHNESS_SEC"} {
		n, err := envInt(v, key, 1, int((1<<63-1)/int64(time.Second)))
		if err != nil {
			return Config{}, err
		}
		durations[key] = time.Duration(n) * time.Second
	}
	if durations["SOURCE_RETRY_MAX_SEC"] < durations["POLL_INTERVAL"] {
		return Config{}, errors.New("SOURCE_RETRY_MAX_SEC must be at least POLL_INTERVAL")
	}
	if v["NATS_LIFECYCLE_STREAM"] == v["NATS_STREAM"] || v["NATS_LIFECYCLE_SUBJECT"] == v["NATS_SUBJECT"] {
		return Config{}, errors.New("canonical and lifecycle streams/subjects must be distinct")
	}
	return Config{AdminToken: v["AGGREGATOR_ADMIN_TOKEN"], BMKGSeismicReaderSchema: v["BMKG_SEISMIC_READER_SCHEMA"], BMKGTsunamiReaderSchema: v["BMKG_TSUNAMI_READER_SCHEMA"], PVMBGReaderSchema: v["PVMBG_READER_SCHEMA"], Port: v["AGGREGATOR_PORT"], BMKGBaseURL: v["BMKG_BASE_URL"], BMKGAPIKey: v["BMKG_API_KEY"], PVMBGBaseURL: v["PVMBG_BASE_URL"], PVMBGToken: v["PVMBG_TOKEN"], PollInterval: durations["POLL_INTERVAL"], MaxBackoff: durations["SOURCE_RETRY_MAX_SEC"], BMKGFreshness: durations["BMKG_FRESHNESS_SEC"], PVMBGFreshness: durations["PVMBG_FRESHNESS_SEC"], DatabaseURL: v["DATABASE_URL"], NATSURL: v["NATS_URL"], NATSStream: v["NATS_STREAM"], NATSSubject: v["NATS_SUBJECT"], SchemaRegistryURL: v["SCHEMA_REGISTRY_URL"], LifecycleStream: v["NATS_LIFECYCLE_STREAM"], LifecycleSubject: v["NATS_LIFECYCLE_SUBJECT"]}, nil
}
