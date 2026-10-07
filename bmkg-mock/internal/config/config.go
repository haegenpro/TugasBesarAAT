package config

type Config struct {
	Port            string
	APIKey          string
	DelayMs         int
	EventIntervalMs int
	RegistryURL     string
	ArchivePath     string
}

func Load() (Config, error) {
	v, err := requiredEnv("BMKG_PORT", "BMKG_API_KEY", "BMKG_DELAY_MS", "BMKG_EVENT_INTERVAL_MS", "SCHEMA_REGISTRY_URL", "SOURCE_ARCHIVE_PATH")
	if err != nil {
		return Config{}, err
	}
	if _, err = envInt(v, "BMKG_PORT", 1, 65535); err != nil {
		return Config{}, err
	}
	delay, err := envInt(v, "BMKG_DELAY_MS", 0, 0)
	if err != nil {
		return Config{}, err
	}
	interval, err := envInt(v, "BMKG_EVENT_INTERVAL_MS", 1, 10000)
	if err != nil {
		return Config{}, err
	}
	if err = envURL(v, "SCHEMA_REGISTRY_URL", "http", "https"); err != nil {
		return Config{}, err
	}
	return Config{Port: v["BMKG_PORT"], APIKey: v["BMKG_API_KEY"], DelayMs: delay, EventIntervalMs: interval, RegistryURL: v["SCHEMA_REGISTRY_URL"], ArchivePath: v["SOURCE_ARCHIVE_PATH"]}, nil
}
