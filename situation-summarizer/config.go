package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	Port            string
	AggregatorURL   string
	OpenRouterURL   string
	APIKey          string
	SourceToken     string
	Model           string
	FallbackModels  []string
	Window          time.Duration
	CheckInterval   time.Duration
	RefreshInterval time.Duration
	UrgentDelay     time.Duration
	MinCallInterval time.Duration
	LLMTimeout      time.Duration
	MaxCallsPerDay  int
	MaxHighlights   int
	MaxTokens       int
	ReasoningTokens int
	ChatTimeout     time.Duration
	ChatCallsPerDay int
	ChatConcurrency int
	ChatHistory     int
	Fault           string
}

func loadConfig() (config, error) {
	c := config{
		Port:          os.Getenv("SUMMARIZER_PORT"),
		AggregatorURL: strings.TrimRight(os.Getenv("AGGREGATOR_BASE_URL"), "/"),
		OpenRouterURL: strings.TrimRight(envOr("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"), "/"),
		APIKey:        os.Getenv("OPENROUTER_API_KEY"),
		SourceToken:   os.Getenv("SUMMARY_SERVICE_TOKEN"),
		Model:         os.Getenv("SUMMARY_MODEL"),
		Fault:         envOr("SUMMARY_FAULT_MODE", "off"),
	}
	for key, value := range map[string]string{"SUMMARIZER_PORT": c.Port, "AGGREGATOR_BASE_URL": c.AggregatorURL, "OPENROUTER_API_KEY": c.APIKey, "SUMMARY_MODEL": c.Model} {
		if value == "" {
			return c, fmt.Errorf("missing %s", key)
		}
	}
	if port, err := strconv.Atoi(c.Port); err != nil || port < 1 || port > 65535 {
		return c, fmt.Errorf("invalid SUMMARIZER_PORT")
	}
	for key, raw := range map[string]string{"AGGREGATOR_BASE_URL": c.AggregatorURL, "OPENROUTER_BASE_URL": c.OpenRouterURL} {
		if u, err := url.Parse(raw); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return c, fmt.Errorf("invalid %s", key)
		}
	}
	for _, model := range strings.Split(os.Getenv("SUMMARY_FALLBACK_MODELS"), ",") {
		if model = strings.TrimSpace(model); model != "" && model != c.Model {
			c.FallbackModels = append(c.FallbackModels, model)
		}
	}
	var err error
	durations := []struct {
		key      string
		fallback time.Duration
		target   *time.Duration
	}{
		{"SUMMARY_WINDOW", 2 * time.Hour, &c.Window},
		{"SUMMARY_CHECK_INTERVAL", time.Minute, &c.CheckInterval},
		{"SUMMARY_REFRESH_INTERVAL", 30 * time.Minute, &c.RefreshInterval},
		{"SUMMARY_URGENT_DELAY", 30 * time.Second, &c.UrgentDelay},
		{"SUMMARY_MIN_CALL_INTERVAL", 2 * time.Minute, &c.MinCallInterval},
		{"SUMMARY_LLM_TIMEOUT", 60 * time.Second, &c.LLMTimeout},
		{"CHAT_TIMEOUT", 60 * time.Second, &c.ChatTimeout},
	}
	for _, d := range durations {
		if *d.target, err = envDuration(d.key, d.fallback); err != nil {
			return c, err
		}
	}
	ints := []struct {
		key              string
		fallback, lo, hi int
		target           *int
	}{
		{"SUMMARY_MAX_CALLS_PER_DAY", 45, 1, 10000, &c.MaxCallsPerDay},
		{"SUMMARY_MAX_HIGHLIGHTS", 8, 1, 20, &c.MaxHighlights},
		{"LLM_MAX_TOKENS", 4096, 2000, 16384, &c.MaxTokens},
		{"LLM_REASONING_TOKENS", 1024, 256, 4096, &c.ReasoningTokens},
		{"CHAT_MAX_CALLS_PER_DAY", 150, 0, 10000, &c.ChatCallsPerDay},
		{"CHAT_MAX_CONCURRENT", 2, 1, 16, &c.ChatConcurrency},
		{"CHAT_MAX_HISTORY", 8, 0, 20, &c.ChatHistory},
	}
	for _, n := range ints {
		if *n.target, err = envInt(n.key, n.fallback, n.lo, n.hi); err != nil {
			return c, err
		}
	}
	if c.ReasoningTokens*2 > c.MaxTokens {
		return c, fmt.Errorf("LLM_REASONING_TOKENS must leave at least half of LLM_MAX_TOKENS for the answer")
	}
	if c.Fault != "off" && c.Fault != "slow" && c.Fault != "invalid" {
		return c, fmt.Errorf("SUMMARY_FAULT_MODE must be off, slow or invalid")
	}
	return c, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return d, nil
}

func envInt(key string, fallback, lo, hi int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return n, nil
}
