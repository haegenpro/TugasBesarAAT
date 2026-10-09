package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	LifecycleStream  string
	LifecycleSubject string
	LifecycleMaxAge  time.Duration
	NATSURL          string
	StreamName       string
	StreamSubject    string
	StreamMaxAge     time.Duration
	DLQStream        string
	DLQPrefix        string
	DLQMaxAge        time.Duration
}

func loadConfig() (Config, error) {
	v, err := requiredEnv("NATS_PORT", "NATS_MONITOR_PORT", "NATS_URL", "NATS_STREAM", "NATS_SUBJECT", "NATS_STREAM_MAX_AGE", "NATS_DLQ_STREAM", "NATS_DLQ_SUBJECT_PREFIX", "NATS_DLQ_MAX_AGE", "NATS_LIFECYCLE_STREAM", "NATS_LIFECYCLE_SUBJECT", "NATS_LIFECYCLE_MAX_AGE")
	if err != nil {
		return Config{}, err
	}
	for _, key := range []string{"NATS_PORT", "NATS_MONITOR_PORT"} {
		port, err := strconv.Atoi(v[key])
		if err != nil || port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("%s must be an integer between 1 and 65535", key)
		}
	}
	if v["NATS_PORT"] == v["NATS_MONITOR_PORT"] {
		return Config{}, errors.New("NATS_PORT and NATS_MONITOR_PORT must be different")
	}
	if err = envURL(v, "NATS_URL", "nats", "tls", "ws", "wss"); err != nil {
		return Config{}, err
	}

	natsURL, err := requireEnv("NATS_URL")
	if err != nil {
		return Config{}, err
	}

	streamName, err := requireEnv("NATS_STREAM")
	if err != nil {
		return Config{}, err
	}

	streamSubject, err := requireEnv("NATS_SUBJECT")
	if err != nil {
		return Config{}, err
	}

	maxAgeRaw, err := requireEnv("NATS_STREAM_MAX_AGE")
	if err != nil {
		return Config{}, err
	}

	maxAge, err := time.ParseDuration(maxAgeRaw)
	if err != nil || maxAge <= 0 {
		return Config{}, fmt.Errorf("invalid NATS_STREAM_MAX_AGE %q", maxAgeRaw)
	}
	dlqStream, err := requireEnv("NATS_DLQ_STREAM")
	if err != nil {
		return Config{}, err
	}
	dlqPrefix, err := requireEnv("NATS_DLQ_SUBJECT_PREFIX")
	if err != nil {
		return Config{}, err
	}
	dlqAgeRaw, err := requireEnv("NATS_DLQ_MAX_AGE")
	if err != nil {
		return Config{}, err
	}
	dlqAge, err := time.ParseDuration(dlqAgeRaw)
	if err != nil || dlqAge <= 0 {
		return Config{}, fmt.Errorf("invalid NATS_DLQ_MAX_AGE %q", dlqAgeRaw)
	}
	if dlqAge < maxAge {
		return Config{}, fmt.Errorf("NATS_DLQ_MAX_AGE must be at least NATS_STREAM_MAX_AGE")
	}

	lifecycleStream, err := requireEnv("NATS_LIFECYCLE_STREAM")
	if err != nil {
		return Config{}, err
	}
	lifecycleSubject, err := requireEnv("NATS_LIFECYCLE_SUBJECT")
	if err != nil {
		return Config{}, err
	}
	raw, err := requireEnv("NATS_LIFECYCLE_MAX_AGE")
	if err != nil {
		return Config{}, err
	}
	lifecycleAge, err := time.ParseDuration(raw)
	if err != nil || lifecycleAge <= 0 {
		return Config{}, fmt.Errorf("NATS_LIFECYCLE_MAX_AGE must be a positive duration")
	}
	if dlqAge < lifecycleAge {
		return Config{}, fmt.Errorf("DLQ retention must cover lifecycle retention")
	}
	return Config{
		LifecycleStream: lifecycleStream, LifecycleSubject: lifecycleSubject, LifecycleMaxAge: lifecycleAge,
		NATSURL:       natsURL,
		StreamName:    streamName,
		StreamSubject: streamSubject,
		StreamMaxAge:  maxAge,
		DLQStream:     dlqStream,
		DLQPrefix:     dlqPrefix,
		DLQMaxAge:     dlqAge,
	}, nil
}

func ensureStream(parent context.Context, js jetstream.JetStream, cfg Config) error {
	ctx, cancel := context.WithTimeout(withBrokerTrace(parent), 10*time.Second)
	defer cancel()

	streamConfig := jetstream.StreamConfig{
		Name: cfg.StreamName,
		Subjects: []string{
			cfg.StreamSubject,
		},
		Storage: jetstream.FileStorage,
		MaxAge:  cfg.StreamMaxAge,
	}

	stream, err := brokerCall(ctx, "stream_create_or_update", func() (jetstream.Stream, error) { return js.CreateOrUpdateStream(ctx, streamConfig) })
	if err != nil {
		return fmt.Errorf("create/update stream %q: %w", cfg.StreamName, err)
	}

	info, err := brokerCall(ctx, "stream_info", func() (*jetstream.StreamInfo, error) { return stream.Info(ctx) })
	if err != nil {
		return fmt.Errorf("retrieve stream %q info: %w", cfg.StreamName, err)
	}

	brokerLog(ctx, slog.LevelInfo, "stream ready", "stream", info.Config.Name, "subjects", info.Config.Subjects, "storage", info.Config.Storage, "max_age", info.Config.MaxAge.String())
	if cfg.LifecycleStream != "" {
		if _, err = brokerCall(ctx, "lifecycle_stream_create_or_update", func() (jetstream.Stream, error) {
			return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: cfg.LifecycleStream, Subjects: []string{cfg.LifecycleSubject}, Storage: jetstream.FileStorage, MaxAge: cfg.LifecycleMaxAge})
		}); err != nil {
			return fmt.Errorf("create lifecycle stream: %w", err)
		}
	}
	_, err = brokerCall(ctx, "dlq_stream_create_or_update", func() (jetstream.Stream, error) {
		return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: cfg.DLQStream, Subjects: []string{cfg.DLQPrefix + ".>"},
			Storage: jetstream.FileStorage, MaxAge: cfg.DLQMaxAge,
			Discard: jetstream.DiscardNew, DiscardNewPerSubject: true, MaxMsgsPerSubject: 1,
		})
	})
	if err != nil {
		return fmt.Errorf("create/update DLQ stream %q: %w", cfg.DLQStream, err)
	}

	return nil
}

// checkStream is a read-only health probe. It uses the configured NATS URL
// and verifies the stream and subject that the initializer is responsible for.
func checkStream(parent context.Context, cfg Config) error {
	ctx, cancel := context.WithTimeout(withBrokerTrace(parent), 3*time.Second)
	defer cancel()

	nc, err := brokerCall(ctx, "broker_connect", func() (*nats.Conn, error) {
		return nats.Connect(cfg.NATSURL, nats.Name("broker-health"), nats.Timeout(2*time.Second), nats.NoReconnect())
	})
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("create JetStream client: %w", err)
	}
	stream, err := brokerCall(ctx, "stream_lookup", func() (jetstream.Stream, error) { return js.Stream(ctx, cfg.StreamName) })
	if err != nil {
		return fmt.Errorf("find stream %q: %w", cfg.StreamName, err)
	}
	info, err := brokerCall(ctx, "stream_info", func() (*jetstream.StreamInfo, error) { return stream.Info(ctx) })
	if err != nil {
		return fmt.Errorf("inspect stream %q: %w", cfg.StreamName, err)
	}
	if info.Config.Storage != jetstream.FileStorage || info.Config.MaxAge != cfg.StreamMaxAge ||
		len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != cfg.StreamSubject {
		return fmt.Errorf("stream %q has unexpected configuration", cfg.StreamName)
	}
	if cfg.LifecycleStream != "" {
		lifecycle, err := brokerCall(ctx, "lifecycle_stream_lookup", func() (jetstream.Stream, error) { return js.Stream(ctx, cfg.LifecycleStream) })
		if err != nil {
			return err
		}
		info, err := brokerCall(ctx, "lifecycle_stream_info", func() (*jetstream.StreamInfo, error) { return lifecycle.Info(ctx) })
		if err != nil {
			return err
		}
		if info.Config.Storage != jetstream.FileStorage || info.Config.MaxAge != cfg.LifecycleMaxAge || len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != cfg.LifecycleSubject {
			return fmt.Errorf("unexpected lifecycle stream configuration")
		}
	}
	dlq, err := brokerCall(ctx, "dlq_stream_lookup", func() (jetstream.Stream, error) { return js.Stream(ctx, cfg.DLQStream) })
	if err != nil {
		return fmt.Errorf("find DLQ stream %q: %w", cfg.DLQStream, err)
	}
	dlqInfo, err := brokerCall(ctx, "dlq_stream_info", func() (*jetstream.StreamInfo, error) { return dlq.Info(ctx) })
	if err != nil {
		return err
	}
	if dlqInfo.Config.Storage != jetstream.FileStorage || dlqInfo.Config.MaxAge != cfg.DLQMaxAge ||
		dlqInfo.Config.Discard != jetstream.DiscardNew || !dlqInfo.Config.DiscardNewPerSubject || dlqInfo.Config.MaxMsgsPerSubject != 1 ||
		len(dlqInfo.Config.Subjects) != 1 || dlqInfo.Config.Subjects[0] != cfg.DLQPrefix+".>" {
		return fmt.Errorf("DLQ stream %q has unexpected configuration", cfg.DLQStream)
	}
	return nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx = withBrokerTrace(ctx)
	if err := run(ctx, os.Args[1:]); err != nil {
		brokerLog(ctx, slog.LevelError, "broker initializer failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	ctx = withBrokerTrace(ctx)
	checkOnly := len(args) == 1 && args[0] == "--check"
	validateOnly := len(args) == 1 && args[0] == "--validate"
	if len(args) > 0 && !checkOnly && !validateOnly {
		return errors.New("unsupported broker initializer arguments")
	}
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	if validateOnly {
		return nil
	}
	if checkOnly {
		return checkStream(ctx, cfg)
	}
	brokerLog(ctx, slog.LevelInfo, "starting broker initializer", "stream", cfg.StreamName, "subject", cfg.StreamSubject)
	nc, err := connectWithRetry(ctx, cfg.NATSURL)
	if errors.Is(err, context.Canceled) {
		brokerLog(ctx, slog.LevelInfo, "broker initialization cancelled")
		return nil
	}
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("create JetStream client: %w", err)
	}
	if err := ensureStream(ctx, js, cfg); err != nil {
		return err
	}
	brokerLog(ctx, slog.LevelInfo, "broker initialization complete")
	return nil
}

func requireEnv(key string) (string, error) {
	value := os.Getenv(key)
	if value == "" {
		return "", fmt.Errorf("environment variable %s is required", key)
	}

	return value, nil
}

func connectWithRetry(ctx context.Context, url string) (*nats.Conn, error) {
	const retryInterval = 2 * time.Second
	ctx = withBrokerTrace(ctx)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		nc, err := brokerCall(ctx, "broker_connect", func() (*nats.Conn, error) {
			return nats.Connect(url, nats.Name("broker-init"), nats.Timeout(3*time.Second))
		})
		if err == nil {
			brokerLog(ctx, slog.LevelInfo, "connected to NATS")
			return nc, nil
		}
		brokerLog(ctx, slog.LevelWarn, "NATS unavailable; retrying", "retry_interval_sec", retryInterval.Seconds())

		if err := sleepCtx(ctx, retryInterval); err != nil {
			return nil, err
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
