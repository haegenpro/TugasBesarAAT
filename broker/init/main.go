package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	v, err := requiredEnv("NATS_URL", "NATS_STREAM", "NATS_SUBJECT", "NATS_STREAM_MAX_AGE", "NATS_DLQ_STREAM", "NATS_DLQ_SUBJECT_PREFIX", "NATS_DLQ_MAX_AGE", "NATS_LIFECYCLE_STREAM", "NATS_LIFECYCLE_SUBJECT", "NATS_LIFECYCLE_MAX_AGE")
	if err != nil {
		return Config{}, err
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
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	streamConfig := jetstream.StreamConfig{
		Name: cfg.StreamName,
		Subjects: []string{
			cfg.StreamSubject,
		},
		Storage: jetstream.FileStorage,
		MaxAge:  cfg.StreamMaxAge,
	}

	stream, err := js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return fmt.Errorf("create/update stream %q: %w", cfg.StreamName, err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("retrieve stream %q info: %w", cfg.StreamName, err)
	}

	log.Printf(
		"stream ready: name=%s subjects=%v storage=%s max_age=%s",
		info.Config.Name,
		info.Config.Subjects,
		info.Config.Storage,
		info.Config.MaxAge,
	)
	if cfg.LifecycleStream != "" {
		if _, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: cfg.LifecycleStream, Subjects: []string{cfg.LifecycleSubject}, Storage: jetstream.FileStorage, MaxAge: cfg.LifecycleMaxAge}); err != nil {
			return fmt.Errorf("create lifecycle stream: %w", err)
		}
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: cfg.DLQStream, Subjects: []string{cfg.DLQPrefix + ".>"},
		Storage: jetstream.FileStorage, MaxAge: cfg.DLQMaxAge,
		Discard: jetstream.DiscardNew, DiscardNewPerSubject: true, MaxMsgsPerSubject: 1,
	})
	if err != nil {
		return fmt.Errorf("create/update DLQ stream %q: %w", cfg.DLQStream, err)
	}

	return nil
}

// checkStream is a read-only health probe. It uses the configured NATS URL
// and verifies the stream and subject that the initializer is responsible for.
func checkStream(parent context.Context, cfg Config) error {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("broker-health"), nats.Timeout(2*time.Second), nats.NoReconnect())
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("create JetStream client: %w", err)
	}
	stream, err := js.Stream(ctx, cfg.StreamName)
	if err != nil {
		return fmt.Errorf("find stream %q: %w", cfg.StreamName, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("inspect stream %q: %w", cfg.StreamName, err)
	}
	if info.Config.Storage != jetstream.FileStorage || info.Config.MaxAge != cfg.StreamMaxAge ||
		len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != cfg.StreamSubject {
		return fmt.Errorf("stream %q has unexpected configuration", cfg.StreamName)
	}
	if cfg.LifecycleStream != "" {
		lifecycle, err := js.Stream(ctx, cfg.LifecycleStream)
		if err != nil {
			return err
		}
		info, err := lifecycle.Info(ctx)
		if err != nil {
			return err
		}
		if info.Config.Storage != jetstream.FileStorage || info.Config.MaxAge != cfg.LifecycleMaxAge || len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != cfg.LifecycleSubject {
			return fmt.Errorf("unexpected lifecycle stream configuration")
		}
	}
	dlq, err := js.Stream(ctx, cfg.DLQStream)
	if err != nil {
		return fmt.Errorf("find DLQ stream %q: %w", cfg.DLQStream, err)
	}
	dlqInfo, err := dlq.Info(ctx)
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
	checkOnly := len(os.Args) == 2 && os.Args[1] == "--check"
	validateOnly := len(os.Args) == 2 && os.Args[1] == "--validate"
	if len(os.Args) > 1 && !checkOnly && !validateOnly {
		log.Fatalf("unsupported arguments: %v", os.Args[1:])
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}
	if validateOnly {
		return
	}
	if checkOnly {
		if err := checkStream(ctx, cfg); err != nil {
			log.Fatalf("broker unhealthy: %v", err)
		}
		return
	}

	log.Printf(
		"starting broker initializer: url=%s stream=%s subject=%s",
		cfg.NATSURL,
		cfg.StreamName,
		cfg.StreamSubject,
	)

	nc, err := connectWithRetry(ctx, cfg.NATSURL)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Println("broker initialization cancelled")
			return
		}

		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("failed to create JetStream client: %v", err)
	}

	if err := ensureStream(ctx, js, cfg); err != nil {
		log.Fatalf("failed to initialize stream: %v", err)
	}

	log.Println("broker initialization complete")
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

	for {
		nc, err := nats.Connect(
			url,
			nats.Name("broker-init"),
			nats.Timeout(3*time.Second),
		)
		if err == nil {
			log.Printf("connected to NATS: %s", nc.ConnectedUrl())
			return nc, nil
		}

		log.Printf("NATS unavailable: %v; retrying in %s", err, retryInterval)

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
