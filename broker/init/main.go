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
	NATSURL       string
	StreamName    string
	StreamSubject string
	StreamMaxAge  time.Duration
}

func loadConfig() (Config, error) {
	natsURL, err := requireEnv("NATS_URL")
	if err != nil {
		return Config{}, err
	}

	streamName, err := requireEnv("NATS_STREAM_NAME")
	if err != nil {
		return Config{}, err
	}

	streamSubject, err := requireEnv("NATS_STREAM_SUBJECT")
	if err != nil {
		return Config{}, err
	}

	maxAgeRaw, err := requireEnv("NATS_STREAM_MAX_AGE")
	if err != nil {
		return Config{}, err
	}

	maxAge, err := time.ParseDuration(maxAgeRaw)
	if err != nil {
		return Config{}, fmt.Errorf("Invalid NATS_STREAM_MAX_AGE %q: %w", maxAgeRaw, err)
	}

	return Config{
		NATSURL:       natsURL,
		StreamName:    streamName,
		StreamSubject: streamSubject,
		StreamMaxAge:  maxAge,
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

	return nil
}

func main() {
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
