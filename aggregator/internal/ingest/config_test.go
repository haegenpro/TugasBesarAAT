package ingest

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestIngestionDatabaseOverridesReachConnectionConfiguration(t *testing.T) {
	t.Setenv("DB_MAX_OPEN_CONNS", "5")
	t.Setenv("DB_STATEMENT_TIMEOUT", "900ms")
	t.Setenv("DB_LOCK_TIMEOUT", "250ms")
	t.Setenv("DB_IDLE_TRANSACTION_TIMEOUT", "4s")
	s, err := New(context.Background(), "postgres://test:test@127.0.0.1:1/unused?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	cfg := s.Pool.Config()
	if cfg.MaxConns != 5 {
		t.Fatalf("pool size = %d", cfg.MaxConns)
	}
	for key, want := range map[string]string{"statement_timeout": "900", "lock_timeout": "250", "idle_in_transaction_session_timeout": "4000"} {
		if got := cfg.ConnConfig.RuntimeParams[key]; got != want {
			t.Fatalf("%s = %s, want %s", key, got, want)
		}
	}
}

func TestSatelliteActivityUsesConfiguredIdleDeadline(t *testing.T) {
	expired := make(chan struct{})
	timer := time.AfterFunc(time.Hour, func() { close(expired) })
	defer timer.Stop()
	r := streamActivity{Reader: strings.NewReader("heartbeat"), timer: timer, timeout: 20 * time.Millisecond}
	if _, err := r.Read(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("activity reset ignored the configured idle deadline")
	}
}
