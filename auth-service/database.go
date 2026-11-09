package main

import (
	"authservice/internal/platform"
	"context"
	"database/sql"
	"time"
)

type Sessions interface {
	issue(context.Context, identity) (tokenPair, error)
	verify(context.Context, string) (principal, error)
	rotate(context.Context, string) (tokenPair, error)
	logout(context.Context, string) error
	Ping(context.Context) error
}

func (s *sessionStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func configurePool(db *sql.DB) {
	max := platform.Int("DB_MAX_OPEN_CONNS", 16)
	if max < 1 {
		panic("DB_MAX_OPEN_CONNS must be positive")
	}
	db.SetMaxOpenConns(max)
	db.SetMaxIdleConns(platform.Int("DB_MAX_IDLE_CONNS", 8))
	db.SetConnMaxLifetime(30 * time.Minute)
}

func (s *sessionStore) collectMetrics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			st := s.db.Stats()
			platform.Gauge("db_connections_in_use", float64(st.InUse))
			platform.Gauge("db_connections_idle", float64(st.Idle))
			platform.Gauge("db_pool_wait_total", float64(st.WaitCount))
		}
	}
}
