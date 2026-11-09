package main

import (
	"consumernotifier/internal/platform"
	"context"
	"database/sql"
	"sort"
	"time"
)

func configurePool(db *sql.DB) {
	max := platform.Int("DB_MAX_OPEN_CONNS", 16)
	if max < 1 {
		panic("DB_MAX_OPEN_CONNS must be positive")
	}
	db.SetMaxOpenConns(max)
	db.SetMaxIdleConns(platform.Int("DB_MAX_IDLE_CONNS", 8))
	db.SetConnMaxLifetime(30 * time.Minute)
}
func lockApplication(ctx context.Context, tx *sql.Tx, keys ...string) error {
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || chr(31) || $1,0))`, key); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) collectMetrics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			st := a.db.Stats()
			platform.Gauge("db_connections_in_use", float64(st.InUse))
			platform.Gauge("db_connections_idle", float64(st.Idle))
			platform.Gauge("db_pool_wait_total", float64(st.WaitCount))
			var count int64
			if a.db.QueryRowContext(ctx, `SELECT count(*) FROM processed_messages`).Scan(&count) == nil {
				platform.Gauge("processed_records", float64(count))
			}
		}
	}
}
