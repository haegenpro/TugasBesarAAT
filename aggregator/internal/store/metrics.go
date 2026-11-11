package store

import (
	"aggregator/internal/platform"
	"context"
	"time"
)

func (s *Store) CollectMetrics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			st := s.Pool.Stat()
			platform.Gauge("db_connections_in_use", float64(st.AcquiredConns()))
			platform.Gauge("db_connections_idle", float64(st.IdleConns()))
			platform.Gauge("db_pool_wait_total", float64(st.EmptyAcquireCount()))
			c, cancel := context.WithTimeout(ctx, 2*time.Second)
			var n int64
			if s.Pool.QueryRow(c, `SELECT (SELECT count(*) FROM hazard_outbox)+(SELECT count(*) FROM report_lifecycle_outbox)+(SELECT count(*) FROM canonical_lifecycle_outbox)`).Scan(&n) == nil {
				platform.Gauge("outbox_pending", float64(n))
			}
			cancel()
		}
	}
}
