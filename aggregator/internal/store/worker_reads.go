package store

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Each group returns only its ordered page, rather than one page per shard.
// Taking the top K per shard, then per group, preserves the global top K.
func groupHazardQuery(f Filters, shards []int64) (string, []any) {
	parts := make([]string, 0, len(shards))
	var args []any
	for _, shard := range shards {
		table := pgx.Identifier{fmt.Sprintf("hazard_events_%d", shard)}.Sanitize()
		query, values := hazardQuery(f, table, true)
		parts = append(parts, "("+query+")")
		args = values // Identical filters share the same placeholders in every branch.
	}
	query := "SELECT * FROM (" + strings.Join(parts, " UNION ALL ") + ") AS group_events ORDER BY occurred_at DESC,hazard_id DESC"
	if f.Limit > 0 {
		query += " LIMIT $" + strconv.Itoa(len(args))
	}
	return query, args
}

// Pools are keyed by the current catalog member, so failover cannot reuse a
// connection addressed to a different primary. The caller also checks recovery.
// Worker connections are reserved from the configured per-member budget. Field
// gets one connection; general reads use one or two. At the default budgets,
// with worker reads enabled, six Aggregators need at most 54 connections per member.
func (s *Store) workerConnection(ctx context.Context, member storageMember) (*pgxpool.Conn, error) {
	key := net.JoinHostPort(member.Host, strconv.Itoa(member.Port))
	s.workerMu.Lock()
	pool := s.workerPools[key]
	if pool == nil {
		cfg := s.Pool.Config()
		cfg.ConnConfig.Host = member.Host
		cfg.ConnConfig.Port = uint16(member.Port)
		cfg.ConnConfig.ConnectTimeout = 500 * time.Millisecond
		cfg.MaxConns, cfg.MinConns = s.workerLimit, 0
		cfg.MaxConnIdleTime = 5 * time.Second
		cfg.HealthCheckPeriod = time.Second
		var err error
		pool, err = pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			s.workerMu.Unlock()
			return nil, err
		}
		if s.workerPools == nil {
			s.workerPools = make(map[string]*pgxpool.Pool)
		}
		s.workerPools[key] = pool
	}
	s.workerMu.Unlock()
	return pool.Acquire(ctx)
}
