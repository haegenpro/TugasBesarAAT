package store

import (
	"aggregator/internal/canonical"
	"context"
	"github.com/jackc/pgx/v5"
	"time"
)

// Published immutable copies do not require healthy write quorums on unrelated
// workers. Renewal/read guards require durable authoritative coordinator metadata.
func (s *Store) prepareSnapshotMetadata(ctx context.Context, tx pgx.Tx) error {
	if adapter, ok := s.Canonical.Durability.(interface {
		PrepareSnapshotMetadata(context.Context, pgx.Tx) error
	}); ok {
		return adapter.PrepareSnapshotMetadata(ctx, tx)
	}
	if s.Canonical.Durability == nil {
		return canonical.Unavailable()
	}
	return s.Canonical.Durability.Prepare(ctx, tx)
}
func (s *Store) confirmSnapshotMetadata(ctx context.Context, id string) error {
	if adapter, ok := s.Canonical.Durability.(interface {
		ConfirmSnapshotMetadata(context.Context, string) error
	}); ok {
		return adapter.ConfirmSnapshotMetadata(ctx, id)
	}
	return s.confirmCapture(ctx, id, "ready")
}
func (r *replicatedDurability) PrepareSnapshotMetadata(ctx context.Context, tx pgx.Tx) error {
	if _, e := tx.Exec(ctx, "SET LOCAL synchronous_commit='on'"); e != nil {
		return canonical.Unavailable()
	}
	status, e := readReplication(ctx, tx)
	if e != nil {
		return canonical.Unavailable()
	}
	if _, _, e = r.quorum(status, r.cfg.Groups[0]); e != nil {
		return canonical.Unavailable()
	}
	return nil
}
func (r *replicatedDurability) ConfirmSnapshotMetadata(ctx context.Context, id string) error {
	conn, e := r.s.Pool.Acquire(ctx)
	if e != nil {
		return canonical.Unavailable()
	}
	defer conn.Release()
	before, e := readReplication(ctx, conn)
	if e != nil {
		return canonical.Unavailable()
	}
	target, e := lsn(before.InsertLSN)
	if e != nil {
		return canonical.Unavailable()
	}
	for {
		var valid bool
		e = conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM platform_audit_captures WHERE capture_id=$1 AND state='ready' AND idle_expires_at>clock_timestamp() AND absolute_expires_at>clock_timestamp())`, id).Scan(&valid)
		if e != nil || !valid {
			return canonical.Unavailable()
		}
		after, e := readReplication(ctx, conn)
		if e != nil || !samePrimary(before, after) {
			return canonical.Unavailable()
		}
		n, candidates, e := r.quorum(after, r.cfg.Groups[0])
		if e != nil {
			return canonical.Unavailable()
		}
		local, e := lsn(after.FlushLSN)
		copies := map[string]bool{}
		for _, peer := range after.Replicas {
			flush, err := lsn(peer.FlushLSN)
			if err == nil && flush >= target && candidates[peer.Member] && peer.State == "streaming" && (peer.SyncState == "sync" || peer.SyncState == "quorum") {
				copies[peer.Member] = true
			}
		}
		if e == nil && local >= target && len(copies) >= n {
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return canonical.Unavailable()
		case <-timer.C:
		}
	}
}
