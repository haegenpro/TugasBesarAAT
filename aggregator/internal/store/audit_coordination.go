package store

import (
	"context"
	"fmt"
	"time"

	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type CaptureToken struct {
	ID     string `json:"capture_id"`
	Epoch  int64  `json:"epoch"`
	Holder string `json:"holder"`
}

// checkAuditAdmission holds the gate row SHARE lock through registration. A
// closing gate takes UPDATE, so no writer can register outside the drained set.
// It expires by database time even if the capture process never resumes.
func checkAuditAdmission(ctx context.Context, tx pgx.Tx) error {
	var expires *time.Time
	if err := tx.QueryRow(ctx, "SELECT expires_at FROM platform_audit_gate WHERE singleton FOR SHARE").Scan(&expires); err != nil {
		return err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	if expires != nil && now.Before(*expires) {
		return canonical.Unavailable()
	}
	return nil
}

// CaptureBoundary closes canonical admission, drains prior attempts and invokes
// the storage owner's real view-pinning callback. The callback must establish
// every expected primary view and return its cleanup function, not just BEGIN.
// The returned cleanup remains the caller's responsibility during copying.
func (s *Store) CaptureBoundary(ctx context.Context, holder string, source *domain.Source,
	pin func(context.Context) (func(), error)) (CaptureToken, func(), error) {
	var token CaptureToken
	if holder == "" || pin == nil || s.Canonical.Durability == nil || (source != nil && !domain.ValidSource(string(*source))) {
		return token, nil, canonical.Unavailable()
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	topology, _, err := s.primaryTopology(ctx)
	if err != nil {
		return token, nil, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return token, nil, err
	}
	defer tx.Rollback(context.Background())
	if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return token, nil, err
	}
	var epoch int64
	var expires *time.Time
	if err = tx.QueryRow(ctx, `SELECT epoch,expires_at FROM platform_audit_gate WHERE singleton FOR UPDATE`).Scan(&epoch, &expires); err != nil {
		return token, nil, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return token, nil, err
	}
	if (expires != nil && now.Before(*expires)) || epoch == int64(^uint64(0)>>1) {
		return token, nil, canonical.Unavailable()
	}
	// Serialize with a renewal/publication already in flight, so an old build
	// cannot regain a live slot after this transaction admits another capture.
	builds, err := tx.Query(ctx, "SELECT capture_id FROM platform_audit_captures WHERE state='building' FOR UPDATE")
	if err != nil {
		return token, nil, err
	}
	for builds.Next() {
	}
	err = builds.Err()
	builds.Close()
	if err != nil {
		return token, nil, err
	}
	var active bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM platform_audit_captures WHERE state='building'
 AND owner_expires_at>clock_timestamp() AND copy_deadline>clock_timestamp())`).Scan(&active); err != nil {
		return token, nil, err
	}
	if active {
		return token, nil, canonical.Unavailable()
	}
	token = CaptureToken{ID: uuid.NewString(), Epoch: epoch + 1, Holder: holder}
	_, err = tx.Exec(ctx, `UPDATE platform_audit_gate SET epoch=$1,capture_id=$2,holder=$3,expires_at=clock_timestamp()+interval '2 seconds' WHERE singleton`, token.Epoch, token.ID, holder)
	if err != nil {
		return token, nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO platform_audit_captures(capture_id,epoch,holder,source,state,topology,owner_expires_at,copy_deadline,absolute_expires_at)
 VALUES($1,$2,$3,$4,'building',$5,clock_timestamp()+interval '15 seconds',clock_timestamp()+interval '5 minutes',clock_timestamp()+interval '1 hour')`, token.ID, token.Epoch, holder, source, topology)
	if err != nil {
		return token, nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return token, nil, err
	}
	failed := true
	var release func()
	defer func() {
		if failed {
			if release != nil {
				release()
			}
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_ = s.AbortCapture(cleanup, token, "capture boundary not established")
		}
	}()
	for {
		if err = s.RecoverAttempts(ctx); err != nil {
			return token, nil, err
		}
		var pending bool
		if err = s.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM canonical_attempts WHERE state='pending')").Scan(&pending); err != nil {
			return token, nil, err
		}
		if !pending {
			break
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return token, nil, canonical.Unavailable()
		case <-timer.C:
		}
	}
	if err = s.distributedSettled(ctx, topology); err != nil {
		return token, nil, err
	}
	if err = s.checkCaptureGate(ctx, token); err != nil {
		return token, nil, err
	}
	release, err = pin(ctx)
	if err != nil || release == nil {
		return token, nil, canonical.Unavailable()
	}
	current, _, err := s.primaryTopology(ctx)
	if err != nil || current != topology {
		return token, nil, canonical.Unavailable()
	}
	tx, err = s.Pool.Begin(ctx)
	if err != nil {
		return token, nil, err
	}
	defer tx.Rollback(context.Background())
	if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return token, nil, err
	}
	// UPDATE locks the exact still-live gate through metadata commit and opening.
	row, err := tx.Exec(ctx, `UPDATE platform_audit_gate SET capture_id=NULL,holder=NULL,expires_at=NULL
 WHERE singleton AND epoch=$1 AND capture_id=$2 AND holder=$3 AND expires_at>clock_timestamp()`, token.Epoch, token.ID, token.Holder)
	if err != nil || row.RowsAffected() != 1 {
		return token, nil, canonical.Unavailable()
	}
	_, err = tx.Exec(ctx, `UPDATE platform_audit_captures SET views_pinned=true,cut_at=clock_timestamp()
 WHERE capture_id=$1 AND epoch=$2 AND holder=$3 AND state='building'`, token.ID, token.Epoch, token.Holder)
	if err != nil {
		return token, nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return token, nil, err
	}
	failed = false
	return token, release, nil
}

func (s *Store) checkCaptureGate(ctx context.Context, token CaptureToken) error {
	var valid bool
	err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM platform_audit_gate WHERE singleton AND
 epoch=$1 AND capture_id=$2 AND holder=$3 AND expires_at>clock_timestamp())`, token.Epoch, token.ID, token.Holder).Scan(&valid)
	if err != nil || !valid {
		return canonical.Unavailable()
	}
	return nil
}

// ProtectCaptureCopy fences caller-owned distributed copy/progress writes.
func (s *Store) ProtectCaptureCopy(ctx context.Context, tx pgx.Tx, token CaptureToken) error {
	if s.Canonical.Durability == nil {
		return canonical.Unavailable()
	}
	if err := s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SET LOCAL idle_in_transaction_session_timeout='2s'; SET LOCAL statement_timeout='2s'"); err != nil {
		return err
	}
	var valid bool
	err := tx.QueryRow(ctx, `SELECT epoch=$2 AND holder=$3 AND state='building' AND views_pinned
 AND owner_expires_at>clock_timestamp() AND copy_deadline>clock_timestamp()
 FROM platform_audit_captures WHERE capture_id=$1 FOR SHARE`, token.ID, token.Epoch, token.Holder).Scan(&valid)
	if err != nil || !valid {
		return canonical.Unavailable()
	}
	_, err = tx.Exec(ctx, `INSERT INTO platform_capture_checks(transaction_id,capture_id,epoch,holder)
 VALUES(txid_current(),$1,$2,$3) ON CONFLICT DO NOTHING`, token.ID, token.Epoch, token.Holder)
	return err
}

func (s *Store) RenewCapture(ctx context.Context, token CaptureToken) error {
	return s.captureUpdate(ctx, `UPDATE platform_audit_captures SET owner_expires_at=LEAST(clock_timestamp()+interval '15 seconds',copy_deadline)
 WHERE capture_id=$1 AND epoch=$2 AND holder=$3 AND state='building' AND views_pinned AND owner_expires_at>clock_timestamp() AND copy_deadline>clock_timestamp()`, token)
}

// PublishCapture requires a storage-owner proof in this same fenced transaction.
// That callback must establish complete, replicated immutable units and valid
// retained views. Platform metadata alone is never evidence of a full snapshot.
func (s *Store) PublishCapture(ctx context.Context, token CaptureToken, complete func(context.Context, pgx.Tx) error) error {
	if complete == nil {
		return canonical.Unavailable()
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.ProtectCaptureCopy(ctx, tx, token); err != nil {
		return err
	}
	if err = complete(ctx, tx); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE platform_audit_captures SET state='ready',idle_expires_at=LEAST(clock_timestamp()+interval '15 minutes',absolute_expires_at)
 WHERE capture_id=$1 AND epoch=$2 AND holder=$3`, token.ID, token.Epoch, token.Holder)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return s.confirmCapture(ctx, token.ID, "ready")
}

func (s *Store) confirmCapture(ctx context.Context, id, state string) error {
	adapter, ok := s.Canonical.Durability.(interface {
		ConfirmCapture(context.Context, string, string) error
	})
	if !ok {
		return canonical.Unavailable()
	}
	return adapter.ConfirmCapture(ctx, id, state)
}

func (s *Store) captureUpdate(ctx context.Context, query string, token CaptureToken) error {
	if s.Canonical.Durability == nil {
		return canonical.Unavailable()
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, query, token.ID, token.Epoch, token.Holder)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return canonical.Unavailable()
	}
	return tx.Commit(ctx)
}

func (s *Store) AbortCapture(ctx context.Context, token CaptureToken, reason string) error {
	if len(reason) > 512 {
		reason = reason[:512]
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if s.Canonical.Durability == nil {
		return canonical.Unavailable()
	}
	if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "SELECT singleton FROM platform_audit_gate WHERE singleton FOR UPDATE"); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE platform_audit_captures SET state='aborted',reason=$4 WHERE capture_id=$1 AND epoch=$2 AND holder=$3 AND state='building'`, token.ID, token.Epoch, token.Holder, reason)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE platform_audit_gate SET capture_id=NULL,holder=NULL,expires_at=NULL WHERE singleton AND capture_id=$1 AND epoch=$2 AND holder=$3`, token.ID, token.Epoch, token.Holder)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// TouchSnapshot is called only after the owning HTTP layer has authorized and
// validated the session/filter scope. Invalid/unauthorized traffic must not call it.
func (s *Store) TouchSnapshot(ctx context.Context, id string) error {
	if s.Canonical.Durability == nil {
		return canonical.Unavailable()
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.prepareSnapshotMetadata(ctx, tx); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE platform_audit_captures SET idle_expires_at=LEAST(clock_timestamp()+interval '15 minutes',absolute_expires_at)
 WHERE capture_id=$1 AND state='ready' AND idle_expires_at>clock_timestamp() AND absolute_expires_at>clock_timestamp()`, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("snapshot expired or not ready")
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return s.confirmSnapshotMetadata(ctx, id)
}

// ProtectSnapshotRead holds metadata protection through the caller's frozen-row
// query and COMMIT, excluding GC and rejecting pages that finish after expiry.
// Use a read-write transaction for the deferred bookkeeping receipt and commit
// before sending the response. The owning API validates auth and filters first.
func (s *Store) ProtectSnapshotRead(ctx context.Context, tx pgx.Tx, id string) error {
	if s.Canonical.Durability == nil {
		return canonical.Unavailable()
	}
	if err := s.prepareSnapshotMetadata(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SET LOCAL idle_in_transaction_session_timeout='2s'; SET LOCAL statement_timeout='2s'"); err != nil {
		return err
	}
	var valid bool
	err := tx.QueryRow(ctx, `SELECT state='ready' AND idle_expires_at>clock_timestamp() AND absolute_expires_at>clock_timestamp()
 FROM platform_audit_captures WHERE capture_id=$1 FOR SHARE`, id).Scan(&valid)
	if err != nil || !valid {
		return canonical.Unavailable()
	}
	_, err = tx.Exec(ctx, "INSERT INTO platform_snapshot_read_checks(transaction_id,capture_id) VALUES(txid_current(),$1) ON CONFLICT DO NOTHING", id)
	return err
}

// ExpireCaptures marks only dead builds/expired published metadata. The storage
// owner deletes frozen rows by these exact IDs, never by an unfenced global sweep.
func (s *Store) ExpireCaptures(ctx context.Context) ([]string, error) {
	if s.Canonical.Durability == nil {
		return nil, canonical.Unavailable()
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `UPDATE platform_audit_captures SET state=CASE WHEN state='ready' THEN 'expired' ELSE 'aborted' END,reason='platform expiry'
 WHERE (state='building' AND (owner_expires_at<=clock_timestamp() OR copy_deadline<=clock_timestamp()))
 OR (state='ready' AND (idle_expires_at<=clock_timestamp() OR absolute_expires_at<=clock_timestamp()))`)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT capture_id::text FROM platform_audit_captures WHERE state IN ('aborted','expired') AND cleaned_at IS NULL ORDER BY created_at LIMIT 32 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return ids, nil
}

// MarkCaptureCleaned follows durable removal of only this terminal capture's
// rows. ExpireCaptures keeps returning its ID until cleanup is acknowledged.
func (s *Store) MarkCaptureCleaned(ctx context.Context, id string) error {
	if s.Canonical.Durability == nil {
		return canonical.Unavailable()
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.Canonical.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "UPDATE platform_audit_captures SET cleaned_at=clock_timestamp() WHERE capture_id=$1 AND state IN ('aborted','expired')", id)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
