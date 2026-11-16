package ingestion

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type Progress struct {
	Position int64
	Cursor   *string
}

type SourceStore struct {
	DB         *store.Store
	Durability Durability
	Fence      Fence
	Source     string
	Namespace  string
}

func (s *SourceStore) Check(ctx context.Context) error {
	if s.DB == nil || s.Durability == nil || s.Fence == nil || s.Source == "" || s.Namespace == "" {
		return canonical.Unavailable()
	}
	_, err := s.DB.Pool.Exec(ctx, `SELECT position FROM source_progress LIMIT 0`)
	return err
}

func (s *SourceStore) Progress(ctx context.Context, start int64) (Progress, error) {
	p := Progress{Position: start}
	err := s.DB.Pool.QueryRow(ctx, `SELECT position,cursor FROM source_progress WHERE source=$1 AND namespace=$2`, s.Source, s.Namespace).Scan(&p.Position, &p.Cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil
	}
	return p, err
}

// Preserve freezes the first receipt before publication. Reacquisition after a
// crash must not give an existing offset a new timestamp, trace or owner: the
// canonical history fingerprints the complete envelope, not just its payload.
func (s *SourceStore) Preserve(ctx context.Context, owner canonical.Ownership, e rawinput.Envelope) (rawinput.Envelope, error) {
	if err := rawinput.Validate(e); err != nil {
		return e, err
	}
	if e.Source != s.Source || e.Namespace != s.Namespace {
		return e, canonical.Fail(400, "invalid_request", "source receipt scope mismatch")
	}
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return e, canonical.Unavailable()
	}
	defer tx.Rollback(context.Background())
	if err = s.Durability.Prepare(ctx, tx); err != nil {
		return e, err
	}
	if err = s.Fence.Protect(ctx, tx, owner, []canonical.Mutation{{Source: domain.Source(s.Source)}}); err != nil {
		return e, err
	}
	body, err := json.Marshal(e)
	if err != nil {
		return e, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO source_envelopes(source,namespace,position,envelope) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, s.Source, s.Namespace, *e.Position, body)
	if err != nil {
		return e, err
	}
	var kept []byte
	if err = tx.QueryRow(ctx, `SELECT envelope FROM source_envelopes WHERE source=$1 AND namespace=$2 AND position=$3`, s.Source, s.Namespace, *e.Position).Scan(&kept); err != nil {
		return e, err
	}
	var original rawinput.Envelope
	if err = json.Unmarshal(kept, &original); err != nil {
		return e, err
	}
	if original.MessageID != e.MessageID || original.Digest != e.Digest || original.ContentType != e.ContentType {
		return e, canonical.Fail(409, "identity_conflict", "source offset reused with different input")
	}
	if err = tx.Commit(ctx); err != nil {
		return e, canonical.Unavailable()
	}
	err = s.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM source_envelopes WHERE source=$1 AND namespace=$2 AND position=$3)`, s.Source, s.Namespace, *e.Position)
	return original, err
}

func (s *SourceStore) Advance(ctx context.Context, owner canonical.Ownership, p Progress) error {
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return canonical.Unavailable()
	}
	defer tx.Rollback(context.Background())
	if err = s.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	if err = s.Fence.Protect(ctx, tx, owner, []canonical.Mutation{{Source: domain.Source(s.Source)}}); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO source_progress(source,namespace,position,cursor) VALUES($1,$2,$3,$4) ON CONFLICT(source,namespace) DO UPDATE SET position=excluded.position,cursor=excluded.cursor,updated_at=clock_timestamp() WHERE source_progress.position<=excluded.position`, s.Source, s.Namespace, p.Position, p.Cursor)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM source_envelopes WHERE source=$1 AND namespace=$2 AND position<(SELECT position FROM source_progress WHERE source=$1 AND namespace=$2)`, s.Source, s.Namespace); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	return s.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM source_progress WHERE source=$1 AND namespace=$2 AND position>=$3)`, s.Source, s.Namespace, p.Position)
}

func (s *SourceStore) Quarantine(ctx context.Context, e rawinput.Envelope, payload []byte, reason string) error {
	if e.MessageID == "" || reason == "" {
		return canonical.Fail(400, "invalid_request", "quarantine requires an identified message and a reason")
	}
	received, err := time.Parse(time.RFC3339Nano, e.ReceivedAt)
	if err != nil {
		received = time.Now().UTC()
	}
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return canonical.Unavailable()
	}
	defer tx.Rollback(context.Background())
	if err = s.Durability.Prepare(ctx, tx); err != nil {
		return err
	}
	var inserted bool
	err = tx.QueryRow(ctx, `INSERT INTO ingestion_failures(namespace,message_id,source,position,received_at,reason,classification,status,attempts) VALUES($1,$2,$3,$4,$5,$6,'incompatible','parked',1) ON CONFLICT(namespace,message_id) DO NOTHING RETURNING true`, e.Namespace, e.MessageID, e.Source, e.Position, received, reason).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Rollback(ctx)
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ingestion_failure_attempts(namespace,message_id,attempt,reason) VALUES($1,$2,1,$3)`, e.Namespace, e.MessageID, reason); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ingestion_failure_payloads(namespace,message_id,content_type,payload,sha256) VALUES($1,$2,$3,$4,$5)`, e.Namespace, e.MessageID, e.ContentType, payload, rawinput.Hash(payload)); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	return s.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_failure_payloads WHERE namespace=$1 AND message_id=$2)`, e.Namespace, e.MessageID)
}
