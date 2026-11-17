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
	kept, err := s.PreserveBatch(ctx, owner, []rawinput.Envelope{e})
	if err != nil {
		return e, err
	}
	return kept[0], nil
}

func (s *SourceStore) PreserveBatch(ctx context.Context, owner canonical.Ownership, envelopes []rawinput.Envelope) ([]rawinput.Envelope, error) {
	if len(envelopes) == 0 {
		return nil, nil
	}
	positions := make([]int64, len(envelopes))
	bodies := make([]string, len(envelopes))
	seen := make(map[int64]bool, len(envelopes))
	for i, e := range envelopes {
		if err := rawinput.Validate(e); err != nil {
			return nil, err
		}
		if e.Source != s.Source || e.Namespace != s.Namespace || seen[*e.Position] {
			return nil, canonical.Fail(400, "invalid_request", "source receipt scope mismatch")
		}
		body, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		positions[i], bodies[i], seen[*e.Position] = *e.Position, string(body), true
	}
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return nil, canonical.Unavailable()
	}
	defer tx.Rollback(context.Background())
	if err = s.Durability.Prepare(ctx, tx); err != nil {
		return nil, err
	}
	if err = s.Fence.Protect(ctx, tx, owner, []canonical.Mutation{{Source: domain.Source(s.Source)}}); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO source_envelopes(source,namespace,position,envelope) SELECT $1,$2,p,e FROM unnest($3::bigint[],$4::jsonb[]) AS t(p,e) ON CONFLICT DO NOTHING`, s.Source, s.Namespace, positions, bodies)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT position,envelope FROM source_envelopes WHERE source=$1 AND namespace=$2 AND position=ANY($3)`, s.Source, s.Namespace, positions)
	if err != nil {
		return nil, err
	}
	stored := make(map[int64]rawinput.Envelope, len(envelopes))
	for rows.Next() {
		var position int64
		var body []byte
		var original rawinput.Envelope
		if err = rows.Scan(&position, &body); err == nil {
			err = json.Unmarshal(body, &original)
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		stored[position] = original
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	kept := make([]rawinput.Envelope, len(envelopes))
	for i, e := range envelopes {
		original, ok := stored[*e.Position]
		if !ok || original.MessageID != e.MessageID || original.Digest != e.Digest || original.ContentType != e.ContentType {
			return nil, canonical.Fail(409, "identity_conflict", "source offset reused with different input")
		}
		kept[i] = original
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, canonical.Unavailable()
	}
	if err = s.Durability.ConfirmRecord(ctx, `SELECT count(*)=$4 FROM source_envelopes WHERE source=$1 AND namespace=$2 AND position=ANY($3)`, s.Source, s.Namespace, positions, len(positions)); err != nil {
		return nil, err
	}
	return kept, nil
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
