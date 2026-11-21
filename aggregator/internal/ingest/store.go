// Package ingest owns raw source evidence, acceptance receipts and progress.
// Canonical storage is reached exclusively through its HTTP owner.
package ingest

import (
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

//go:embed schema.sql
var Schema string
var ErrConflict = errors.New("identity reused with different content")
var ErrGap = errors.New("noncontiguous source position")

type Store struct {
	Pool   *pgxpool.Pool
	Leases *ownership.Authority
}

func New(ctx context.Context, url string) (*Store, error) {
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, errors.New("invalid ingestion database URL")
	}
	cfg.MaxConns = int32(platform.Int("DB_MAX_OPEN_CONNS", 8))
	cfg.ConnConfig.Tracer = platform.SQLTracer{}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "10000"
	p, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	return &Store{p, ownership.New(p)}, nil
}
func (s *Store) Get(ctx context.Context, id int) (string, error) {
	var text string
	e := s.Pool.QueryRow(ctx, `SELECT schema FROM schema_cache WHERE id=$1`, id).Scan(&text)
	return text, e
}
func (s *Store) Put(ctx context.Context, id int, text string) error {
	r, e := s.Pool.Exec(ctx, `INSERT INTO schema_cache(id,schema) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET schema=excluded.schema WHERE schema_cache.schema=excluded.schema`, id, text)
	if e == nil && r.RowsAffected() != 1 {
		return ErrConflict
	}
	return e
}
func (s *Store) Find(ctx context.Context, text string) (int, error) {
	var id int
	e := s.Pool.QueryRow(ctx, `SELECT id FROM schema_cache WHERE schema=$1`, text).Scan(&id)
	return id, e
}

type Input struct {
	ID            string         `json:"message_id"`
	Source        string         `json:"source"`
	Namespace     string         `json:"namespace"`
	Position      int64          `json:"position"`
	Entity        string         `json:"entity_key"`
	Payload       []byte         `json:"payload"`
	Metadata      map[string]any `json:"metadata"`
	CorrelationID string         `json:"correlation_id"`
	ReceivedAt    time.Time      `json:"received_at"`
}

func Hash(b []byte) string     { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func WarningID(i Input) string { return "warning-v1:" + Hash([]byte(i.Namespace+"\x00"+i.ID)) }
func (s *Store) Progress(ctx context.Context, source, ns string, initial int64) (received, prepared, applied int64, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT received,prepared,applied FROM source_progress WHERE source=$1 AND namespace=$2`, source, ns).Scan(&received, &prepared, &applied)
	if errors.Is(err, pgx.ErrNoRows) {
		return initial - 1, initial - 1, initial - 1, nil
	}
	return
}
func (s *Store) Append(ctx context.Context, i Input, initial int64, subject string) error {
	ctx, span := platform.Start(ctx, "durable source capture")
	defer span.End()
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO source_progress(source,namespace,received,prepared,applied) VALUES($1,$2,$3,$3,$3) ON CONFLICT DO NOTHING`, i.Source, i.Namespace, initial-1); e != nil {
		return e
	}
	var pos int64
	if e = tx.QueryRow(ctx, `SELECT received FROM source_progress WHERE source=$1 AND namespace=$2 FOR UPDATE`, i.Source, i.Namespace).Scan(&pos); e != nil {
		return e
	}
	metadata, e := json.Marshal(i.Metadata)
	if e != nil {
		return e
	}
	digest := Hash(append(append([]byte{}, i.Payload...), metadata...))
	if i.Position <= pos {
		var old string
		if e = tx.QueryRow(ctx, `SELECT digest FROM raw_inputs WHERE source=$1 AND namespace=$2 AND position=$3`, i.Source, i.Namespace, i.Position).Scan(&old); e != nil {
			return e
		}
		if old != digest {
			return ErrConflict
		}
		return tx.Commit(ctx)
	}
	if i.Position != pos+1 {
		return ErrGap
	}
	if i.ID == "" {
		i.ID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(i.Source+"\x00"+i.Namespace+"\x00"+stringPosition(i.Position))).String()
	}
	// Operational trace context must survive a crash/outbox retry, but must not
	// change the source identity/content digest on a new acquisition attempt.
	metadata, e = json.Marshal(traceMetadata(ctx, i.Metadata))
	if e != nil {
		return e
	}
	if e = tx.QueryRow(ctx, `INSERT INTO raw_inputs(message_id,source,namespace,position,entity_key,payload,metadata,digest,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING received_at`, i.ID, i.Source, i.Namespace, i.Position, i.Entity, i.Payload, metadata, digest, i.CorrelationID).Scan(&i.ReceivedAt); e != nil {
		return e
	}
	if critical, _ := i.Metadata["critical"].(bool); critical {
		if _, e = tx.Exec(ctx, `INSERT INTO warning_expectations(notification_id,message_id,first_received_at) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, WarningID(i), i.ID, i.ReceivedAt); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(ctx, `INSERT INTO raw_outbox(message_id,subject) VALUES($1,$2)`, i.ID, subject); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE source_progress SET received=$3 WHERE source=$1 AND namespace=$2`, i.Source, i.Namespace, i.Position); e != nil {
		return e
	}
	if i.Source == "SATELIT" {
		if e = resolveResponse(ctx, tx, i.Source, i.Namespace, "stream:"+stringPosition(i.Position)); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
func stringPosition(pos int64) string { return fmt.Sprintf("%d", pos) }
