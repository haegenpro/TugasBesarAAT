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
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strconv"
	"sync/atomic"
	"time"
)

//go:embed schema.sql
var Schema string
var ErrConflict = errors.New("identity reused with different content")
var ErrGap = errors.New("noncontiguous source position")

type Store struct {
	Pool   *pgxpool.Pool
	Leases *ownership.Authority
	// noSequence is set once the owner answers 404 to the multi-operation call (an older owner
	// during a rolling update); delivery then sends operations one at a time.
	noSequence atomic.Bool
}

func New(ctx context.Context, url string) (*Store, error) {
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, errors.New("invalid ingestion database URL")
	}
	cfg.MaxConns = int32(platform.Int("DB_MAX_OPEN_CONNS", 8))
	cfg.ConnConfig.Tracer = platform.SQLTracer{}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(platform.Duration("DB_STATEMENT_TIMEOUT", 5*time.Second).Milliseconds(), 10)
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = strconv.FormatInt(platform.Duration("DB_LOCK_TIMEOUT", 2*time.Second).Milliseconds(), 10)
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = strconv.FormatInt(platform.Duration("DB_IDLE_TRANSACTION_TIMEOUT", 10*time.Second).Milliseconds(), 10)
	p, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	return &Store{Pool: p, Leases: ownership.New(p)}, nil
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
func stringPosition(pos int64) string { return fmt.Sprintf("%d", pos) }
