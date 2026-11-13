// Package store is the Aggregator's exclusive repository abstraction over
// the Canonical Store (PostgreSQL 16 via pgx v5's native interface). No
// other package should touch *pgxpool.Pool directly for hazard data.
package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"aggregator/internal/domain"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"aggregator/internal/timeutil"
)

type Store struct {
	Pool                   *pgxpool.Pool
	logger                 *slog.Logger
	control                *pgxpool.Pool
	Canonical              CanonicalOptions
	Acquisition            Fence
	idleTransactionTimeout time.Duration
}

func New(ctx context.Context, databaseURL string, logger *slog.Logger) (*Store, error) {
	if logger == nil {
		logger = slog.Default()
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("invalid DATABASE_URL")
	}
	timeouts, err := loadSessionTimeouts()
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(platform.Int("DB_MAX_OPEN_CONNS", 16))
	cfg.MinConns = 0
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(platform.Duration("DB_STATEMENT_TIMEOUT", 5*time.Second).Milliseconds(), 10)
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = strconv.FormatInt(platform.Duration("DB_LOCK_TIMEOUT", 2*time.Second).Milliseconds(), 10)
	cfg.ConnConfig.Tracer = &sqlTracer{logger: logger}
	timeouts.configureData(cfg)
	pool, err := pgxpool.NewWithConfig(middleware.EnsureCorrelationID(ctx), cfg)
	if err != nil {
		return nil, err
	}
	controlConfig := cfg.Copy()
	controlConfig.MaxConns = 4
	timeouts.configureControl(controlConfig)
	beforeControlAcquire := controlConfig.BeforeAcquire
	controlConfig.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		if beforeControlAcquire != nil && !beforeControlAcquire(ctx, conn) {
			return false
		}
		// A process resuming after a freeze can have server-expired connections
		// still cached locally. Discard them before handing a session to a caller.
		return conn.Ping(ctx) == nil
	}
	control, err := pgxpool.NewWithConfig(ctx, controlConfig)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool, control: control, logger: logger, idleTransactionTimeout: timeouts.transaction}, nil
}

func (s *Store) Close() {
	if adapter, ok := s.Canonical.Durability.(interface{ Close() }); ok {
		adapter.Close()
	}
	s.Pool.Close()
	s.control.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	ctx = middleware.EnsureCorrelationID(ctx)
	started := time.Now()
	err := s.Pool.Ping(ctx)
	logSQL(s.logger, ctx, "ping", "", started, err)
	return err
}

// CheckSchema validates the canonical tables without changing the schema.
func (s *Store) CheckSchema(ctx context.Context) error {
	ctx = middleware.EnsureCorrelationID(ctx)
	for _, query := range []string{
		`SELECT route_key, hazard_id, source, source_ref_id, attributes, active FROM hazard_events LIMIT 0`,
		`SELECT generation FROM canonical_visibility LIMIT 0`,
		`SELECT attempt_id,state FROM canonical_attempts LIMIT 0`,
		`SELECT volcano_id, area_name, latitude, longitude FROM volcano_reference LIMIT 0`,
		`SELECT hazard_id, event_payload, correlation_id, queued_at FROM hazard_outbox LIMIT 0`,
	} {
		if _, err := s.Pool.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

// SaveDedup upserts a HazardEvent, deduplicating on (source, source_ref_id).
// A genuinely new source record inserts a fresh row and outbox event and
// returns inserted=true. A record seen on an earlier poll cycle updates its
// mutable fields in place but keeps its original hazard_id and returns
// inserted=false. Material changes queue a lifecycle correction; identical
// observations only refresh last-seen metadata. Canonical initial snapshots
// remain revision 1 in the original outbox.
func (s *Store) SaveDedup(ctx context.Context, e domain.HazardEvent, correlationID string) (inserted bool, err error) {
	ctx = middleware.WithCorrelationID(ctx, correlationID)
	tx, err := s.beginMutation(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if err = s.protectAcquisition(ctx, tx, string(e.Source)); err != nil {
		return false, err
	}
	_, inserted, err = s.saveHazardTx(ctx, tx, e, correlationID, true)
	if err != nil {
		return false, err
	}
	return inserted, tx.Commit(ctx)
}

type OutboxEvent struct {
	Hazard        domain.HazardEvent
	CorrelationID string
}

// DeliverNext holds a row lock until publish is acknowledged. A failed publish
// or interrupted transaction leaves the row pending for the next attempt.

func scanRow(row pgx.Row) (domain.HazardEvent, error) {
	return scanHazard(row, nil)
}

func scanHazard(row pgx.Row, metadata *domain.RecordMetadata) (domain.HazardEvent, error) {
	var e domain.HazardEvent
	var source, hazardType, severity string
	var occurredAt, ingestedAt time.Time
	var attrsJSON []byte

	dest := []any{&e.HazardID, &source, &e.SourceRefID, &hazardType, &severity,
		&e.AreaName, &e.Latitude, &e.Longitude, &occurredAt, &ingestedAt, &attrsJSON}
	if metadata != nil {
		dest = append(dest, &metadata.Revision, &metadata.LastSeenAt)
	}
	err := row.Scan(dest...)
	if err != nil {
		return e, err
	}
	e.Source = domain.Source(source)
	e.HazardType = domain.HazardType(hazardType)
	e.Severity = domain.Severity(severity)
	e.OccurredAt = timeutil.New(occurredAt)
	e.IngestedAt = timeutil.New(ingestedAt)
	if metadata != nil {
		metadata.ObservationTime = occurredAt
	}
	e.Attributes = map[string]interface{}{}
	if len(attrsJSON) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(attrsJSON))
		decoder.UseNumber()
		if err := decoder.Decode(&e.Attributes); err != nil {
			return e, err
		}
	}
	return e, nil
}

const selectColumns = `hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes`
const selectColumnsWithMetadata = selectColumns + `, revision, last_seen_at`

var ErrNotFound = errors.New("hazard not found")

func (s *Store) GetByID(ctx context.Context, hazardID string) (domain.HazardEvent, error) {
	event, _, err := s.GetByIDWithMetadata(ctx, hazardID)
	return event, err
}

// GetByIDWithMetadata reads the canonical record and its revision metadata from the same row.
func (s *Store) getByIDWithMetadata(ctx context.Context, hazardID string) (domain.HazardEvent, domain.RecordMetadata, error) {
	ctx = middleware.EnsureCorrelationID(ctx)
	row := s.Pool.QueryRow(ctx, `SELECT `+selectColumnsWithMetadata+` FROM hazard_events WHERE hazard_id = $1 AND active`, hazardID)
	var metadata domain.RecordMetadata
	e, err := scanHazard(row, &metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.HazardEvent{}, domain.RecordMetadata{}, ErrNotFound
	}
	return e, metadata, err
}

type Filters struct {
	BeforeTime *time.Time
	BeforeID   string
	Limit      int
	Since      time.Time
	HazardType string
	Severity   string
	AreaName   string
}

// Query performs all filtering in SQL, not in memory.
func (s *Store) Query(ctx context.Context, f Filters) ([]domain.HazardEvent, error) {
	events, _, err := s.QueryWithMetadata(ctx, f)
	return events, err
}

// QueryWithMetadata preserves the list filters and ordering while reading metadata in the same query.
func (s *Store) queryWithMetadata(ctx context.Context, f Filters) ([]domain.HazardEvent, map[string]domain.RecordMetadata, error) {
	ctx = middleware.EnsureCorrelationID(ctx)
	sql := `SELECT ` + selectColumnsWithMetadata + ` FROM hazard_events WHERE active`
	args := []interface{}{}
	n := 0
	next := func() string { n++; return "$" + strconv.Itoa(n) }

	if !f.Since.IsZero() {
		sql += ` AND occurred_at > ` + next()
		args = append(args, f.Since)
	}
	if f.HazardType != "" {
		sql += ` AND hazard_type = ` + next()
		args = append(args, f.HazardType)
	}
	if f.Severity != "" {
		sql += ` AND severity = ` + next()
		args = append(args, f.Severity)
	}
	if f.AreaName != "" {
		sql += ` AND area_name ILIKE ` + next()
		args = append(args, "%"+f.AreaName+"%")
	}
	if f.BeforeTime != nil {
		sql += ` AND (occurred_at,hazard_id) < (` + next() + `,` + next() + `::uuid)`
		args = append(args, *f.BeforeTime, f.BeforeID)
	}
	sql += ` ORDER BY occurred_at DESC, hazard_id DESC`
	if f.Limit > 0 {
		sql += ` LIMIT ` + next()
		args = append(args, f.Limit)
	}

	rows, err := s.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	out := []domain.HazardEvent{}
	metadata := map[string]domain.RecordMetadata{}
	for rows.Next() {
		var m domain.RecordMetadata
		e, err := scanHazard(rows, &m)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, e)
		metadata[e.HazardID] = m
	}
	return out, metadata, rows.Err()
}

// LastKnownVolcanic returns the most recently stored VOLCANIC hazard events,
// used as the outage fallback when PVMBG is unreachable.
func (s *Store) LastKnownVolcanic(ctx context.Context) ([]domain.HazardEvent, error) {
	return s.Query(ctx, Filters{HazardType: string(domain.HazardVolcanic)})
}

func (s *Store) GetByIDWithMetadata(ctx context.Context, id string) (e domain.HazardEvent, m domain.RecordMetadata, err error) {
	err = s.validatedRead(ctx, func(ctx context.Context) error { var e2 error; e, m, e2 = s.getByIDWithMetadata(ctx, id); return e2 })
	return
}
func (s *Store) QueryWithMetadata(ctx context.Context, f Filters) (events []domain.HazardEvent, metadata map[string]domain.RecordMetadata, err error) {
	err = s.validatedRead(ctx, func(ctx context.Context) error {
		var e error
		events, metadata, e = s.queryWithMetadata(ctx, f)
		return e
	})
	return
}
