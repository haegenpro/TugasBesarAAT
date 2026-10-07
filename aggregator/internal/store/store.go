// Package store is the Aggregator's exclusive repository abstraction over
// the Canonical Store (PostgreSQL 16 via pgx v5's native interface). No
// other package should touch *pgxpool.Pool directly for hazard data.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"aggregator/internal/domain"
	"aggregator/internal/timeutil"
)

type Store struct {
	Pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() {
	s.Pool.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.Pool.Ping(ctx)
}

// CheckSchema applies idempotent lifecycle migrations before validating the canonical tables.
func (s *Store) CheckSchema(ctx context.Context) error {
	if err := s.Migrate(ctx); err != nil {
		return err
	}
	for _, query := range []string{
		`SELECT hazard_id, source, source_ref_id, attributes FROM hazard_events LIMIT 0`,
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
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
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
func (s *Store) DeliverNext(ctx context.Context, publish func(context.Context, OutboxEvent) error) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var hazardID string
	var payload []byte
	var correlationID string
	err = tx.QueryRow(ctx, `
		SELECT hazard_id::text, event_payload, correlation_id
		FROM hazard_outbox
		ORDER BY queued_at, hazard_id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&hazardID, &payload, &correlationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	var hazard domain.HazardEvent
	if err := json.Unmarshal(payload, &hazard); err != nil {
		return true, err
	}
	if err := publish(ctx, OutboxEvent{Hazard: hazard, CorrelationID: correlationID}); err != nil {
		return true, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM hazard_outbox WHERE hazard_id = $1`, hazardID); err != nil {
		return true, err
	}
	if err := tx.Commit(ctx); err != nil {
		return true, err
	}
	return true, nil
}

func scanRow(row pgx.Row) (domain.HazardEvent, error) {
	var e domain.HazardEvent
	var source, hazardType, severity string
	var occurredAt, ingestedAt time.Time
	var attrsJSON []byte

	err := row.Scan(&e.HazardID, &source, &e.SourceRefID, &hazardType, &severity,
		&e.AreaName, &e.Latitude, &e.Longitude, &occurredAt, &ingestedAt, &attrsJSON)
	if err != nil {
		return e, err
	}
	e.Source = domain.Source(source)
	e.HazardType = domain.HazardType(hazardType)
	e.Severity = domain.Severity(severity)
	e.OccurredAt = timeutil.New(occurredAt)
	e.IngestedAt = timeutil.New(ingestedAt)
	e.Attributes = map[string]interface{}{}
	if len(attrsJSON) > 0 {
		if err := json.Unmarshal(attrsJSON, &e.Attributes); err != nil {
			return e, err
		}
	}
	return e, nil
}

const selectColumns = `hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude, occurred_at, ingested_at, attributes`

var ErrNotFound = errors.New("hazard not found")

func (s *Store) GetByID(ctx context.Context, hazardID string) (domain.HazardEvent, error) {
	row := s.Pool.QueryRow(ctx, `SELECT `+selectColumns+` FROM hazard_events WHERE hazard_id = $1`, hazardID)
	e, err := scanRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.HazardEvent{}, ErrNotFound
	}
	return e, err
}

type Filters struct {
	Since      time.Time
	HazardType string
	Severity   string
	AreaName   string
}

// Query performs all filtering in SQL, not in memory.
func (s *Store) Query(ctx context.Context, f Filters) ([]domain.HazardEvent, error) {
	sql := `SELECT ` + selectColumns + ` FROM hazard_events WHERE 1=1`
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
	sql += ` ORDER BY occurred_at DESC`

	rows, err := s.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []domain.HazardEvent{}
	for rows.Next() {
		e, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// LastKnownVolcanic returns the most recently stored VOLCANIC hazard events,
// used as the outage fallback when PVMBG is unreachable.
func (s *Store) LastKnownVolcanic(ctx context.Context) ([]domain.HazardEvent, error) {
	return s.Query(ctx, Filters{HazardType: string(domain.HazardVolcanic)})
}
