package store

import (
	"aggregator/internal/domain"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/http"
)

// Candidate selection is unlocked; the second query routes by the exact shard
// key and locks one row. This avoids unsupported global distributed row locks.
func (s *Store) DeliverNext(ctx context.Context, publish func(context.Context, OutboxEvent) error) (bool, error) {
	var route, id string
	err := s.Pool.QueryRow(ctx, "SELECT route_key,hazard_id::text FROM hazard_outbox ORDER BY queued_at,hazard_id LIMIT 1").Scan(&route, &id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var payload []byte
	var cid, tp, ts string
	var attempt *string
	err = tx.QueryRow(ctx, "SELECT event_payload,correlation_id,traceparent,tracestate,attempt_id::text FROM hazard_outbox WHERE route_key=$1 AND hazard_id=$2 FOR UPDATE SKIP LOCKED", route, id).Scan(&payload, &cid, &tp, &ts, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = s.publicationReady(ctx, attempt); err != nil {
		return false, err
	}
	ctx = middleware.WithCorrelationID(platform.Extract(ctx, http.Header{"Traceparent": []string{tp}, "Tracestate": []string{ts}}), cid)
	var h domain.HazardEvent
	if err = json.Unmarshal(payload, &h); err != nil {
		return true, err
	}
	if err = publish(ctx, OutboxEvent{Hazard: h, CorrelationID: cid}); err != nil {
		return true, err
	}
	if _, err = tx.Exec(ctx, "DELETE FROM hazard_outbox WHERE route_key=$1 AND hazard_id=$2", route, id); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
func (s *Store) DeliverLifecycle(ctx context.Context, publish func(context.Context, domain.LifecycleEvent) error) (bool, error) {
	var route, id string
	err := s.Pool.QueryRow(ctx, "SELECT route_key,event_id::text FROM canonical_lifecycle_outbox ORDER BY queued_at,event_id LIMIT 1").Scan(&route, &id)
	entity := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		err = s.Pool.QueryRow(ctx, "SELECT event_id::text FROM report_lifecycle_outbox ORDER BY queued_at,event_id LIMIT 1").Scan(&id)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var raw []byte
	var tp, ts string
	var attempt *string
	if entity {
		err = tx.QueryRow(ctx, `SELECT e.event_payload,o.traceparent,o.tracestate,o.attempt_id::text FROM canonical_lifecycle_outbox o JOIN canonical_lifecycle_events e USING(route_key,event_id) WHERE o.route_key=$1 AND o.event_id=$2 FOR UPDATE OF o SKIP LOCKED`, route, id).Scan(&raw, &tp, &ts, &attempt)
	} else {
		err = tx.QueryRow(ctx, `SELECT e.event_payload,o.traceparent,o.tracestate,o.attempt_id::text FROM report_lifecycle_outbox o JOIN report_lifecycle_events e USING(event_id) WHERE o.event_id=$1 FOR UPDATE OF o SKIP LOCKED`, id).Scan(&raw, &tp, &ts, &attempt)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = s.publicationReady(ctx, attempt); err != nil {
		return false, err
	}
	var e domain.LifecycleEvent
	if err = json.Unmarshal(raw, &e); err != nil {
		return true, err
	}
	ctx = middleware.WithCorrelationID(platform.Extract(ctx, http.Header{"Traceparent": []string{tp}, "Tracestate": []string{ts}}), e.CorrelationID)
	if err = publish(ctx, e); err != nil {
		return true, err
	}
	if entity {
		_, err = tx.Exec(ctx, "DELETE FROM canonical_lifecycle_outbox WHERE route_key=$1 AND event_id=$2", route, id)
	} else {
		_, err = tx.Exec(ctx, "DELETE FROM report_lifecycle_outbox WHERE event_id=$1", id)
	}
	if err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
