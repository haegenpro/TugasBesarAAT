package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go/jetstream"
	"time"
)

// Events owns the atomic projection/inbox boundary. ACK decisions stay with the worker.
type Events interface {
	readRecords(context.Context, string) ([]map[string]any, error)
	saveLifecycle(context.Context, *jetstream.MsgMetadata, []byte, lifecycleEvent) error
	save(context.Context, *jetstream.MsgMetadata, []byte, envelope) error
	alreadyProcessed(context.Context, *jetstream.MsgMetadata, []byte) (bool, error)
	recordUnknown(context.Context, *jetstream.MsgMetadata, int, time.Time) (retryState, error)
	recordConstraint(context.Context, *jetstream.MsgMetadata, time.Time) (retryState, error)
	recordCorrupt(context.Context, *jetstream.MsgMetadata, time.Time) (int, error)
	clearRetries(context.Context, *jetstream.MsgMetadata) error
}
type projectionStore struct {
	db   *sql.DB
	role string
}

func (a *app) repository() Events {
	if a.events != nil {
		return a.events
	}
	return &projectionStore{db: a.db, role: a.cfg.Role}
}
func (a *app) save(c context.Context, m *jetstream.MsgMetadata, b []byte, e envelope) error {
	return a.repository().save(c, m, b, e)
}
func (a *app) alreadyProcessed(c context.Context, m *jetstream.MsgMetadata, b []byte) (bool, error) {
	return a.repository().alreadyProcessed(c, m, b)
}
func (a *app) recordUnknown(c context.Context, m *jetstream.MsgMetadata, id int, t time.Time) (retryState, error) {
	return a.repository().recordUnknown(c, m, id, t)
}
func (a *app) recordConstraint(c context.Context, m *jetstream.MsgMetadata, t time.Time) (retryState, error) {
	return a.repository().recordConstraint(c, m, t)
}
func (a *app) recordCorrupt(c context.Context, m *jetstream.MsgMetadata, t time.Time) (int, error) {
	return a.repository().recordCorrupt(c, m, t)
}
func (a *app) clearRetries(c context.Context, m *jetstream.MsgMetadata) error {
	return a.repository().clearRetries(c, m)
}

func (a *app) saveLifecycle(c context.Context, m *jetstream.MsgMetadata, b []byte, e lifecycleEvent) error {
	return a.repository().saveLifecycle(c, m, b, e)
}

var errInvalidView = errors.New("invalid view")

func (s *projectionStore) readRecords(ctx context.Context, view string) ([]map[string]any, error) {
	queries := map[string]string{
		"":              `SELECT hazard_id,payload,correlation_id,processed_at FROM records ORDER BY processed_at DESC,hazard_id DESC LIMIT 100`,
		"notices":       `SELECT notice_id,payload,processed_at FROM notice_records ORDER BY processed_at DESC,notice_id DESC LIMIT 100`,
		"history":       `SELECT event_id,payload,processed_at FROM lifecycle_events ORDER BY processed_at DESC,event_id DESC LIMIT 100`,
		"notifications": `SELECT event_id,payload,processed_at FROM notifications ORDER BY processed_at DESC,event_id DESC LIMIT 100`,
	}
	query, ok := queries[view]
	if !ok {
		return nil, errInvalidView
	}
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, payload, cid, at string
		if view == "" {
			err = rows.Scan(&id, &payload, &cid, &at)
		} else {
			err = rows.Scan(&id, &payload, &at)
		}
		if err != nil {
			return nil, err
		}
		item := map[string]any{"payload": json.RawMessage(payload), "processed_at": at}
		if view == "" {
			item["hazard_id"] = id
			item["correlation_id"] = cid
		} else {
			item["id"] = id
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
