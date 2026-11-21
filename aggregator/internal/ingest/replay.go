package ingest

import (
	"aggregator/internal/ownership"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"strconv"
	"time"
)

type failureCursor struct {
	Source string
	At     time.Time
	ID     string
}

func replayGeneration(i Input) int64 {
	value, _ := strconv.ParseInt(fmt.Sprint(i.Metadata["_replay_generation"]), 10, 64)
	return value
}
func (s *Store) Failures(ctx context.Context, source string, limit int, cursor string) (map[string]any, error) {
	var p failureCursor
	if cursor != "" {
		b, e := base64.RawURLEncoding.DecodeString(cursor)
		if e != nil || len(cursor) > 2048 || json.Unmarshal(b, &p) != nil || p.Source != source || p.At.IsZero() {
			return nil, ErrConflict
		}
		if _, e = uuid.Parse(p.ID); e != nil {
			return nil, ErrConflict
		}
	}
	var at any
	id := "00000000-0000-0000-0000-000000000000"
	if !p.At.IsZero() {
		at = p.At
		id = p.ID
	}
	rows, e := s.Pool.Query(ctx, `SELECT message_id::text,source,received_at,CASE WHEN reason<>'' THEN reason ELSE 'awaiting durable processing or canonical commit' END,attempts,state FROM raw_inputs WHERE source<>'FIELD_REPORT' AND state<>'applied' AND ($1='' OR source=$1) AND ($2::timestamptz IS NULL OR (received_at,message_id)>($2,$3::uuid)) ORDER BY received_at,message_id LIMIT $4`, source, at, id, limit+1)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, src, reason, state string
		var at time.Time
		var attempts int64
		if e = rows.Scan(&id, &src, &at, &reason, &attempts, &state); e != nil {
			return nil, e
		}
		status := "retrying"
		if state == "parked" {
			status = "parked"
		}
		items = append(items, map[string]any{"message_id": id, "source": src, "received_at": at, "reason": reason, "retryable": state != "parked", "attempts": attempts, "status": status})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	var next any
	if len(items) > limit {
		items = items[:limit]
		last := items[limit-1]
		b, _ := json.Marshal(failureCursor{source, last["received_at"].(time.Time), last["message_id"].(string)})
		next = base64.RawURLEncoding.EncodeToString(b)
	}
	return map[string]any{"items": items, "next_cursor": next}, nil
}
func (s *Store) RequestReplay(ctx context.Context, ids []string) (int64, error) {
	if len(ids) < 1 || len(ids) > 1000 {
		return 0, ErrConflict
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback(ctx)
	var count int64
	if e = tx.QueryRow(ctx, `SELECT count(*) FROM raw_inputs WHERE state='parked' AND message_id::text=ANY($1)`, ids).Scan(&count); e != nil {
		return 0, e
	}
	if count != int64(len(ids)) {
		return 0, ErrConflict
	}
	for _, id := range ids {
		if e = enqueueReplay(ctx, tx, id); e != nil {
			return 0, e
		}
	}
	return count, tx.Commit(ctx)
}
func enqueueReplay(ctx context.Context, tx pgx.Tx, id string) error {
	var source string
	var critical bool
	e := tx.QueryRow(ctx, `UPDATE raw_inputs SET state='pending',metadata=jsonb_set(metadata,'{_replay_generation}',to_jsonb(coalesce((metadata->>'_replay_generation')::bigint,0)+1)) WHERE message_id=$1 RETURNING source,coalesce((metadata->>'critical')::boolean,false)`, id).Scan(&source, &critical)
	if e != nil {
		return e
	}
	subject := "raw.satellite"
	if source == "SENSOR_PANTAI" {
		subject = "raw.coastal.normal"
		if critical {
			subject = "raw.coastal.critical"
		}
	}
	_, e = tx.Exec(ctx, `INSERT INTO raw_outbox(message_id,subject) VALUES($1,$2) ON CONFLICT DO NOTHING`, id, subject)
	return e
}
func blocked(ctx context.Context, tx pgx.Tx, i Input) (bool, error) {
	var id string
	e := tx.QueryRow(ctx, `SELECT message_id::text FROM blocked_entities WHERE source=$1 AND namespace=$2 AND entity_key=$3`, i.Source, i.Namespace, i.Entity).Scan(&id)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return id != i.ID, nil
}
func releaseEntity(ctx context.Context, tx pgx.Tx, i Input) error {
	result, e := tx.Exec(ctx, `DELETE FROM blocked_entities WHERE source=$1 AND namespace=$2 AND entity_key=$3 AND message_id=$4`, i.Source, i.Namespace, i.Entity, i.ID)
	if e != nil || result.RowsAffected() == 0 {
		return e
	}
	var next string
	e = tx.QueryRow(ctx, `SELECT message_id::text FROM raw_inputs WHERE source=$1 AND namespace=$2 AND entity_key=$3 AND state='parked' ORDER BY position LIMIT 1`, i.Source, i.Namespace, i.Entity).Scan(&next)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO blocked_entities(source,namespace,entity_key,message_id) VALUES($1,$2,$3,$4)`, i.Source, i.Namespace, i.Entity, next); e != nil {
		return e
	}
	return enqueueReplay(ctx, tx, next)
}
func (s *Store) NoteAttempt(ctx context.Context, id string) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE raw_inputs SET attempts=attempts+1 WHERE message_id=$1`, id); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
