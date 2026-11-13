package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"math"
	"strings"
)

type InputOptions struct {
	// Explicitly configured sessions only. New namespace is never inferred from
	// a caller or reconnect; the operator provisions its first offset.
	Starts       map[string]int64
	Resolver     rawinput.Resolver
	PayloadQuota int64
}

func InputSession(source, namespace string) string {
	return string(rawinput.Compact(source, namespace))
}

type recordDurability interface {
	ConfirmRecord(context.Context, string, ...any) error
}

func (s *Store) confirmRecord(ctx context.Context, q string, args ...any) error {
	d, ok := s.Canonical.Durability.(recordDurability)
	if !ok {
		return canonical.Unavailable()
	}
	return d.ConfirmRecord(ctx, q, args...)
}
func (s *Store) AppendHistory(ctx context.Context, req rawinput.AppendRequest) (rawinput.Receipt, error) {
	out := rawinput.Receipt{Items: []rawinput.ReceiptItem{}}
	if req.Version != 1 || len(req.Messages) == 0 || len(req.Messages) > 32 {
		return out, canonical.Fail(400, "invalid_request", "invalid history batch")
	}
	first := req.Messages[0]
	if err := rawinput.Validate(first); err != nil {
		return out, err
	}
	out.Namespace = first.Namespace
	ctx = middleware.WithCorrelationID(ctx, first.CorrelationID)
	if first.Traceparent != nil {
		ctx = platform.Extract(ctx, mapHeader(*first.Traceparent))
	}
	start, ok := s.Inputs.Starts[InputSession(first.Source, first.Namespace)]
	if !ok || start < 0 {
		return out, canonical.Fail(400, "invalid_request", "session start is not configured")
	}
	if req.Ownership.Scope != "processor:"+first.Source {
		return out, canonical.Fail(403, "stale_owner", "wrong processor authority")
	}
	payloads := make([][]byte, len(req.Messages))
	var total int64
	for i, e := range req.Messages {
		if e.Size < 0 || e.Size > math.MaxInt64-total {
			return out, canonical.Fail(400, "invalid_request", "invalid payload size")
		}
		total += e.Size
		if s.Inputs.PayloadQuota > 0 && total > s.Inputs.PayloadQuota {
			return out, canonical.Unavailable()
		}
		if e.Source != first.Source || e.Namespace != first.Namespace {
			return out, canonical.Fail(400, "invalid_request", "one source and namespace required")
		}
		if err := rawinput.Validate(e); err != nil {
			return out, err
		}
		if i > 0 && *e.Position <= *req.Messages[i-1].Position {
			return out, canonical.Fail(400, "invalid_request", "messages must be ordered")
		}
		b, err := rawinput.Resolve(ctx, e, s.Inputs.Resolver, s.Inputs.PayloadQuota)
		if err != nil {
			return out, err
		}
		payloads[i] = b
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return out, canonical.Unavailable()
	}
	defer tx.Rollback(context.Background())
	if err = s.ProtectProcessorState(ctx, tx, req.Ownership); err != nil {
		return out, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO input_sessions(source,namespace,initial_position,received_through,prepared_through,applied_through) VALUES($1,$2,$3,$4,$4,$4) ON CONFLICT DO NOTHING`, first.Source, first.Namespace, start, start-1)
	if err != nil {
		return out, err
	}
	var received, initial int64
	err = tx.QueryRow(ctx, `SELECT initial_position,received_through FROM input_sessions WHERE source=$1 AND namespace=$2 FOR UPDATE`, first.Source, first.Namespace).Scan(&initial, &received)
	if err != nil {
		return out, err
	}
	if initial != start {
		return out, canonical.Fail(409, "identity_conflict", "configured session start differs from persisted start")
	}
	for i, e := range req.Messages {
		route := rawinput.Route(e)
		fp := rawinput.Fingerprint(e)
		var prior string
		err = tx.QueryRow(ctx, `SELECT fingerprint FROM input_history WHERE route_key=$1 AND source=$2 AND namespace=$3 AND position=$4`, route, e.Source, e.Namespace, *e.Position).Scan(&prior)
		outcome := "stored"
		if err == nil {
			if prior != fp {
				return out, canonical.Fail(409, "identity_conflict", "offset reused with different input")
			}
			outcome = "duplicate"
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return out, err
		} else {
			if received == math.MaxInt64 || *e.Position != received+1 {
				p := canonical.Fail(409, "source_gap", "contiguous input required")
				p.Details = map[string]any{"expected_position": received + 1}
				return out, p
			}
			b, _ := json.Marshal(e)
			_, err = tx.Exec(ctx, `INSERT INTO input_history(route_key,source,namespace,position,message_id,fingerprint,envelope,original_payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, route, e.Source, e.Namespace, *e.Position, e.MessageID, fp, b, payloads[i])
			if err != nil {
				return out, err
			}
			received = *e.Position
		}
		out.Items = append(out.Items, rawinput.ReceiptItem{MessageID: e.MessageID, Outcome: outcome})
	}
	_, err = tx.Exec(ctx, `UPDATE input_sessions SET received_through=$3 WHERE source=$1 AND namespace=$2`, first.Source, first.Namespace, received)
	if err != nil {
		return out, err
	}
	if err = tx.Commit(ctx); err != nil {
		return out, canonical.Unavailable()
	}
	if err = s.confirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM input_sessions WHERE source=$1 AND namespace=$2 AND received_through>=$3)`, first.Source, first.Namespace, received); err != nil {
		return out, canonical.Unavailable()
	}
	out.Through = received
	platform.Gauge("raw_"+strings.ToLower(first.Source)+"_received_through", float64(received))
	return out, nil
}

func (s *Store) ReadHistory(ctx context.Context, namespace string, after *int64, limit int) (rawinput.HistoryPage, error) {
	out := rawinput.HistoryPage{Items: []rawinput.Envelope{}}
	if limit < 1 || limit > 1000 {
		return out, canonical.Fail(400, "invalid_request", "invalid limit")
	}
	start, ok := s.Inputs.Starts[InputSession("SATELIT", namespace)]
	if !ok {
		return out, canonical.Fail(400, "invalid_request", "unknown session")
	}
	pos := start - 1
	if after != nil {
		pos = *after
	}
	if err := s.confirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM input_sessions WHERE source='SATELIT' AND namespace=$1)`, namespace); err != nil {
		return out, canonical.Unavailable()
	}
	rows, err := s.Pool.Query(ctx, `SELECT envelope,original_payload FROM input_history WHERE source='SATELIT' AND namespace=$1 AND position>$2 ORDER BY position LIMIT $3`, namespace, pos, limit+1)
	if err != nil {
		return out, canonical.Unavailable()
	}
	defer rows.Close()
	total := 0
	for rows.Next() {
		var b, payload []byte
		var e rawinput.Envelope
		if err = rows.Scan(&b, &payload); err != nil {
			return out, err
		}
		if err = json.Unmarshal(b, &e); err != nil {
			return out, err
		}
		if err = rawinput.Validate(e); err != nil {
			return out, err
		}
		if rawinput.Hash(payload) != e.Digest || int64(len(payload)) != e.Size {
			return out, fmt.Errorf("history byte integrity failure")
		}
		// Bound retained response bytes independently of the row limit. A reference
		// envelope remains replayable through its pinned owner-store object.
		if len(out.Items) == limit || total+len(b) > 4<<20 {
			if len(out.Items) == 0 {
				return out, canonical.Unavailable()
			}
			last := *out.Items[len(out.Items)-1].Position
			out.NextAfter = &last
			break
		}
		out.Items = append(out.Items, e)
		total += len(b)
	}
	if err = rows.Err(); err != nil {
		return out, canonical.Unavailable()
	}
	if err = s.confirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM input_sessions WHERE source='SATELIT' AND namespace=$1)`, namespace); err != nil {
		return out, canonical.Unavailable()
	}
	return out, nil
}

// SchemaCache deliberately refuses an ID whose text or digest changes.
type SchemaCache struct {
	Pool interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}
}

func (c SchemaCache) Get(ctx context.Context, id int) (string, error) {
	var s, d string
	err := c.Pool.QueryRow(ctx, `SELECT schema_text,sha256 FROM input_schema_cache WHERE schema_id=$1`, id).Scan(&s, &d)
	if err != nil {
		return "", err
	}
	if rawinput.Hash([]byte(s)) != d {
		return "", fmt.Errorf("schema cache checksum mismatch")
	}
	return s, nil
}
func (c SchemaCache) Put(ctx context.Context, id int, s string) error {
	var stored string
	err := c.Pool.QueryRow(ctx, `INSERT INTO input_schema_cache(schema_id,schema_text,sha256) VALUES($1,$2,$3) ON CONFLICT(schema_id) DO UPDATE SET schema_id=excluded.schema_id RETURNING sha256`, id, s, rawinput.Hash([]byte(s))).Scan(&stored)
	if err != nil {
		return err
	}
	if stored != rawinput.Hash([]byte(s)) {
		return fmt.Errorf("schema ID conflict")
	}
	return nil
}
func (c SchemaCache) Find(ctx context.Context, text string) (int, error) {
	var id int
	err := c.Pool.QueryRow(ctx, `SELECT schema_id FROM input_schema_cache WHERE sha256=$1 AND schema_text=$2 ORDER BY schema_id LIMIT 1`, rawinput.Hash([]byte(text)), text).Scan(&id)
	return id, err
}
