package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

func ValidateReport(body []byte) error {
	if len(body) > 1<<20 || !utf8.Valid(body) {
		return errors.New("report must be UTF-8 JSON below 1 MiB")
	}
	var r struct {
		Type        *string        `json:"report_type"`
		At          *time.Time     `json:"occurred_at"`
		Lat         *float64       `json:"latitude"`
		Lon         *float64       `json:"longitude"`
		Description *string        `json:"description"`
		Attributes  map[string]any `json:"attributes"`
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&r) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid report JSON fields")
	}
	if r.Type == nil || strings.TrimSpace(*r.Type) == "" || r.Description == nil || strings.TrimSpace(*r.Description) == "" || r.At == nil || r.At.IsZero() || r.Lat == nil || r.Lon == nil || math.Abs(*r.Lat) > 90 || math.Abs(*r.Lon) > 180 {
		return errors.New("report_type, occurred_at, coordinates and description required")
	}
	return nil
}
func ValidKey(key string) bool {
	if len(key) < 1 || len(key) > 128 {
		return false
	}
	for _, b := range []byte(key) {
		if b < 33 || b > 126 {
			return false
		}
	}
	return true
}
func (s *Store) AcceptReport(ctx context.Context, submitter, key string, body []byte, cid string) (map[string]any, error) {
	tx, e := s.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	// The same client/key always locks the same ledger row, including concurrent
	// requests whose first transaction has not committed yet.
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,5))`, fmt.Sprintf("%d:%s%s", len(submitter), submitter, key)); e != nil {
		return nil, e
	}
	var id, digest, status string
	var at time.Time
	e = tx.QueryRow(ctx, `SELECT receipt_id::text,digest,status,received_at FROM field_reports WHERE submitter=$1 AND idempotency_key=$2`, submitter, key).Scan(&id, &digest, &status, &at)
	if e == nil {
		if digest != Hash(body) {
			return nil, ErrConflict
		}
		return receipt(id, status, at), commit(ctx, tx, "field")
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return nil, e
	}
	id = uuid.NewString()
	if e = tx.QueryRow(ctx, `INSERT INTO field_reports(receipt_id,submitter,idempotency_key,digest,payload) VALUES($1,$2,$3,$4,$5) RETURNING received_at`, id, submitter, key, Hash(body), body).Scan(&at); e != nil {
		return nil, e
	}
	metadata, _ := json.Marshal(traceMetadata(ctx, map[string]any{"submitter": submitter, "receipt_id": id}))
	// Receipt identity is also the raw message identity, so retries cannot create another
	// obligation. The position only has to be unique: it comes from a sequence, which takes no
	// row lock, so concurrent reports of one submitter no longer queue behind each other.
	var position int64
	if e = tx.QueryRow(ctx, `SELECT nextval('field_report_position')`).Scan(&position); e != nil {
		return nil, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO raw_inputs(message_id,source,namespace,position,entity_key,payload,metadata,digest,correlation_id,received_at) VALUES($1::uuid,'FIELD_REPORT',$2,$3,$1::text,$4,$5,$6,$7,$8)`, id, submitter, position, body, metadata, Hash(body), cid, at); e != nil {
		return nil, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO raw_outbox(message_id,subject) VALUES($1,'raw.field')`, id); e != nil {
		return nil, e
	}
	return receipt(id, "accepted", at), commit(ctx, tx, "field")
}
func receipt(id, status string, at time.Time) map[string]any {
	return map[string]any{"receipt_id": id, "report_id": id, "status": status, "received_at": at}
}
func (s *Store) Report(ctx context.Context, id, submitter, role string) (map[string]any, error) {
	var status string
	var at time.Time
	e := s.Pool.QueryRow(ctx, `SELECT status,received_at FROM field_reports WHERE receipt_id=$1 AND (submitter=$2 OR $3='internal')`, id, submitter, role).Scan(&status, &at)
	if e != nil {
		return nil, e
	}
	return receipt(id, status, at), nil
}
func (s *Store) ProcessReport(ctx context.Context, i Input) error {
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	body := map[string]any{"report_id": i.ID, "disposition": "awaiting_operator_review"}
	result, _ := json.Marshal(body)
	if _, e = tx.Exec(ctx, `INSERT INTO field_results(receipt_id,result) VALUES($1,$2) ON CONFLICT DO NOTHING`, i.ID, result); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE field_reports SET status='processed' WHERE receipt_id=$1`, i.ID); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE raw_inputs SET state='applied' WHERE message_id=$1`, i.ID); e != nil {
		return e
	}
	return commit(ctx, tx, "field")
}
