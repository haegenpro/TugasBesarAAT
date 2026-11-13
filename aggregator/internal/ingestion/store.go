package ingestion

import (
	"aggregator/internal/canonical"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"sync/atomic"
	"time"
)

type Durability interface {
	Prepare(context.Context, pgx.Tx) error
	ConfirmRecord(context.Context, string, ...any) error
}
type Fence interface {
	Protect(context.Context, pgx.Tx, canonical.Ownership, []canonical.Mutation) error
}
type Store struct {
	DB           *store.Store
	Durability   Durability
	Fence        Fence
	Namespace    string
	Start, Quota int64
}
type Capture struct {
	Position  int64
	PayloadID string
	Size      int64
	Digest    string
	Context   CaptureContext
	Envelope  *rawinput.Envelope
	State     string
}
type CaptureContext struct {
	MediaType            string                 `json:"content_type"`
	SourceSequenceHeader string                 `json:"source_sequence_header"`
	Cursor               rawinput.CaptureCursor `json:"cursor"`
	ReceivedAt           string                 `json:"received_at"`
	CorrelationID        string                 `json:"correlation_id"`
	Traceparent          *string                `json:"traceparent"`
	Owner                canonical.Ownership    `json:"owner"`
	HTTPStatus           int                    `json:"http_status"`
}

func (s *Store) Check(ctx context.Context) error {
	if s.Durability == nil || s.Fence == nil || s.Namespace == "" || s.Start < 0 || s.Quota <= 0 {
		return canonical.Unavailable()
	}
	_, err := s.DB.Pool.Exec(ctx, `SELECT next_position FROM ingestion_state LIMIT 0`)
	return err
}
func (s *Store) begin(ctx context.Context, owner canonical.Ownership) (pgx.Tx, error) {
	if err := s.Check(ctx); err != nil {
		return nil, err
	}
	tx, err := s.DB.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err = s.Durability.Prepare(ctx, tx); err == nil {
		err = s.Fence.Protect(ctx, tx, owner, []canonical.Mutation{{Source: "BMKG"}})
	}
	if err != nil {
		_ = tx.Rollback(context.Background())
		return nil, err
	}
	return tx, nil
}
func (s *Store) Next(ctx context.Context) (*Capture, error) {
	var c Capture
	var meta, env []byte
	err := s.DB.Pool.QueryRow(ctx, `SELECT position,payload_id,size,sha256,context,envelope,state FROM ingestion_captures WHERE namespace=$1 AND state<>'published' ORDER BY position LIMIT 1`, s.Namespace).Scan(&c.Position, &c.PayloadID, &c.Size, &c.Digest, &meta, &env, &c.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(meta, &c.Context); err != nil {
		return nil, err
	}
	if len(env) > 0 {
		c.Envelope = new(rawinput.Envelope)
		if err = json.Unmarshal(env, c.Envelope); err != nil {
			return nil, err
		}
	}
	return &c, nil
}

// Capture streams a complete HTTP body into immutable 64 KiB chunks before any
// schema decode. A failed/over-quota stream rolls back and cannot advance input.
func (s *Store) Capture(ctx context.Context, owner canonical.Ownership, meta CaptureContext, r io.Reader) (*Capture, error) {
	tx, err := s.begin(ctx, owner)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, `INSERT INTO ingestion_state(namespace,initial_position,next_position,published_through,quota_bytes) VALUES($1,$2,$2,$3,$4) ON CONFLICT DO NOTHING`, s.Namespace, s.Start, s.Start-1, s.Quota)
	if err != nil {
		return nil, err
	}
	var pos, initial, quota, used int64
	err = tx.QueryRow(ctx, `SELECT next_position,initial_position,quota_bytes,used_bytes FROM ingestion_state WHERE namespace=$1 FOR UPDATE`, s.Namespace).Scan(&pos, &initial, &quota, &used)
	if err != nil {
		return nil, err
	}
	if initial != s.Start || quota != s.Quota {
		return nil, fmt.Errorf("ingestion session configuration changed")
	}
	var pending bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_captures WHERE namespace=$1 AND state<>'published')`, s.Namespace).Scan(&pending); err != nil {
		return nil, err
	}
	if pending {
		return nil, canonical.Unavailable()
	}
	id := "payload-v1:" + uuid.NewString()
	b, _ := json.Marshal(meta)
	_, err = tx.Exec(ctx, `INSERT INTO ingestion_captures(namespace,position,payload_id,size,sha256,context,state) VALUES($1,$2,$3,0,'',$4,'captured')`, s.Namespace, pos, id, b)
	if err != nil {
		return nil, err
	}
	sum := sha256.New()
	buf := make([]byte, 64<<10)
	var size int64
	ordinal := 0
	for {
		n, e := io.ReadFull(r, buf)
		if n > 0 {
			size += int64(n)
			if size > quota-used {
				return nil, canonical.Fail(503, "capacity_exhausted", "payload quota exhausted; cursor unchanged")
			}
			if _, err = tx.Exec(ctx, `INSERT INTO ingestion_payload_chunks(payload_id,ordinal,data) VALUES($1,$2,$3)`, id, ordinal, buf[:n]); err != nil {
				return nil, err
			}
			_, _ = sum.Write(buf[:n])
			ordinal++
		}
		if e == io.EOF || e == io.ErrUnexpectedEOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	digest := fmt.Sprintf("%x", sum.Sum(nil))
	if _, err = tx.Exec(ctx, `UPDATE ingestion_captures SET size=$3,sha256=$4 WHERE namespace=$1 AND position=$2`, s.Namespace, pos, size, digest); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `UPDATE ingestion_state SET next_position=next_position+1,used_bytes=used_bytes+$2 WHERE namespace=$1`, s.Namespace, size); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, canonical.Unavailable()
	}
	if err = s.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_captures WHERE namespace=$1 AND position=$2 AND sha256=$3)`, s.Namespace, pos, digest); err != nil {
		return nil, err
	}
	return &Capture{Position: pos, PayloadID: id, Size: size, Digest: digest, Context: meta, State: "captured"}, nil
}
func (s *Store) Payload(ctx context.Context, c Capture) ([]byte, error) {
	if c.Size > s.Quota || c.Size < 0 || c.Size > 16<<20 {
		return nil, canonical.Unavailable()
	}
	r, err := s.Open(ctx, c.PayloadID)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	b, err := io.ReadAll(io.LimitReader(r, c.Size+1))
	if err != nil || int64(len(b)) != c.Size || rawinput.Hash(b) != c.Digest {
		return nil, fmt.Errorf("payload integrity failure")
	}
	return b, nil
}

type chunkReader struct {
	ctx     context.Context
	s       *Store
	id      string
	ordinal int
	buf     *bytes.Reader
	closed  atomic.Bool
}

func (r *chunkReader) Read(b []byte) (int, error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	for {
		if r.ctx.Err() != nil {
			return 0, r.ctx.Err()
		}
		if r.buf != nil && r.buf.Len() > 0 {
			return r.buf.Read(b)
		}
		var chunk []byte
		err := r.s.DB.Pool.QueryRow(r.ctx, `SELECT data FROM ingestion_payload_chunks WHERE payload_id=$1 AND ordinal=$2`, r.id, r.ordinal).Scan(&chunk)
		if err == pgx.ErrNoRows {
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		r.ordinal++
		r.buf = bytes.NewReader(chunk)
	}
}
func (r *chunkReader) Close() error { r.closed.Store(true); return nil }
func (s *Store) Open(ctx context.Context, id string) (io.ReadCloser, error) {
	if !rawinput.Opaque(id) {
		return nil, canonical.Fail(400, "invalid_request", "opaque payload identity required")
	}
	var exists bool
	if err := s.DB.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_captures WHERE payload_id=$1)`, id).Scan(&exists); err != nil || !exists {
		return nil, canonical.Unavailable()
	}
	if s.Durability == nil {
		return nil, canonical.Unavailable()
	}
	if err := s.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_captures WHERE payload_id=$1)`, id); err != nil {
		return nil, err
	}
	return &chunkReader{ctx: ctx, s: s, id: id}, nil
}
func (s *Store) Finalize(ctx context.Context, owner canonical.Ownership, c Capture, e rawinput.Envelope) error {
	if err := rawinput.Validate(e); err != nil {
		return err
	}
	tx, err := s.begin(ctx, owner)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	b, _ := json.Marshal(e)
	fp := rawinput.Fingerprint(e)
	var existing *string
	if err = tx.QueryRow(ctx, `SELECT fingerprint FROM ingestion_captures WHERE namespace=$1 AND position=$2 FOR UPDATE`, s.Namespace, c.Position).Scan(&existing); err != nil {
		return err
	}
	if existing != nil && *existing != fp {
		return canonical.Fail(409, "identity_conflict", "capture was already finalized differently")
	}
	if _, err = tx.Exec(ctx, `UPDATE ingestion_captures SET envelope=$3,fingerprint=$4,state='pending' WHERE namespace=$1 AND position=$2 AND state<>'published'`, s.Namespace, c.Position, b, fp); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Published(ctx context.Context, owner canonical.Ownership, c Capture, e rawinput.Envelope) error {
	tx, err := s.begin(ctx, owner)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var through int64
	if err = tx.QueryRow(ctx, `SELECT published_through FROM ingestion_state WHERE namespace=$1 FOR UPDATE`, s.Namespace).Scan(&through); err != nil {
		return err
	}
	if through >= c.Position {
		if err = tx.Rollback(ctx); err != nil {
			return err
		}
		return s.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_state WHERE namespace=$1 AND published_through>=$2)`, s.Namespace, c.Position)
	}
	if through != c.Position-1 {
		return canonical.Fail(409, "source_gap", "publication frontier gap")
	}
	cursor, err := rawinput.ParseCursor(e.Cursor)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE ingestion_captures SET state='published' WHERE namespace=$1 AND position=$2 AND fingerprint=$3`, s.Namespace, c.Position, rawinput.Fingerprint(e)); err != nil {
		return err
	}
	if cursor.History {
		if _, err = tx.Exec(ctx, `INSERT INTO ingestion_progress(namespace,entity,archive_cursor) VALUES($1,$2,$3) ON CONFLICT(namespace,entity) DO UPDATE SET archive_cursor=greatest(ingestion_progress.archive_cursor,excluded.archive_cursor)`, s.Namespace, cursor.Entity, cursor.Next); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE ingestion_state SET published_through=$2,next_task=(next_task+1)%4 WHERE namespace=$1`, s.Namespace, c.Position); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE ingestion_failures SET status='recovered',recovered_at=clock_timestamp() WHERE namespace=$1 AND position=$2`, s.Namespace, c.Position); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return canonical.Unavailable()
	}
	return s.Durability.ConfirmRecord(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_state WHERE namespace=$1 AND published_through>=$2)`, s.Namespace, c.Position)
}
func (s *Store) Progress(ctx context.Context, entity string) (int64, error) {
	var n int64
	err := s.DB.Pool.QueryRow(ctx, `SELECT archive_cursor FROM ingestion_progress WHERE namespace=$1 AND entity=$2`, s.Namespace, entity).Scan(&n)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return n, err
}
func (s *Store) Task(ctx context.Context) (int, error) {
	var n int
	err := s.DB.Pool.QueryRow(ctx, `SELECT next_task FROM ingestion_state WHERE namespace=$1`, s.Namespace).Scan(&n)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return n, err
}

var ErrDeferred = errors.New("input is parked or waiting for its retry deadline")

func (s *Store) Runnable(ctx context.Context, c Capture) error {
	var blocked bool
	err := s.DB.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_failures WHERE namespace=$1 AND position=$2 AND (status='parked' OR status='retrying' AND next_attempt_at>clock_timestamp()))`, s.Namespace, c.Position).Scan(&blocked)
	if err != nil {
		return err
	}
	if blocked {
		return ErrDeferred
	}
	return nil
}
func (s *Store) Failure(ctx context.Context, owner canonical.Ownership, c Capture, reason string, retryable bool) error {
	tx, err := s.begin(ctx, owner)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	status, classification := "parked", "incompatible"
	if retryable {
		status, classification = "retrying", "transient"
	}
	// The complete captured response is one update. Its transport identity is
	// known even while decoding/registry access has not finalized the envelope.
	id := rawinput.Identity("raw-v1", "BMKG", s.Namespace, c.Position, nil, "update")
	if c.Envelope != nil {
		id = c.Envelope.MessageID
	}
	var attempt int64
	err = tx.QueryRow(ctx, `INSERT INTO ingestion_failures(namespace,message_id,source,position,received_at,reason,classification,status,attempts,next_attempt_at) VALUES($1,$2,'BMKG',$3,$4,$5,$6,$7,1,clock_timestamp()+interval '1 second') ON CONFLICT(namespace,message_id) DO UPDATE SET reason=excluded.reason,classification=excluded.classification,status=excluded.status,attempts=ingestion_failures.attempts+1,next_attempt_at=excluded.next_attempt_at RETURNING attempts`, s.Namespace, id, c.Position, c.Context.ReceivedAt, reason, classification, status).Scan(&attempt)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ingestion_failure_attempts(namespace,message_id,attempt,reason) VALUES($1,$2,$3,$4)`, s.Namespace, id, attempt, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func ReceivedNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }
