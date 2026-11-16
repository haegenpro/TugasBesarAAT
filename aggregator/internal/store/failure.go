package store

import (
	"aggregator/internal/canonical"
	"aggregator/internal/clients"
	"aggregator/internal/platform"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"io"
)

func (s *Store) RecordFailure(ctx context.Context, f clients.Failure) error {
	if f.PayloadStream != nil {
		return s.recordFailureStream(ctx, f)
	}
	tx, err := s.sourceTx(ctx, f.Source)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	digest := sha256.Sum256(f.Payload)
	status := "parked"
	if f.Retryable {
		status = "retrying"
	}
	_, err = tx.Exec(ctx, `INSERT INTO failed_ingestions(message_id,source,entity,digest,raw_payload,truncated,retryable,reason,status,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(source,entity,digest) DO UPDATE SET attempts=failed_ingestions.attempts+1,reason=excluded.reason,status=excluded.status,retryable=excluded.retryable,last_seen_at=now()`, uuid.NewString(), f.Source, f.Entity, hex.EncodeToString(digest[:]), f.Payload, f.Truncated, f.Retryable, f.Reason, status, f.CorrelationID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) recordFailureStream(ctx context.Context, f clients.Failure) error {
	tx, err := s.sourceTx(ctx, f.Source)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	// Serialize quota admission, not canonical event writes. Retained failures
	// are never evicted to make room for new input.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(726113)`); err != nil {
		return err
	}
	var used int64
	if err = tx.QueryRow(ctx, `SELECT coalesce(sum(size),0) FROM failed_ingestion_payloads`).Scan(&used); err != nil {
		return err
	}
	quota := int64(platform.Int("FAILED_INPUT_QUOTA_BYTES", 256<<20))
	id := uuid.NewString()
	if _, err = tx.Exec(ctx, `INSERT INTO failed_ingestion_payloads(payload_id,content_type) VALUES($1,$2)`, id, f.ContentType); err != nil {
		return err
	}
	sum, size, ordinal := sha256.New(), int64(0), 0
	buf := make([]byte, 64<<10)
	reader := &failureStreamReader{Reader: f.PayloadStream}
	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			size += int64(n)
			if size > quota-used {
				return canonical.Fail(503, "capacity_exhausted", "failed-input quota exhausted; source progress unchanged")
			}
			if _, err = tx.Exec(ctx, `INSERT INTO failed_ingestion_payload_chunks(payload_id,ordinal,data) VALUES($1,$2,$3)`, id, ordinal, buf[:n]); err != nil {
				return err
			}
			sum.Write(buf[:n])
			ordinal++
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF && reader.last == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("incomplete upstream response; failure capture rolled back: %w", readErr)
		}
		if err = ctx.Err(); err != nil {
			return err
		}
	}
	digest := hex.EncodeToString(sum.Sum(nil))
	if _, err = tx.Exec(ctx, `UPDATE failed_ingestion_payloads SET size=$2,sha256=$3 WHERE payload_id=$1`, id, size, digest); err != nil {
		return err
	}
	status := "parked"
	if f.Retryable {
		status = "retrying"
	}
	_, err = tx.Exec(ctx, `INSERT INTO failed_ingestions(message_id,source,entity,digest,raw_payload,truncated,retryable,reason,status,correlation_id,payload_id) VALUES($1,$2,$3,$4,''::bytea,false,$5,$6,$7,$8,$9) ON CONFLICT(source,entity,digest) DO UPDATE SET attempts=failed_ingestions.attempts+1,reason=excluded.reason,status=excluded.status,retryable=excluded.retryable,last_seen_at=now()`, uuid.NewString(), f.Source, f.Entity, digest, f.Retryable, f.Reason, status, f.CorrelationID, id)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM failed_ingestion_payloads WHERE payload_id=$1 AND NOT EXISTS(SELECT 1 FROM failed_ingestions WHERE payload_id=$1)`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type failureStreamReader struct {
	io.Reader
	last error
}

func (r *failureStreamReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.last = err
	return n, err
}

// WriteFailurePayload streams an authenticated Ops inspection without loading
// an oversized body in memory. All chunks and the metadata commit atomically.
func (s *Store) WriteFailurePayload(ctx context.Context, id string, out io.Writer) (int64, string, string, error) {
	var payloadID *string
	var inline []byte
	var digest string
	if err := s.Pool.QueryRow(ctx, `SELECT payload_id::text,raw_payload,digest FROM failed_ingestions WHERE message_id::text=$1`, id).Scan(&payloadID, &inline, &digest); err != nil {
		return 0, "", "", err
	}
	if payloadID == nil {
		n, err := out.Write(inline)
		return int64(n), digest, "", err
	}
	var size int64
	var media string
	if err := s.Pool.QueryRow(ctx, `SELECT size,sha256,content_type FROM failed_ingestion_payloads WHERE payload_id=$1`, *payloadID).Scan(&size, &digest, &media); err != nil {
		return 0, "", "", err
	}
	rows, err := s.Pool.Query(ctx, `SELECT data FROM failed_ingestion_payload_chunks WHERE payload_id=$1 ORDER BY ordinal`, *payloadID)
	if err != nil {
		return 0, "", "", err
	}
	defer rows.Close()
	var written int64
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return written, digest, media, err
		}
		n, err := out.Write(b)
		written += int64(n)
		if err != nil {
			return written, digest, media, err
		}
		if n != len(b) {
			return written, digest, media, io.ErrShortWrite
		}
	}
	if err = rows.Err(); err != nil {
		return written, digest, media, err
	}
	if written != size {
		return written, digest, media, fmt.Errorf("failure payload size mismatch")
	}
	return written, digest, media, nil
}
