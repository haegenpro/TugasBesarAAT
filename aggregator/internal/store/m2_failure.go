package store

import (
	"aggregator/internal/clients"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
)

func (s *Store) recordM2Failure(ctx context.Context, f clients.Failure) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(726113)`); e != nil {
		return e
	}
	var used int64
	if e = tx.QueryRow(ctx, `SELECT (SELECT coalesce(sum(octet_length(raw_payload)),0) FROM failed_ingestions)+(SELECT coalesce(sum(size),0) FROM failed_ingestion_payloads)`).Scan(&used); e != nil {
		return e
	}
	quota := int64(platform.Int("FAILED_INPUT_QUOTA_BYTES", 256<<20))
	raw := f.Payload
	var payloadID any
	digest := ""
	if f.PayloadStream != nil {
		id := uuid.NewString()
		payloadID = id
		raw = []byte{}
		if _, e = tx.Exec(ctx, `INSERT INTO failed_ingestion_payloads(payload_id,content_type) VALUES($1,$2)`, id, f.ContentType); e != nil {
			return e
		}
		sum := sha256.New()
		var size int64
		ordinal := 0
		buffer := make([]byte, 64<<10)
		reader := &failureStreamReader{Reader: f.PayloadStream}
		for {
			n, readErr := io.ReadFull(reader, buffer)
			if n > 0 {
				size += int64(n)
				if used+size > quota {
					return errors.New("quarantine capacity exhausted; source progress unchanged")
				}
				if _, e = tx.Exec(ctx, `INSERT INTO failed_ingestion_payload_chunks(payload_id,ordinal,data) VALUES($1,$2,$3)`, id, ordinal, buffer[:n]); e != nil {
					return e
				}
				sum.Write(buffer[:n])
				ordinal++
			}
			if readErr == io.EOF || (readErr == io.ErrUnexpectedEOF && reader.last == io.EOF) {
				break
			}
			if readErr != nil {
				return fmt.Errorf("original response incomplete: %w", readErr)
			}
			if e = ctx.Err(); e != nil {
				return e
			}
		}
		digest = hex.EncodeToString(sum.Sum(nil))
		if _, e = tx.Exec(ctx, `UPDATE failed_ingestion_payloads SET size=$2,sha256=$3 WHERE payload_id=$1`, id, size, digest); e != nil {
			return e
		}
	} else {
		if used+int64(len(raw)) > quota {
			return errors.New("quarantine capacity exhausted; source progress unchanged")
		}
		sum := sha256.Sum256(raw)
		digest = hex.EncodeToString(sum[:])
	}
	identity := f.Identity
	if identity == "" {
		identity = "attempt:" + f.CorrelationID
	}
	if identity == "attempt:" {
		identity = "attempt:" + uuid.NewString()
	}
	status := "parked"
	if f.Retryable {
		status = "retrying"
	}
	// A content hash checks integrity; source response/sequence identity prevents
	// unrelated messages with identical bytes from collapsing into one record.
	_, e = tx.Exec(ctx, `INSERT INTO failed_ingestions(message_id,source,entity,original_identity,digest,raw_payload,truncated,retryable,reason,status,correlation_id,payload_id) VALUES($1,$2,$3,$4,$5,$6,false,$7,$8,$9,$10,$11)
 ON CONFLICT(source,entity,original_identity,digest) DO UPDATE SET attempts=failed_ingestions.attempts+1,reason=excluded.reason,last_seen_at=clock_timestamp(),status=CASE WHEN failed_ingestions.status='parked' THEN 'parked' ELSE excluded.status END`, uuid.NewString(), f.Source, f.Entity, identity, digest, raw, f.Retryable, f.Reason, status, f.CorrelationID, payloadID)
	if e != nil {
		return e
	}
	if payloadID != nil {
		if _, e = tx.Exec(ctx, `DELETE FROM failed_ingestion_payloads WHERE payload_id=$1 AND NOT EXISTS(SELECT 1 FROM failed_ingestions WHERE payload_id=$1)`, payloadID); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) FailurePayloadMetadata(ctx context.Context, id string) (int64, string, error) {
	var size int64
	var digest string
	e := s.Pool.QueryRow(ctx, `SELECT coalesce(p.size,octet_length(f.raw_payload)),f.digest FROM failed_ingestions f LEFT JOIN failed_ingestion_payloads p USING(payload_id) WHERE f.message_id=$1`, id).Scan(&size, &digest)
	return size, digest, e
}
