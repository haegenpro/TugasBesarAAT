package store

import (
	"aggregator/internal/clients"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/google/uuid"
)

func (s *Store) RecordFailure(ctx context.Context, f clients.Failure) error {
	digest := sha256.Sum256(f.Payload)
	status := "parked"
	if f.Retryable {
		status = "retrying"
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO failed_ingestions(message_id,source,entity,digest,raw_payload,truncated,retryable,reason,status,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(source,entity,digest) DO UPDATE SET attempts=failed_ingestions.attempts+1,reason=excluded.reason,status=excluded.status,retryable=excluded.retryable,last_seen_at=now()`, uuid.NewString(), f.Source, f.Entity, hex.EncodeToString(digest[:]), f.Payload, f.Truncated, f.Retryable, f.Reason, status, f.CorrelationID)
	return err
}
