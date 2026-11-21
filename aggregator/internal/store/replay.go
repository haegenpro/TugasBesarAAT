package store

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/clients"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"path/filepath"
	"strings"
)

func (s *Store) Policy(ctx context.Context) (int, error) {
	var version int
	e := s.Pool.QueryRow(ctx, `SELECT version FROM source_policy WHERE source='PVMBG'`).Scan(&version)
	return version, e
}
func (s *Store) SetPolicy(ctx context.Context, version int) error {
	if version != 1 && version != 2 {
		return ErrOrder
	}
	_, e := s.Pool.Exec(ctx, `UPDATE source_policy SET version=$1 WHERE source='PVMBG'`, version)
	return e
}
func (s *Store) SyncPolicy(ctx context.Context, readers *avrowire.ReaderSwitch, baseline string) error {
	version, e := s.Policy(ctx)
	if e != nil {
		return e
	}
	if version == clients.VolcanicAdapter() {
		return nil
	}
	path := baseline
	if version == 2 {
		path = filepath.Join(filepath.Dir(baseline), "volcanic-adapter-v2.avsc")
	}
	reader, e := avrowire.LoadSourceReader(path, "VolcanicHistoryPage")
	if e != nil {
		return e
	}
	return readers.Install(ctx, reader, func() error { return clients.SetVolcanicAdapter(version) })
}
func (s *Store) BlockParkedVolcanic(ctx context.Context, _ string) error {
	var blocked bool
	e := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM failed_ingestions WHERE source='PVMBG' AND status='parked')`).Scan(&blocked)
	if e != nil {
		return e
	}
	if blocked {
		return errors.New("PVMBG evidence is parked; activate a compatible policy and replay the retained records")
	}
	return nil
}
func (s *Store) RequestReplay(ctx context.Context, ids []string) (int64, error) {
	if len(ids) < 1 || len(ids) > 1000 {
		return 0, ErrOrder
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			return 0, ErrOrder
		}
		seen[id] = true
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback(ctx)
	var count int64
	if e = tx.QueryRow(ctx, `SELECT count(*) FROM failed_ingestions WHERE source='PVMBG' AND message_id::text=ANY($1)`, ids).Scan(&count); e != nil {
		return 0, e
	}
	if count != int64(len(ids)) {
		return 0, ErrOrder
	}
	result, e := tx.Exec(ctx, `UPDATE failed_ingestions SET replay_requested=true,replay_generation=replay_generation+1 WHERE source='PVMBG' AND message_id::text=ANY($1) AND NOT replay_requested`, ids)
	if e != nil {
		return 0, e
	}
	return result.RowsAffected(), tx.Commit(ctx)
}
func (s *Store) NextLegacyReplay(ctx context.Context) (*clients.ParkedPayload, error) {
	var p clients.ParkedPayload
	var size *int64
	e := s.Pool.QueryRow(ctx, `SELECT f.message_id::text,f.entity,f.raw_payload,f.truncated,p.size FROM failed_ingestions f LEFT JOIN failed_ingestion_payloads p USING(payload_id) WHERE source='PVMBG' AND replay_requested ORDER BY received_at,message_id LIMIT 1`).Scan(&p.ID, &p.Entity, &p.Payload, &p.Truncated, &size)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	if size != nil {
		limit := int64(platform.Int("MAX_PAYLOAD_BYTES", 4<<20))
		if *size > limit {
			p.Rejection = fmt.Sprintf("complete payload retained (%d bytes); raise MAX_PAYLOAD_BYTES before replay (currently %d)", *size, limit)
		} else {
			var body bytes.Buffer
			_, digest, _, e := s.WriteFailurePayload(ctx, p.ID, &body)
			if e != nil {
				return nil, e
			}
			sum := sha256.Sum256(body.Bytes())
			if hex.EncodeToString(sum[:]) != digest {
				return nil, errors.New("retained payload integrity failure")
			}
			p.Payload = body.Bytes()
			p.Truncated = false
		}
	}
	return &p, nil
}
func (s *Store) FinishLegacyReplay(ctx context.Context, id string, failure error) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if s.Distributed {
		if e = ownership.Guard(ctx, tx); e != nil {
			return e
		}
	}
	if failure != nil {
		_, e = tx.Exec(ctx, `UPDATE failed_ingestions SET replay_requested=false,status='parked',reason=$2,attempts=attempts+1,last_seen_at=clock_timestamp() WHERE message_id::text=$1`, id, failure.Error())
	} else {
		var payload *string
		e = tx.QueryRow(ctx, `DELETE FROM failed_ingestions WHERE message_id::text=$1 AND replay_requested RETURNING payload_id::text`, id).Scan(&payload)
		if errors.Is(e, pgx.ErrNoRows) {
			return tx.Commit(ctx)
		}
		if e == nil && payload != nil {
			_, e = tx.Exec(ctx, `DELETE FROM failed_ingestion_payloads WHERE payload_id=$1 AND NOT EXISTS(SELECT 1 FROM failed_ingestions WHERE payload_id=$1)`, *payload)
		}
	}
	if e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func volcanicScope(scope string) bool { return scope == "PVMBG" || strings.Contains(scope, "volcanic") }
