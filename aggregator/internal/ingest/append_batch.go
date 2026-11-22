package ingest

import (
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Append durably captures one source record. It is AppendBatch with a batch of
// one, which is also the comparison mode used when judging batch sizes.
func (s *Store) Append(ctx context.Context, i Input, initial int64, subject string) error {
	return s.AppendBatch(ctx, []Input{i}, initial, subject)
}

// AppendBatch captures a contiguous run of records of one source and namespace in
// one transaction: original payloads and metadata, raw outbox entries, resolution
// of retained response failures, warning expectations and the received frontier
// either all become durable or none do.
//
// Positions that are already durable must carry identical content (the retry
// after an ambiguous commit); they are skipped. The first new position must be
// the next one after the received frontier, so the stored sequence never has a gap.
func (s *Store) AppendBatch(ctx context.Context, inputs []Input, initial int64, subject string) error {
	if len(inputs) == 0 {
		return nil
	}
	ctx, span := platform.Start(ctx, "durable source capture")
	defer span.End()
	defer stage("acquire_append")()
	source, namespace := inputs[0].Source, inputs[0].Namespace
	ids := make([]string, len(inputs))
	digests := make([]string, len(inputs))
	metadata := make([]string, len(inputs))
	payloads := make([][]byte, len(inputs))
	// Everything that does not need the database is prepared before the locks are taken.
	for k := range inputs {
		i := inputs[k]
		if i.Source != source || i.Namespace != namespace {
			return errors.New("batch spans more than one source namespace")
		}
		if k > 0 && i.Position != inputs[k-1].Position+1 {
			return ErrGap
		}
		original, e := json.Marshal(i.Metadata)
		if e != nil {
			return e
		}
		digests[k] = Hash(append(append([]byte{}, i.Payload...), original...))
		ids[k] = i.ID
		if ids[k] == "" {
			ids[k] = uuid.NewSHA1(uuid.NameSpaceOID, []byte(i.Source+"\x00"+i.Namespace+"\x00"+stringPosition(i.Position))).String()
		}
		// Operational trace context must survive a crash/outbox retry, but must not
		// change the source identity/content digest on a new acquisition attempt.
		traced, e := json.Marshal(traceMetadata(ctx, i.Metadata))
		if e != nil {
			return e
		}
		metadata[k] = string(traced)
		payloads[k] = i.Payload
		if payloads[k] == nil {
			payloads[k] = []byte{}
		}
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO source_progress(source,namespace,received,prepared,applied) VALUES($1,$2,$3,$3,$3) ON CONFLICT DO NOTHING`, source, namespace, initial-1); e != nil {
		return e
	}
	var pos int64
	locked := time.Now()
	e = tx.QueryRow(ctx, `SELECT received FROM source_progress WHERE source=$1 AND namespace=$2 FOR UPDATE`, source, namespace).Scan(&pos)
	lockWait("source_progress", locked)
	if e != nil {
		return e
	}
	first, last := inputs[0].Position, inputs[len(inputs)-1].Position
	skip := 0
	if first <= pos {
		upto := min(last, pos)
		rows, e := tx.Query(ctx, `SELECT position,digest FROM raw_inputs WHERE source=$1 AND namespace=$2 AND position BETWEEN $3 AND $4`, source, namespace, first, upto)
		if e != nil {
			return e
		}
		stored := map[int64]string{}
		for rows.Next() {
			var position int64
			var digest string
			if e = rows.Scan(&position, &digest); e != nil {
				rows.Close()
				return e
			}
			stored[position] = digest
		}
		rows.Close()
		if e = rows.Err(); e != nil {
			return e
		}
		for k := 0; int64(k) <= upto-first; k++ {
			old, ok := stored[inputs[k].Position]
			if !ok {
				return fmt.Errorf("position %d is below the received frontier but not stored", inputs[k].Position)
			}
			if old != digests[k] {
				return ErrConflict
			}
		}
		skip = int(upto - first + 1)
	}
	if skip == len(inputs) {
		return commit(ctx, tx, "append")
	}
	if inputs[skip].Position != pos+1 {
		return ErrGap
	}
	fresh := inputs[skip:]
	positions := make([]int64, len(fresh))
	entities := make([]string, len(fresh))
	correlations := make([]string, len(fresh))
	resolved := make([]string, len(fresh))
	newBytes := 0
	for k, i := range fresh {
		positions[k] = i.Position
		entities[k] = i.Entity
		correlations[k] = i.CorrelationID
		resolved[k] = "stream:" + stringPosition(i.Position)
		newBytes += len(i.Payload)
	}
	rows, e := tx.Query(ctx, `INSERT INTO raw_inputs(message_id,source,namespace,position,entity_key,payload,metadata,digest,correlation_id)
 SELECT t.id::uuid,$1,$2,t.position,t.entity,t.payload,t.metadata::jsonb,t.digest,t.correlation
 FROM unnest($3::text[],$4::bigint[],$5::text[],$6::bytea[],$7::text[],$8::text[],$9::text[]) WITH ORDINALITY AS t(id,position,entity,payload,metadata,digest,correlation,ordinal)
 ORDER BY t.ordinal RETURNING position,received_at`,
		source, namespace, ids[skip:], positions, entities, payloads[skip:], metadata[skip:], digests[skip:], correlations)
	if e != nil {
		return e
	}
	receivedAt := map[int64]time.Time{}
	for rows.Next() {
		var position int64
		var at time.Time
		if e = rows.Scan(&position, &at); e != nil {
			rows.Close()
			return e
		}
		receivedAt[position] = at
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	for k, i := range fresh {
		if critical, _ := i.Metadata["critical"].(bool); critical {
			i.ID = ids[skip+k]
			if _, e = tx.Exec(ctx, `INSERT INTO warning_expectations(notification_id,message_id,first_received_at) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, WarningID(i), i.ID, receivedAt[i.Position]); e != nil {
				return e
			}
		}
	}
	if _, e = tx.Exec(ctx, `INSERT INTO raw_outbox(message_id,subject) SELECT unnest($1::text[])::uuid,$2`, ids[skip:], subject); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE source_progress SET received=$3 WHERE source=$1 AND namespace=$2`, source, namespace, last); e != nil {
		return e
	}
	if source == "SATELIT" {
		if _, e = tx.Exec(ctx, `UPDATE response_failures SET resolved=true WHERE source=$1 AND namespace=$2 AND entity=ANY($3) AND NOT resolved`, source, namespace, resolved); e != nil {
			return e
		}
	}
	if e = commit(ctx, tx, "append"); e != nil {
		return e
	}
	batchStats("append", len(fresh), newBytes)
	return nil
}
