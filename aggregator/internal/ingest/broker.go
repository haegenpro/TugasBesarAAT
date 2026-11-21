package ingest

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"net/http"
	"time"
)

// Original bytes are already durable in this owner's database. Broker messages
// carry an Avro reference; workers reload that immutable record before acting.
// This also prevents a large retained source body from wedging the NATS lane.
const RawSchema = `{"type":"record","name":"AcquiredInput","namespace":"id.go.bnpb.m2","fields":[{"name":"message_id","type":"string"},{"name":"source","type":"string"},{"name":"namespace","type":"string"},{"name":"position","type":"long"},{"name":"received_at","type":"string"},{"name":"correlation_id","type":"string"}]}`

func scanInput(row pgx.Row) (Input, error) {
	var i Input
	var metadata []byte
	e := row.Scan(&i.ID, &i.Source, &i.Namespace, &i.Position, &i.Entity, &i.Payload, &metadata, &i.CorrelationID, &i.ReceivedAt)
	if e == nil {
		e = decodeMetadata(metadata, &i.Metadata)
	}
	return i, e
}

const inputColumns = `i.message_id::text,i.source,i.namespace,i.position,i.entity_key,i.payload,i.metadata,i.correlation_id,i.received_at`

func (s *Store) Input(ctx context.Context, id string) (Input, error) {
	return scanInput(s.Pool.QueryRow(ctx, `SELECT `+inputColumns+` FROM raw_inputs i WHERE message_id=$1`, id))
}
func (s *Store) PublishOne(ctx context.Context, js jetstream.JetStream, registry *avrowire.Client) (bool, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(ctx)
	if e = ownership.Guard(ctx, tx); e != nil {
		return false, e
	}
	var subject string
	var i Input
	var metadataBytes []byte
	e = tx.QueryRow(ctx, `SELECT `+inputColumns+`,o.subject FROM raw_outbox o JOIN raw_inputs i USING(message_id) ORDER BY CASE o.subject WHEN 'raw.coastal.critical' THEN 0 WHEN 'raw.field' THEN 1 WHEN 'raw.coastal.normal' THEN 2 ELSE 3 END,i.received_at,i.position,i.message_id LIMIT 1 FOR UPDATE OF o`).Scan(&i.ID, &i.Source, &i.Namespace, &i.Position, &i.Entity, &i.Payload, &metadataBytes, &i.CorrelationID, &i.ReceivedAt, &subject)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if e = decodeMetadata(metadataBytes, &i.Metadata); e != nil {
		return false, e
	}
	ctx, span := inputSpan(ctx, i, "raw input publication")
	defer span.End()
	schema, e := registry.Register(ctx, "acquired-input-reference-value", RawSchema)
	if e != nil {
		return false, e
	}
	payload := referenceValue(i)
	frame, e := avrowire.Encode(schema, payload)
	if e != nil {
		return false, e
	}
	msg := nats.NewMsg(subject)
	msg.Data = frame
	msg.Header.Set("Nats-Msg-Id", i.ID+":"+fmt.Sprint(replayGeneration(i)))
	msg.Header.Set("X-Correlation-ID", i.CorrelationID)
	platform.Inject(ctx, http.Header(msg.Header))
	if _, e = js.PublishMsg(ctx, msg); e != nil {
		return false, e
	}
	if _, e = tx.Exec(ctx, `DELETE FROM raw_outbox WHERE message_id=$1`, i.ID); e != nil {
		return false, e
	}
	e = tx.Commit(ctx)
	if e == nil {
		inputLog(ctx, i, "raw input published")
	}
	return true, e
}
func (s *Store) RunPublisher(ctx context.Context, url string, registry *avrowire.Client) {
	for ctx.Err() == nil {
		err := s.Leases.Run(ctx, "raw-outbox", func(c context.Context) error {
			nc, e := nats.Connect(url, nats.Timeout(time.Second))
			if e != nil {
				return e
			}
			defer nc.Close()
			js, e := jetstream.New(nc)
			if e != nil {
				return e
			}
			for c.Err() == nil {
				found, e := s.PublishOne(c, js, registry)
				if e != nil {
					return e
				}
				if !found {
					if !Sleep(c, 100*time.Millisecond) {
						break
					}
				}
			}
			return c.Err()
		})
		if err != nil {
			platform.Add("raw_publish_retries_total", 1)
		}
		Sleep(ctx, time.Second)
	}
}
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func decodeInput(ctx context.Context, registry *avrowire.Client, raw []byte) (Input, error) {
	r, e := avrowire.NewReader(RawSchema)
	if e != nil {
		return Input{}, e
	}
	native, e := registry.DecodeWithReader(ctx, raw, r)
	if e != nil {
		return Input{}, e
	}
	m, ok := native.(map[string]any)
	if !ok {
		return Input{}, fmt.Errorf("invalid acquired-input record")
	}
	body, e := json.Marshal(m)
	if e != nil {
		return Input{}, e
	}
	var wire struct {
		ID        string    `json:"message_id"`
		Source    string    `json:"source"`
		Namespace string    `json:"namespace"`
		Position  int64     `json:"position"`
		At        time.Time `json:"received_at"`
		CID       string    `json:"correlation_id"`
	}
	if e = json.Unmarshal(body, &wire); e != nil {
		return Input{}, e
	}
	return Input{ID: wire.ID, Source: wire.Source, Namespace: wire.Namespace, Position: wire.Position, CorrelationID: wire.CID, ReceivedAt: wire.At}, nil
}

func referenceValue(i Input) map[string]any {
	return map[string]any{"message_id": i.ID, "source": i.Source, "namespace": i.Namespace, "position": i.Position, "received_at": i.ReceivedAt.UTC().Format(time.RFC3339Nano), "correlation_id": i.CorrelationID}
}

func decodeMetadata(raw []byte, out *map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(out)
}
