package ingest

import (
	"aggregator/internal/avrowire"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
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
