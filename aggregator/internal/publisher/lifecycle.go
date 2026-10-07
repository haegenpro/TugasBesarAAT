package publisher

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/domain"
	"aggregator/internal/middleware"
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"time"
)

// A dedicated registered envelope carries a versioned JSON snapshot without altering HazardEvent.
const lifecycleSchema = `{"type":"record","name":"ReportLifecycleEnvelope","namespace":"id.go.bnpb","fields":[{"name":"envelope_version","type":"int"},{"name":"correlation_id","type":"string"},{"name":"event_id","type":"string"},{"name":"record_id","type":"string"},{"name":"revision","type":"long"},{"name":"event_type","type":"string"},{"name":"snapshot","type":"bytes"}]}`

func (p *Publisher) PublishLifecycle(ctx context.Context, e domain.LifecycleEvent) error {
	if p.schema == nil {
		schema, err := p.registry.Register(avrowire.WithCorrelationID(ctx, e.CorrelationID), p.subject+"-value", lifecycleSchema)
		if err != nil {
			return fmt.Errorf("register lifecycle schema: %w", err)
		}
		p.schema = schema
	}
	snapshot, err := json.Marshal(e)
	if err != nil {
		return err
	}
	data, err := avrowire.Encode(p.schema, map[string]any{"envelope_version": int32(1), "correlation_id": e.CorrelationID, "event_id": e.EventID, "record_id": e.RecordID, "revision": e.Revision, "event_type": e.EventType, "snapshot": snapshot})
	if err != nil {
		return err
	}
	msg := nats.NewMsg(p.subject)
	msg.Header.Set("X-Correlation-ID", e.CorrelationID)
	msg.Data = data
	started := time.Now()
	_, err = p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(e.EventID))
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	middleware.LogOutbound(p.logger, "aggregator", e.CorrelationID, "nats", p.subject, 0, err == nil, time.Since(started).Milliseconds(), reason)
	return err
}
