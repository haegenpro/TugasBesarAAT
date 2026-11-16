package coastal

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/rawinput"
	"context"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/nats-io/nats.go"
)

const (
	Recipient      = "TIM_LAPANGAN"
	WarningSubject = "bnpb.coastal-warning-value"
)

type Warning struct {
	NotificationID string
	OperationID    string
	Payload        []byte
}

type Warnings struct {
	JS         rawinput.Broker
	Registry   *avrowire.Client
	SchemaText string
}

func OperationID(o Observation, digest string) string {
	return rawinput.Identity("coastal-op-v1", o.ID, digest)
}

func NotificationID(namespace string, o Observation, operation string) string {
	return rawinput.Identity("warning-v1", Recipient, namespace, o.ID, operation)
}

func (w Warnings) Build(ctx context.Context, e rawinput.Envelope, o Observation, s Station) (Warning, error) {
	schema, err := w.Registry.Register(ctx, WarningSubject, w.SchemaText)
	if err != nil {
		return Warning{}, err
	}
	operation := OperationID(o, e.Digest)
	id := NotificationID(e.Namespace, o, operation)
	trace := goavro.Union("null", nil)
	if e.Traceparent != nil {
		trace = goavro.Union("string", *e.Traceparent)
	}
	native := map[string]any{
		"warning_version": int32(1), "notification_id": id, "recipient": Recipient, "source": Source,
		"source_namespace": e.Namespace, "observation_id": o.ID, "operation_id": operation,
		"operation_position": *e.Position, "operation": e.Operation,
		"observed_at": o.ObservedAt.UTC().Format(time.RFC3339Nano), "first_received_at": e.ReceivedAt,
		"critical": o.Critical, "station_id": s.ID, "province": s.Province, "latitude": s.Latitude, "longitude": s.Longitude,
		"sea_level_anomaly_m": o.SeaLevel, "wave_height_m": o.WaveHeight, "quality": o.Quality,
		"payload_sha256": e.Digest, "correlation_id": e.CorrelationID, "traceparent": trace, "hazard_id": goavro.Union("null", nil),
	}
	payload, err := avrowire.Encode(schema, native)
	if err != nil {
		return Warning{}, err
	}
	return Warning{NotificationID: id, OperationID: operation, Payload: payload}, nil
}

func (w Warnings) Publish(ctx context.Context, e rawinput.Envelope, warning Warning) error {
	msg := &nats.Msg{Subject: rawinput.Critical.Subject, Data: warning.Payload, Header: nats.Header{}}
	msg.Header.Set("Nats-Msg-Id", warning.NotificationID)
	msg.Header.Set("X-Correlation-ID", e.CorrelationID)
	if e.Traceparent != nil {
		msg.Header.Set("Traceparent", *e.Traceparent)
	}
	_, err := w.JS.PublishMsg(ctx, msg)
	return err
}
