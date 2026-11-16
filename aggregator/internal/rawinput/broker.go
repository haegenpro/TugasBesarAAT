package rawinput

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"time"
)

type Stream struct{ Name, Subject, Consumer string }

var BMKG = Stream{"BNPB_RAW_BMKG", "bnpb.raw.bmkg", "bmkg-handoff-v1"}
var Satellite = Stream{"BNPB_RAW_SATELLITE", "bnpb.raw.satellite", "wildfire-handoff-v1"}
var Coastal = Stream{"BNPB_RAW_COASTAL", "bnpb.raw.coastal", "coastal-validation-v1"}
var Reports = Stream{"BNPB_RAW_REPORTS", "bnpb.raw.reports", "field-reports-v1"}
var Critical = Stream{"BNPB_CRITICAL", "bnpb.warning.coastal", "tim-lapangan-v1"}

const CoastalCanonical = "coastal-canonical-v1"

// Ensure provisions consumers BEFORE acquisition can become ready. A drifted
// existing stream is rejected, never silently rewritten over retained work.
func Ensure(ctx context.Context, js jetstream.JetStream, s Stream, replicas int) (jetstream.Consumer, error) {
	stream, err := ensureStream(ctx, js, s, replicas)
	if err != nil {
		return nil, err
	}
	return durable(ctx, stream, s.Subject, s.Consumer)
}

func EnsureConsumer(ctx context.Context, js jetstream.JetStream, s Stream, replicas int, name string) (jetstream.Consumer, error) {
	stream, err := ensureStream(ctx, js, s, replicas)
	if err != nil {
		return nil, err
	}
	return durable(ctx, stream, s.Subject, name)
}

func ensureStream(ctx context.Context, js jetstream.JetStream, s Stream, replicas int) (jetstream.Stream, error) {
	if replicas != 3 && replicas != 1 {
		return nil, fmt.Errorf("raw streams require one or three replicas")
	}
	cfg := jetstream.StreamConfig{Name: s.Name, Subjects: []string{s.Subject}, Storage: jetstream.FileStorage, Retention: jetstream.InterestPolicy, Discard: jetstream.DiscardNew, MaxAge: 0, MaxBytes: 1 << 30, MaxMsgSize: MaxMessage, Duplicates: 120 * time.Second, Replicas: replicas}
	stream, err := js.Stream(ctx, s.Name)
	if err != nil {
		if err != jetstream.ErrStreamNotFound {
			return nil, err
		}
		stream, err = js.CreateStream(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	c := info.Config
	if c.Storage != cfg.Storage || c.Retention != cfg.Retention || c.Discard != cfg.Discard || c.MaxAge != 0 || c.Replicas != replicas || c.MaxBytes != cfg.MaxBytes || c.MaxMsgSize != cfg.MaxMsgSize || c.MaxMsgs > 0 || c.MaxMsgsPerSubject > 0 || len(c.Subjects) != 1 || c.Subjects[0] != s.Subject || c.Duplicates != cfg.Duplicates {
		return nil, fmt.Errorf("unsafe raw stream configuration: %s", s.Name)
	}
	return stream, nil
}

func durable(ctx context.Context, stream jetstream.Stream, subject, name string) (jetstream.Consumer, error) {
	consumer, err := stream.Consumer(ctx, name)
	if err == jetstream.ErrConsumerNotFound {
		consumer, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: name, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 128, FilterSubject: subject, MaxRequestBatch: 32, MaxRequestExpires: time.Second})
	}
	if err != nil {
		return nil, err
	}
	ci, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}
	cc := ci.Config
	if cc.Durable != name || cc.AckPolicy != jetstream.AckExplicitPolicy || cc.FilterSubject != subject || cc.AckWait != 30*time.Second || cc.MaxDeliver != -1 || cc.MaxAckPending != 128 || cc.MaxRequestBatch != 32 || cc.MaxRequestExpires != time.Second {
		return nil, fmt.Errorf("unsafe raw consumer configuration: %s", name)
	}
	return consumer, nil
}
