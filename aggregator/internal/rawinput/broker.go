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

// Ensure provisions consumers BEFORE acquisition can become ready. A drifted
// existing stream is rejected, never silently rewritten over retained work.
func Ensure(ctx context.Context, js jetstream.JetStream, s Stream, replicas int) (jetstream.Consumer, error) {
	if replicas != 3 {
		return nil, fmt.Errorf("raw streams require three replicas")
	}
	cfg := jetstream.StreamConfig{Name: s.Name, Subjects: []string{s.Subject}, Storage: jetstream.FileStorage, Retention: jetstream.InterestPolicy, Discard: jetstream.DiscardNew, MaxAge: 0, MaxBytes: 1 << 30, MaxMsgSize: MaxMessage, Duplicates: 120 * time.Second, Replicas: 3}
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
	if c.Storage != cfg.Storage || c.Retention != cfg.Retention || c.Discard != cfg.Discard || c.MaxAge != 0 || c.Replicas != 3 || c.MaxBytes != cfg.MaxBytes || c.MaxMsgSize != cfg.MaxMsgSize || c.MaxMsgs > 0 || c.MaxMsgsPerSubject > 0 || len(c.Subjects) != 1 || c.Subjects[0] != s.Subject || c.Duplicates != cfg.Duplicates {
		return nil, fmt.Errorf("unsafe raw stream configuration: %s", s.Name)
	}
	consumer, err := stream.Consumer(ctx, s.Consumer)
	if err == jetstream.ErrConsumerNotFound {
		consumer, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: s.Consumer, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 128, FilterSubject: s.Subject, MaxRequestBatch: 32, MaxRequestExpires: time.Second})
	}
	if err != nil {
		return nil, err
	}
	ci, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}
	cc := ci.Config
	if cc.Durable != s.Consumer || cc.AckPolicy != jetstream.AckExplicitPolicy || cc.FilterSubject != s.Subject || cc.AckWait != 30*time.Second || cc.MaxDeliver != -1 || cc.MaxAckPending != 128 || cc.MaxRequestBatch != 32 || cc.MaxRequestExpires != time.Second {
		return nil, fmt.Errorf("unsafe raw consumer configuration: %s", s.Consumer)
	}
	return consumer, nil
}
