package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var errDLQConflict = errors.New("DLQ subject contains a different original message")

// The subject itself is the durable uniqueness key; Nats-Msg-Id is only an
// additional short-window optimization. Each durable has an independent DLQ.
func (a *app) dlqSubject(meta *jetstream.MsgMetadata) string {
	return a.cfg.DLQSubject + "." + a.cfg.Durable + "." + meta.Stream + "." + strconv.FormatUint(meta.Sequence.Stream, 10)
}

func (a *app) verifyStoredDLQ(ctx context.Context, js jetstream.JetStream, subject string, expected deadLetter) error {
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	started := time.Now()
	stream, err := js.Stream(lookupCtx, a.cfg.DLQStream)
	logOutbound(ctx, "dlq_stream_lookup", "nats", 0, started, err)
	if err != nil {
		return fmt.Errorf("lookup DLQ stream: %w", err)
	}
	started = time.Now()
	stored, err := stream.GetLastMsgForSubject(lookupCtx, subject)
	logOutbound(ctx, "dlq_subject_lookup", "nats", 0, started, err)
	if err != nil {
		return fmt.Errorf("lookup DLQ subject: %w", err)
	}
	var actual deadLetter
	if err := json.Unmarshal(stored.Data, &actual); err != nil {
		return fmt.Errorf("%w: invalid stored entry: %v", errDLQConflict, err)
	}
	if actual.Consumer != expected.Consumer || actual.OriginalStream != expected.OriginalStream ||
		actual.OriginalSubject != expected.OriginalSubject || actual.StreamSequence != expected.StreamSequence ||
		messageDigest(actual.Raw) != messageDigest(expected.Raw) {
		return fmt.Errorf("%w: %s", errDLQConflict, subject)
	}
	return nil
}
