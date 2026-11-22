package ingest

import (
	"aggregator/internal/avrowire"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// runBatcher drains a consumer through fireBatcher.step until the stream and the unacknowledged set are empty.
func runBatcher(t *testing.T, s *Store, js jetstream.JetStream, registry *avrowire.Client, b *fireBatcher, ns string, ackWait time.Duration) {
	t.Helper()
	ctx := context.Background()
	consumer, e := js.CreateOrUpdateConsumer(ctx, testStream, jetstream.ConsumerConfig{Durable: "wildfire-test", FilterSubject: "raw.satellite",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait, MaxAckPending: b.max})
	require.NoError(t, e)
	deadline := time.Now().Add(20 * time.Second)
	require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error {
		for time.Now().Before(deadline) {
			if e := b.step(c, consumer, registry, 30*time.Minute); e != nil {
				return e
			}
			info, e := consumer.Info(ctx)
			require.NoError(t, e)
			if info.NumPending == 0 && info.NumAckPending == 0 {
				return nil
			}
		}
		t.Fatal("consumer did not drain")
		return nil
	}))
}

func publishFixture(t *testing.T, s *Store, js jetstream.JetStream, registry *avrowire.Client) {
	t.Helper()
	require.NoError(t, s.Leases.Run(context.Background(), "raw-outbox", func(c context.Context) error {
		for {
			n, e := s.PublishOnce(c, js, registry, PublishConfig{Batch: 50})
			if e != nil || n == 0 {
				return e
			}
		}
	}))
}

func controlOutcome(t *testing.T, s *Store) map[string]any {
	ns, inputs := fireFixture(t, s, "mixed-merge-split.jsonl")
	_, e := s.Pool.Exec(context.Background(), `DELETE FROM raw_outbox WHERE message_id IN (SELECT message_id FROM raw_inputs WHERE namespace=$1)`, ns)
	require.NoError(t, e)
	runChunks(t, s, ns, inputs, 1)
	return outcome(t, s, ns)
}

func TestM2FireBatcherWithConcurrentConfirmationEqualsOneAtATime(t *testing.T) {
	s := testStore(t)
	js, _ := brokerFixture(t, 2*time.Minute)
	registry := registryFor(t, s)
	want := controlOutcome(t, s)
	ns, inputs := fireFixture(t, s, "mixed-merge-split.jsonl")
	publishFixture(t, s, js, registry)
	b := &fireBatcher{s: s, max: 4, size: 4, ackWorkers: 4}
	runBatcher(t, s, js, registry, b, ns, 5*time.Second)
	requireSameOutcome(t, want, outcome(t, s, ns), "concurrent confirmation")
	var attempts int64
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT max(attempts) FROM raw_inputs WHERE namespace=$1`, ns).Scan(&attempts))
	require.EqualValues(t, 1, attempts, "a clean run records one attempt per input")
	_ = inputs
}

func TestM2FailedConfirmationRedeliversWithoutRepeatingBusinessEffects(t *testing.T) {
	s := testStore(t)
	js, _ := brokerFixture(t, 2*time.Minute)
	registry := registryFor(t, s)
	want := controlOutcome(t, s)
	ns, inputs := fireFixture(t, s, "mixed-merge-split.jsonl")
	publishFixture(t, s, js, registry)
	var lost atomic.Int32
	b := &fireBatcher{s: s, max: 4, size: 4, ackWorkers: 3}
	b.confirm = func(msg jetstream.Msg) error {
		if lost.Add(1) <= 3 { // the first three confirmations never reach the broker
			return errors.New("confirmation lost")
		}
		return msg.DoubleAck(context.Background())
	}
	runBatcher(t, s, js, registry, b, ns, time.Second)
	require.GreaterOrEqual(t, int(lost.Load()), 4, "the unconfirmed messages came back")
	requireSameOutcome(t, want, outcome(t, s, ns), "redelivery after lost confirmations")
	var canonical int
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM canonical_outbox WHERE namespace LIKE $1||'%'`, ns).Scan(&canonical))
	var controlOutputs int
	for _, line := range want["canonical"].([]string) {
		if line != "" {
			controlOutputs++
		}
	}
	require.Equal(t, controlOutputs, canonical, "no canonical operation was produced twice")
	var maxAttempts int64
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT max(attempts) FROM raw_inputs WHERE namespace=$1`, ns).Scan(&maxAttempts))
	require.EqualValues(t, 2, maxAttempts, "a redelivered input counts another attempt but changes nothing else")
	_ = inputs
}
