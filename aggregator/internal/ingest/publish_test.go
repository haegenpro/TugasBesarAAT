package ingest

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/ownership"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

const testStream = "RAWTEST"

// brokerFixture gives a fresh in-memory stream over the raw subjects. The duplicate window is a
// parameter so the beyond-the-window behavior can be tested without waiting minutes.
func brokerFixture(t *testing.T, duplicates time.Duration) (jetstream.JetStream, jetstream.Stream) {
	t.Helper()
	url := os.Getenv("M2_TEST_NATS_URL")
	if url == "" {
		t.Skip("M2_TEST_NATS_URL not set")
	}
	nc, e := nats.Connect(url, nats.Timeout(2*time.Second))
	require.NoError(t, e)
	js, e := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(512))
	require.NoError(t, e)
	ctx := context.Background()
	_ = js.DeleteStream(ctx, testStream)
	stream, e := js.CreateStream(ctx, jetstream.StreamConfig{Name: testStream, Subjects: []string{"raw.>"}, Storage: jetstream.MemoryStorage, Duplicates: duplicates})
	require.NoError(t, e)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), testStream); nc.Close() })
	return js, stream
}

func registryFor(t *testing.T, s *Store) *avrowire.Client {
	t.Helper()
	_, e := s.Pool.Exec(context.Background(), `INSERT INTO schema_cache(id,schema) VALUES(17,$1) ON CONFLICT DO NOTHING`, RawSchema)
	require.NoError(t, e)
	r := avrowire.NewClient("http://127.0.0.1:1")
	r.SetCache(s)
	return r
}

// seed stores n records of a source in an own namespace and queues them on subject.
func seed(t *testing.T, s *Store, source, subject string, n int) []Input {
	t.Helper()
	ns := "pub:" + uuid.NewString()
	var inputs []Input
	for p := 0; p < n; p++ {
		inputs = append(inputs, Input{Source: source, Namespace: ns, Position: int64(p), Payload: []byte(fmt.Sprintf(`{"p":%d}`, p)), Metadata: map[string]any{}, CorrelationID: "c"})
	}
	require.NoError(t, s.Leases.Run(context.Background(), "acquire:"+ns, func(c context.Context) error { return s.AppendBatch(c, inputs, 0, subject) }))
	return inputs
}

func outboxCount(t *testing.T, s *Store) (total, claimed int) {
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT count(*),count(*) FILTER (WHERE claim_holder IS NOT NULL) FROM raw_outbox`).Scan(&total, &claimed))
	return
}

// published lists, in stream order, the message ids the broker accepted.
func published(t *testing.T, stream jetstream.Stream) []string {
	ctx := context.Background()
	info, e := stream.Info(ctx)
	require.NoError(t, e)
	var out []string
	for seq := info.State.FirstSeq; seq != 0 && seq <= info.State.LastSeq; seq++ {
		msg, e := stream.GetMsg(ctx, seq)
		if e != nil {
			continue
		}
		out = append(out, msg.Header.Get("Nats-Msg-Id"))
	}
	return out
}

func owner(t *testing.T, s *Store, fn func(context.Context)) {
	t.Helper()
	require.NoError(t, s.Leases.Run(context.Background(), "raw-outbox", func(c context.Context) error { fn(c); return nil }))
}

// otherOwner is a second process incarnation sharing the same database.
func otherOwner(s *Store) *Store { return &Store{Pool: s.Pool, Leases: ownership.New(s.Pool)} }

func expireLease(t *testing.T, s *Store) {
	_, e := s.Pool.Exec(context.Background(), `UPDATE source_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE scope='raw-outbox'`)
	require.NoError(t, e)
}

func TestM2PublicationIsOrderedBatchedAndCompletesOnlyAcknowledgedEntries(t *testing.T) {
	s := testStore(t)
	js, stream := brokerFixture(t, 2*time.Minute)
	registry := registryFor(t, s)
	inputs := seed(t, s, "SATELIT", "raw.satellite", 30)
	owner(t, s, func(c context.Context) {
		for cycle := 0; cycle < 3; cycle++ {
			n, e := s.PublishOnce(c, js, registry, PublishConfig{Batch: 10})
			require.NoError(t, e)
			require.Equal(t, 10, n)
		}
		n, e := s.PublishOnce(c, js, registry, PublishConfig{Batch: 10})
		require.NoError(t, e)
		require.Zero(t, n)
	})
	ids := published(t, stream)
	require.Len(t, ids, 30)
	for index, i := range inputs {
		stored, e := s.Input(context.Background(), ids[index][:36])
		require.NoError(t, e)
		require.Equal(t, i.Position, stored.Position, "published in source order")
		require.Equal(t, ids[index], stored.ID+":0", "stable identity: record id plus replay generation")
	}
	total, _ := outboxCount(t, s)
	require.Zero(t, total)
}

func TestM2CriticalAndFieldEntriesAreClaimedAheadOfBulkAndLanesAreSeparate(t *testing.T) {
	s := testStore(t)
	seed(t, s, "SATELIT", "raw.satellite", 20)
	seed(t, s, "FIELD_REPORT", "raw.field", 2)
	seed(t, s, "SENSOR_PANTAI", "raw.coastal.critical", 3)
	seed(t, s, "SENSOR_PANTAI", "raw.coastal.normal", 4)
	owner(t, s, func(c context.Context) {
		reserved, e := s.claimOutbox(c, 100, time.Minute, 0, 1)
		require.NoError(t, e)
		require.Len(t, reserved, 5)
		require.Equal(t, "raw.coastal.critical", reserved[0].Subject, "critical coastal first")
		require.Equal(t, "raw.field", reserved[3].Subject)
		bulk, e := s.claimOutbox(c, 100, time.Minute, 2, 3)
		require.NoError(t, e)
		require.Len(t, bulk, 24)
		require.Equal(t, "raw.coastal.normal", bulk[0].Subject)
		for k := 4; k < len(bulk); k++ {
			require.Equal(t, int64(k-4), bulk[k].Position, "satellite claims keep source order")
		}
		more, e := s.claimOutbox(c, 100, time.Minute, 0, 3)
		require.NoError(t, e)
		require.Empty(t, more, "claimed entries are exclusive while the claim lives")
	})
}

func TestM2ClaimExpiresAndAnUnpublishedBatchIsClaimableAgainAfterACrash(t *testing.T) {
	s := testStore(t)
	seed(t, s, "SATELIT", "raw.satellite", 12)
	owner(t, s, func(c context.Context) {
		first, e := s.claimOutbox(c, 12, 250*time.Millisecond, 0, 3) // then the process "crashes": no publish, no completion
		require.NoError(t, e)
		require.Len(t, first, 12)
		again, e := s.claimOutbox(c, 12, 250*time.Millisecond, 0, 3)
		require.NoError(t, e)
		require.Empty(t, again)
		time.Sleep(400 * time.Millisecond)
		retry, e := s.claimOutbox(c, 12, time.Minute, 0, 3)
		require.NoError(t, e)
		require.Len(t, retry, 12, "an expired claim is recovered by the same owner")
	})
}

func TestM2OwnershipChangeFreesTheOldOwnersClaimsAndFencesItsCompletion(t *testing.T) {
	s := testStore(t)
	js, stream := brokerFixture(t, 2*time.Minute)
	registry := registryFor(t, s)
	seed(t, s, "SATELIT", "raw.satellite", 10)
	b := otherOwner(s)

	var claims []outboxClaim
	var acked []bool
	oldOwner := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.Leases.Run(context.Background(), "raw-outbox", func(c context.Context) error {
			var e error
			claims, e = s.claimOutbox(c, 10, time.Hour, 0, 3)
			if e != nil {
				return e
			}
			acked, e = s.publishClaims(c, js, registry, claims, 5*time.Second)
			if e != nil {
				return e
			}
			close(oldOwner)
			<-finish // the old owner stalls (pause, partition) after publishing, before completing
			_, e = s.completeOutbox(c, claims, acked)
			return e
		})
	}()
	<-oldOwner
	require.Len(t, published(t, stream), 10)
	expireLease(t, s)
	owner2 := func(c context.Context) {
		// The new owner sees the old owner's one-hour claims as free at once and republishes
		// with the same identities; the broker drops them as duplicates.
		n, e := b.PublishOnce(c, js, registry, PublishConfig{Batch: 100})
		require.NoError(t, e)
		require.Equal(t, 10, n)
	}
	require.NoError(t, b.Leases.Run(context.Background(), "raw-outbox", func(c context.Context) error { owner2(c); return nil }))
	require.Len(t, published(t, stream), 10, "stale-owner publication plus the new owner's repeat is still ten messages")
	total, _ := outboxCount(t, s)
	require.Zero(t, total)
	close(finish)
	select {
	case e := <-done:
		require.Error(t, e, "the stalled owner's late completion is fenced")
	case <-time.After(5 * time.Second):
		t.Fatal("old owner did not finish")
	}
}

func TestM2ReplayDuringAClaimIsNotLostWhenTheOldGenerationCompletes(t *testing.T) {
	s := testStore(t)
	js, stream := brokerFixture(t, 2*time.Minute)
	registry := registryFor(t, s)
	inputs := seed(t, s, "SATELIT", "raw.satellite", 3)
	id := ""
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT message_id::text FROM raw_inputs WHERE namespace=$1 AND position=1`, inputs[0].Namespace).Scan(&id))
	owner(t, s, func(c context.Context) {
		claims, e := s.claimOutbox(c, 10, time.Minute, 0, 3)
		require.NoError(t, e)
		require.Len(t, claims, 3)
		acked, e := s.publishClaims(c, js, registry, claims, 5*time.Second)
		require.NoError(t, e)
		// While the batch is in flight an operator replays input 1 (it was parked): the generation moves on.
		_, e = s.Pool.Exec(context.Background(), `UPDATE raw_inputs SET state='parked' WHERE message_id=$1`, id)
		require.NoError(t, e)
		count, e := s.RequestReplay(c, []string{id})
		require.NoError(t, e)
		require.EqualValues(t, 1, count)
		completed, e := s.completeOutbox(c, claims, acked)
		require.NoError(t, e)
		require.Equal(t, 2, completed, "the replayed entry is not completed by the old generation")
		total, _ := outboxCount(t, s)
		require.Equal(t, 1, total, "the replayed entry stays queued")
		n, e := s.PublishOnce(c, js, registry, PublishConfig{Batch: 10})
		require.NoError(t, e)
		require.Equal(t, 1, n)
	})
	ids := published(t, stream)
	require.Len(t, ids, 4)
	require.Contains(t, ids, id+":0")
	require.Contains(t, ids, id+":1", "the new generation has its own identity, so the broker does not drop it")
	total, _ := outboxCount(t, s)
	require.Zero(t, total)
}

func TestM2UnacknowledgedPublicationKeepsEntriesAndOnlyThePrefixIsCompleted(t *testing.T) {
	s := testStore(t)
	seed(t, s, "SATELIT", "raw.satellite", 6)
	owner(t, s, func(c context.Context) {
		claims, e := s.claimOutbox(c, 6, time.Hour, 0, 3)
		require.NoError(t, e)
		// The broker acknowledged entries 0, 1 and 3 but not 2: order says only 0 and 1 may be completed.
		completed, e := s.completeOutbox(c, claims, []bool{true, true, false, true, false, false})
		require.NoError(t, e)
		require.Equal(t, 2, completed)
		total, claimed := outboxCount(t, s)
		require.Equal(t, 4, total)
		require.Zero(t, claimed, "the rest are released for immediate retry, not left waiting for the claim to expire")
		again, e := s.claimOutbox(c, 10, time.Minute, 0, 3)
		require.NoError(t, e)
		require.Len(t, again, 4)
		require.Equal(t, int64(2), again[0].Position, "retry resumes at the first unacknowledged entry")
	})
}

func TestM2PublishFailureLeavesEntriesQueuedAndReleased(t *testing.T) {
	s := testStore(t)
	js, _ := brokerFixture(t, 2*time.Minute)
	registry := registryFor(t, s)
	seed(t, s, "SATELIT", "nostream.satellite", 4) // no stream captures this subject: no acknowledgement
	owner(t, s, func(c context.Context) {
		n, e := s.PublishOnce(c, js, registry, PublishConfig{Batch: 10, AckTimeout: 2 * time.Second})
		require.Error(t, e)
		require.Equal(t, 4, n)
		total, claimed := outboxCount(t, s)
		require.Equal(t, 4, total)
		require.Zero(t, claimed)
	})
}

func TestM2RepeatBeyondTheBrokerWindowIsABusinessNoOp(t *testing.T) {
	s := testStore(t)
	js, stream := brokerFixture(t, 200*time.Millisecond)
	registry := registryFor(t, s)
	controlNS, control := fireFixture(t, s, "mixed-merge-split.jsonl")
	_, e := s.Pool.Exec(context.Background(), `DELETE FROM raw_outbox WHERE message_id IN (SELECT message_id FROM raw_inputs WHERE namespace=$1)`, controlNS)
	require.NoError(t, e)
	runChunks(t, s, controlNS, control, 1)
	want := outcome(t, s, controlNS)

	ns, inputs := fireFixture(t, s, "mixed-merge-split.jsonl")
	publishAll := func() {
		owner(t, s, func(c context.Context) {
			for {
				n, e := s.PublishOnce(c, js, registry, PublishConfig{Batch: 4})
				require.NoError(t, e)
				if n == 0 {
					return
				}
			}
		})
	}
	publishAll()
	time.Sleep(700 * time.Millisecond) // the broker has forgotten these identities
	_, e = s.Pool.Exec(context.Background(), `INSERT INTO raw_outbox(message_id,subject) SELECT message_id,'raw.satellite' FROM raw_inputs WHERE namespace=$1`, ns)
	require.NoError(t, e)
	publishAll()
	ids := published(t, stream)
	require.Len(t, ids, 2*len(inputs), "past the duplicate window the broker keeps both copies")

	ctx := context.Background()
	consumer, e := js.CreateOrUpdateConsumer(ctx, testStream, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: "raw.satellite"})
	require.NoError(t, e)
	batch, e := consumer.Fetch(len(ids), jetstream.FetchMaxWait(3*time.Second))
	require.NoError(t, e)
	handled := 0
	require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error {
		for msg := range batch.Messages() {
			decoded, e := decodeInput(c, registry, msg.Data())
			require.NoError(t, e)
			loaded, e := s.Inputs(c, []string{decoded.ID})
			require.NoError(t, e)
			if loaded[decoded.ID].Namespace != ns {
				continue // the control run's entries are not part of this comparison
			}
			done, failed, e := s.prepareChunk(c, chunkItems([]Input{loaded[decoded.ID]}), 30*time.Minute)
			require.NoError(t, e)
			require.Nil(t, failed)
			require.Equal(t, 1, done)
			handled++
			require.NoError(t, msg.Ack())
		}
		return nil
	}))
	require.Equal(t, 2*len(inputs), handled)
	requireSameOutcome(t, want, outcome(t, s, ns), "processing every record twice must equal processing it once")
}

func TestM2OutboxOrderingColumnsAreFilledFromTheRawInput(t *testing.T) {
	s := testStore(t)
	inputs := seed(t, s, "SENSOR_PANTAI", "raw.coastal.critical", 2)
	var priority int
	var position int64
	var receivedSet bool
	require.NoError(t, s.Pool.QueryRow(context.Background(), `SELECT o.priority,o.position,o.received_at IS NOT NULL FROM raw_outbox o JOIN raw_inputs i USING(message_id) WHERE i.namespace=$1 AND i.position=1`, inputs[0].Namespace).Scan(&priority, &position, &receivedSet))
	require.Equal(t, 0, priority)
	require.EqualValues(t, 1, position)
	require.True(t, receivedSet)
}
