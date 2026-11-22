package ingest

import (
	"aggregator/internal/wildfire"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// fireFixture stores a recorded operation stream as raw inputs of a fresh namespace and returns
// them in source order.
func fireFixture(t *testing.T, s *Store, file string) (string, []Input) {
	t.Helper()
	ctx := context.Background()
	root := filepath.Join("..", "..", "..", "docs", "m2", "fixtures", "wildfire-m5")
	provinces, e := os.ReadFile(filepath.Join(root, "provinces.json"))
	require.NoError(t, e)
	ns := "fire:" + uuid.NewString()
	var rows []wildfire.Province
	require.NoError(t, json.Unmarshal(provinces, &rows))
	for _, p := range rows {
		raw, _ := json.Marshal(p)
		_, e = s.Pool.Exec(ctx, `INSERT INTO provinces(namespace,id,payload) VALUES($1,$2,$3)`, ns, p.ID, raw)
		require.NoError(t, e)
	}
	f, e := os.Open(filepath.Join(root, file))
	require.NoError(t, e)
	defer f.Close()
	var inputs []Input
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var op wildfire.Operation
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &op))
		op.Namespace = ns
		raw, _ := json.Marshal(op)
		inputs = append(inputs, Input{ID: uuid.NewString(), Source: "SATELIT", Namespace: ns, Position: op.Offset, Payload: op.Payload, Metadata: map[string]any{"operation": json.RawMessage(raw)}, CorrelationID: fmt.Sprint(op.Offset)})
	}
	require.NoError(t, scanner.Err())
	require.NoError(t, s.Leases.Run(ctx, "source:"+ns, func(c context.Context) error {
		return s.AppendBatch(c, inputs, inputs[0].Position, "raw.satellite")
	}))
	return ns, inputs
}

// outcome is everything a variant of the pipeline made durable, with the namespace removed so
// that two runs over the same stream can be compared byte for byte.
func outcome(t *testing.T, s *Store, ns string) map[string]any {
	t.Helper()
	ctx := context.Background()
	// Fire and batch identities are derived from the namespace, so equal runs in different
	// namespaces are compared with those hashes renamed in order of first appearance.
	hashes := regexp.MustCompile(`(fire|fire-batch)-v1:[0-9a-f]{64}`)
	names := map[string]string{}
	related := regexp.MustCompile(`"related": \[([^\]]*)\]`)
	normalize := func(v string) string {
		v = strings.ReplaceAll(v, ns, "NS")
		v = hashes.ReplaceAllStringFunc(v, func(h string) string {
			if _, ok := names[h]; !ok {
				names[h] = fmt.Sprintf("ID%d", len(names))
			}
			return names[h]
		})
		// The related list is ordered by hash, which depends on the namespace.
		return related.ReplaceAllStringFunc(v, func(m string) string {
			parts := strings.Split(related.FindStringSubmatch(m)[1], ", ")
			sort.Strings(parts)
			return `"related": [` + strings.Join(parts, ", ") + `]`
		})
	}
	text := func(query string) []string {
		rows, e := s.Pool.Query(ctx, query, ns)
		require.NoError(t, e)
		defer rows.Close()
		var out []string
		for rows.Next() {
			var v string
			require.NoError(t, rows.Scan(&v))
			out = append(out, normalize(v))
		}
		require.NoError(t, rows.Err())
		return out
	}
	var received, prepared, applied, singletons, excluded, late int64
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT received,prepared,applied,singletons,excluded,late_mutations FROM source_progress WHERE source='SATELIT' AND namespace=$1`, ns).Scan(&received, &prepared, &applied, &singletons, &excluded, &late))
	// Canonical output is ordered by position, so naming identities from it first is deterministic.
	canonical := []string{}
	crows, e := s.Pool.Query(ctx, `SELECT position,operation_id,payload::text FROM canonical_outbox WHERE source='SATELIT' AND namespace LIKE $1||'%' ORDER BY position`, ns)
	require.NoError(t, e)
	for crows.Next() {
		var position int64
		var operation, payload string
		require.NoError(t, crows.Scan(&position, &operation, &payload))
		// The order of changes inside one batch follows map iteration in the existing resolver and
		// is not part of the contract; compare them by reference.
		var doc map[string]any
		require.NoError(t, json.Unmarshal([]byte(payload), &doc))
		changes, _ := doc["changes"].([]any)
		// Sort by something that does not depend on the namespace: deletions first, then the first detection key.
		key := func(c any) string {
			m := c.(map[string]any)
			if m["delete"] == true {
				return "0"
			}
			event, _ := m["event"].(map[string]any)
			attributes, _ := event["attributes"].(map[string]any)
			keys, _ := attributes["detection_keys"].([]any)
			return fmt.Sprint("1", keys)
		}
		sort.Slice(changes, func(x, y int) bool { return key(changes[x]) < key(changes[y]) })
		normalized, _ := json.Marshal(doc)
		canonical = append(canonical, normalize(fmt.Sprintf("%d %s %s", position, operation, normalized)))
	}
	crows.Close()
	fires := text(`SELECT fire_id||' '||active||' '||payload::text FROM wildfire_fires WHERE namespace=$1 ORDER BY fire_id`)
	sort.Strings(fires)
	return map[string]any{
		"fires":     fires,
		"canonical": canonical,
		"members":   text(`SELECT key||' '||active||' '||coalesce(fire_id,'-')||' '||coalesce(previous_fire_id,'-')||' '||last_position FROM wildfire_observations WHERE namespace=$1 ORDER BY key`),
		"states":    text(`SELECT position||' '||state||' '||reason FROM raw_inputs WHERE namespace=$1 ORDER BY position`),
		"progress":  []int64{received, prepared, singletons, excluded, late},
	}
}

func chunkItems(inputs []Input) []chunkItem {
	items := make([]chunkItem, len(inputs))
	for index, i := range inputs {
		items[index] = chunkItem{input: i, ctx: context.Background()}
	}
	return items
}

// run prepares every input with the given chunk size; 0 uses the one-input-per-transaction path.
func runChunks(t *testing.T, s *Store, ns string, inputs []Input, size int) {
	t.Helper()
	require.NoError(t, s.Leases.Run(context.Background(), "worker:"+ns, func(c context.Context) error {
		if size == 0 {
			for _, i := range inputs {
				if e := s.prepareFire(c, i, 30*time.Minute); e != nil {
					return e
				}
			}
			return nil
		}
		for from := 0; from < len(inputs); from += size {
			done, failed, e := s.prepareChunk(c, chunkItems(inputs[from:min(from+size, len(inputs))]), 30*time.Minute)
			if e != nil {
				return e
			}
			require.Nil(t, failed)
			require.Equal(t, min(size, len(inputs)-from), done)
		}
		return nil
	}))
}

func TestM2BatchedPreparationGivesIdenticalResultsAcrossBatchBoundaries(t *testing.T) {
	s := testStore(t)
	for _, file := range []string{"bridge.jsonl", "mixed-merge-split.jsonl"} {
		var reference map[string]any
		for _, size := range []int{0, 1, 2, 5, 1000} {
			ns, inputs := fireFixture(t, s, file)
			runChunks(t, s, ns, inputs, size)
			got := outcome(t, s, ns)
			if reference == nil {
				reference = got
				require.NotEmpty(t, got["fires"], file)
				continue
			}
			requireSameOutcome(t, reference, got, fmt.Sprintf("%s: batch size %d must give exactly the result of one input per transaction", file, size))
		}
	}
}

func TestM2BatchedPreparationIsolatesAFailureAndCountsAttemptsOncePerTry(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns, inputs := fireFixture(t, s, "mixed-merge-split.jsonl")
	require.GreaterOrEqual(t, len(inputs), 8)
	// Withhold input 4 so that input 5 is a gap: the input fails by itself and is a barrier for the rest.
	chunk := append(append([]Input{}, inputs[:4]...), inputs[5:8]...)
	attempts := func() []int64 {
		rows, e := s.Pool.Query(ctx, `SELECT attempts FROM raw_inputs WHERE namespace=$1 ORDER BY position`, ns)
		require.NoError(t, e)
		defer rows.Close()
		var out []int64
		for rows.Next() {
			var n int64
			require.NoError(t, rows.Scan(&n))
			out = append(out, n)
		}
		return out
	}
	var done int
	var failed *chunkFailure
	require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error {
		var e error
		done, failed, e = s.prepareChunk(c, chunkItems(chunk), 30*time.Minute)
		return e
	}))
	require.Equal(t, 4, done, "the inputs before the failing one stay committed")
	require.NotNil(t, failed)
	require.ErrorIs(t, failed.err, ErrGap)
	require.Equal(t, inputs[5].ID, failed.id)
	received, prepared, _, e := s.Progress(ctx, "SATELIT", ns, inputs[0].Position)
	require.NoError(t, e)
	require.EqualValues(t, inputs[len(inputs)-1].Position, received)
	require.EqualValues(t, inputs[3].Position, prepared, "the prepared frontier stops at the last committed input")
	states := outcome(t, s, ns)["states"].([]string)
	for index, line := range states {
		if index < 4 {
			require.NotContains(t, line, " pending ", "input %d was prepared", index)
		} else {
			require.Contains(t, line, " pending ", "input %d must be untouched, not half applied", index)
		}
	}
	// An attempt is recorded before processing and survives the rollback; it does not mean the input finished.
	got := attempts()
	for index := range inputs {
		switch {
		case index < 4:
			require.EqualValues(t, 1, got[index])
		case index == 4:
			require.EqualValues(t, 0, got[index], "never offered")
		case index <= 7:
			require.EqualValues(t, 1, got[index], "offered, rolled back or not reached, attempt still counted")
		default:
			require.EqualValues(t, 0, got[index])
		}
	}
	// Redelivery of the rest, in order, completes the stream and counts one more attempt for each.
	rest := inputs[4:8]
	require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error {
		d, f, e := s.prepareChunk(c, chunkItems(rest), 30*time.Minute)
		require.Nil(t, f)
		require.Equal(t, 4, d)
		return e
	}))
	got = attempts()
	require.EqualValues(t, 1, got[0])
	require.EqualValues(t, 1, got[4])
	require.EqualValues(t, 2, got[5], "failed once, succeeded on the retry")
	// A redelivered, already prepared input is a harmless no-op.
	require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error {
		d, f, e := s.prepareChunk(c, chunkItems(inputs[:4]), 30*time.Minute)
		require.Nil(t, f)
		require.Equal(t, 4, d)
		return e
	}))
	_, prepared, _, e = s.Progress(ctx, "SATELIT", ns, inputs[0].Position)
	require.NoError(t, e)
	require.EqualValues(t, inputs[7].Position, prepared)
}

func TestM2BatchedPreparationWithoutOwnershipChangesNothing(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns, inputs := fireFixture(t, s, "bridge.jsonl")
	_, _, e := s.prepareChunk(ctx, chunkItems(inputs[:3]), 30*time.Minute)
	require.Error(t, e, "a stale or missing owner must not prepare anything")
	var pending int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM raw_inputs WHERE namespace=$1 AND state<>'pending'`, ns).Scan(&pending))
	require.Zero(t, pending)
}

func TestM2BatchSizeFallsBackToOneAfterAWholeBatchFailureAndRecovers(t *testing.T) {
	b := &fireBatcher{max: 25, size: 25}
	b.adapt(true)
	require.Equal(t, 1, b.size)
	for _, want := range []int{2, 4, 8, 16, 25, 25} {
		b.adapt(false)
		require.Equal(t, want, b.size)
	}
	one := &fireBatcher{max: 1, size: 1}
	one.adapt(false)
	require.Equal(t, 1, one.size, "batch size one stays one")
}

// requireSameOutcome reports the first difference compactly instead of dumping both outcomes.
func requireSameOutcome(t *testing.T, want, got map[string]any, message string) {
	t.Helper()
	for key, w := range want {
		switch expected := w.(type) {
		case []string:
			actual := got[key].([]string)
			require.Equal(t, len(expected), len(actual), "%s: %s row count", message, key)
			for index := range expected {
				if expected[index] != actual[index] {
					at := 0
					for at < len(expected[index]) && at < len(actual[index]) && expected[index][at] == actual[index][at] {
						at++
					}
					from := max(0, at-60)
					t.Fatalf("%s: %s row %d differs at byte %d\n  want ...%s\n  got  ...%s", message, key, index, at,
						expected[index][from:min(len(expected[index]), at+80)], actual[index][from:min(len(actual[index]), at+80)])
				}
			}
		default:
			require.Equal(t, w, got[key], "%s: %s", message, key)
		}
	}
}

func TestM2GapAheadOfTheFrontierIsFilledFromDurableInputsInOrder(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	for _, file := range []string{"bridge.jsonl", "mixed-merge-split.jsonl"} {
		refNS, refInputs := fireFixture(t, s, file)
		runChunks(t, s, refNS, refInputs, 0)
		reference := outcome(t, s, refNS)

		ns, inputs := fireFixture(t, s, file)
		require.GreaterOrEqual(t, len(inputs), 6, file)
		attempts := func() int64 {
			var n int64
			require.NoError(t, s.Pool.QueryRow(ctx, `SELECT sum(attempts) FROM raw_inputs WHERE namespace=$1`, ns).Scan(&n))
			return n
		}
		require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error {
			// The first two arrive, then the broker hands out a later run first (the ones in between
			// are delayed or still pending to an earlier subscription).
			done, failed, e := s.prepareChunkFilled(c, chunkItems(inputs[:2]), 30*time.Minute)
			require.NoError(t, e)
			require.Nil(t, failed)
			require.Equal(t, 2, done)
			done, failed, e = s.prepareChunkFilled(c, chunkItems(inputs[4:]), 30*time.Minute)
			require.NoError(t, e)
			require.Nil(t, failed)
			require.Equal(t, len(inputs)-4, done, "the fetched inputs are prepared right away")
			require.EqualValues(t, len(inputs), attempts(), "filled inputs record their attempt like any other")
			// Their own late deliveries are no-ops.
			done, failed, e = s.prepareChunkFilled(c, chunkItems(inputs[2:4]), 30*time.Minute)
			require.NoError(t, e)
			require.Nil(t, failed)
			require.Equal(t, 2, done)
			return nil
		}))
		requireSameOutcome(t, reference, outcome(t, s, ns), file+": filling a gap from raw_inputs gives exactly the in-order result")
	}
}

func TestM2GapLargerThanTheFillBoundKeepsTheOrderingBarrier(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	t.Setenv("WILDFIRE_GAP_FILL_MAX", "2")
	ns, inputs := fireFixture(t, s, "mixed-merge-split.jsonl")
	require.NoError(t, s.Leases.Run(ctx, "worker:"+ns, func(c context.Context) error {
		done, failed, e := s.prepareChunkFilled(c, chunkItems(inputs[5:7]), 30*time.Minute)
		require.NoError(t, e)
		require.Zero(t, done)
		require.NotNil(t, failed)
		require.ErrorIs(t, failed.err, ErrGap)
		return nil
	}))
	var pending int
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM raw_inputs WHERE namespace=$1 AND state='pending'`, ns).Scan(&pending))
	require.Equal(t, len(inputs), pending, "nothing was prepared out of order")
}

// An append commits while frontiers() waits for the progress row: the re-evaluated row carries the
// new received, but the statement's snapshot does not contain the appended (pending) inputs. The
// frontier must not move past them.
func TestM2FrontiersNeverPassInputsAppendedWhileWaiting(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ns, inputs := fireFixture(t, s, "bridge.jsonl")
	last := inputs[len(inputs)-1].Position
	// Everything stored so far is finished, so there is no pending input before the append.
	_, e := s.Pool.Exec(ctx, `UPDATE raw_inputs SET state='applied' WHERE namespace=$1`, ns)
	require.NoError(t, e)

	appender, e := s.Pool.Begin(ctx)
	require.NoError(t, e)
	defer appender.Rollback(ctx)
	_, e = appender.Exec(ctx, `INSERT INTO raw_inputs(message_id,source,namespace,position,entity_key,payload,metadata,digest,correlation_id) VALUES($1,'SATELIT',$2,$3,'k','\x00','{}','d','c')`, uuid.NewString(), ns, last+1)
	require.NoError(t, e)
	_, e = appender.Exec(ctx, `UPDATE source_progress SET received=$2 WHERE source='SATELIT' AND namespace=$1`, ns, last+1)
	require.NoError(t, e)

	done := make(chan error, 1)
	go func() {
		tx, e := s.Pool.Begin(ctx)
		if e != nil {
			done <- e
			return
		}
		defer tx.Rollback(ctx)
		if e = frontiers(ctx, tx, "SATELIT", ns); e != nil {
			done <- e
			return
		}
		done <- tx.Commit(ctx)
	}()
	// Wait until frontiers() is blocked on the row lock, so its snapshot predates the append.
	require.Eventually(t, func() bool {
		var waiting int
		_ = s.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'UPDATE source_progress p SET%'`).Scan(&waiting)
		return waiting > 0
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, appender.Commit(ctx))
	require.NoError(t, <-done)

	var prepared, applied int64
	require.NoError(t, s.Pool.QueryRow(ctx, `SELECT prepared,applied FROM source_progress WHERE source='SATELIT' AND namespace=$1`, ns).Scan(&prepared, &applied))
	require.LessOrEqual(t, prepared, last, "prepared must not pass the input appended while frontiers waited")
	require.LessOrEqual(t, applied, last)
}
