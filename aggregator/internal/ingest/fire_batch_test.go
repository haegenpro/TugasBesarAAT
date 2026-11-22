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
