package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/migrations"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestM6HardeningEndToEnd(t *testing.T) {
	l := databaseLab(t, false)
	config := l.e.Config
	config.PageRows = 32
	config.CacheBytes = 1 << 20
	config.PreparedBytes = 128 << 10
	var err error
	l.e, err = New(l.s, config)
	require.NoError(t, err)
	m := oracle(t, config)
	base := *fixtureOperations(t, "bridge")[0].Detection
	for i := 0; i < 32; i++ {
		d := base
		d.Satellite = "single"
		d.Longitude = fmt.Sprintf("%.6f", float64(i)*.007)
		op := wildfire.Operation{Version: 1, Namespace: config.Namespace, Offset: int64(i + 1), Kind: "detection", Detection: &d, Payload: []byte{byte(i)}}
		l.append(t, op)
		_, err = m.Apply(op)
		require.NoError(t, err)
		l.prepare(t, op.Offset)
		l.deliver(t, op.Offset)
		compareOracle(t, l, m)
	}
	t.Log("32 ordered mutations matched oracle version 2 without retaining the affected transition")
}

func TestM6HardeningFixtures(t *testing.T) {
	l := databaseLab(t, true)
	m := oracle(t, l.e.Config)
	for _, op := range fixtureOperations(t, "mixed-merge-split") {
		applyBoth(t, l, m, op)
	}
	TestM6SharedHistoricalRestorationFixtures(t)
}

func TestM6HardeningPartialSeal(t *testing.T) {
	l := databaseLab(t, false)
	ctx := context.Background()
	m := oracle(t, l.e.Config)
	for _, op := range fixtureOperations(t, "bridge") {
		l.append(t, op)
		_, err := m.Apply(op)
		require.NoError(t, err)
		l.prepare(t, op.Offset)
		var count int64
		require.NoError(t, l.c.QueryRow(ctx, "SELECT output_count FROM wildfire_jobs WHERE namespace=$1 AND position=$2", l.e.Config.Namespace, op.Offset).Scan(&count))
		if count < 2 {
			l.deliver(t, op.Offset)
			continue
		}
		_, err = l.e.StepDelivery(ctx, l.p, l.d)
		require.NoError(t, err)
		var sealed bool
		var through int64
		var prefix []byte
		require.NoError(t, l.c.QueryRow(ctx, "SELECT sealed,seal_count FROM wildfire_jobs WHERE namespace=$1 AND position=$2", l.e.Config.Namespace, op.Offset).Scan(&sealed, &through))
		require.False(t, sealed)
		require.Equal(t, int64(1), through)
		require.NoError(t, l.c.QueryRow(ctx, "SELECT payload FROM wildfire_output_chunks WHERE namespace=$1 AND position=$2 AND ordinal=0", l.e.Config.Namespace, op.Offset).Scan(&prefix))
		l.e, err = New(l.s, l.e.Config)
		require.NoError(t, err)
		l.e.Fault = func(_ context.Context, point string) error {
			if point == "before_seal_commit" {
				return context.Canceled
			}
			return nil
		}
		_, err = l.e.StepDelivery(ctx, l.p, l.d)
		require.Error(t, err)
		var after int64
		require.NoError(t, l.c.QueryRow(ctx, "SELECT seal_count FROM wildfire_jobs WHERE namespace=$1 AND position=$2", l.e.Config.Namespace, op.Offset).Scan(&after))
		require.Equal(t, through, after)
		l.e.Fault = nil
		l.deliver(t, op.Offset)
		var retained []byte
		require.NoError(t, l.c.QueryRow(ctx, "SELECT payload FROM wildfire_output_chunks WHERE namespace=$1 AND position=$2 AND ordinal=0", l.e.Config.Namespace, op.Offset).Scan(&retained))
		require.Equal(t, prefix, retained)
		compareOracle(t, l, m)
		return
	}
	t.Fatal("fixture never exercised a multi-intent partial seal")
}

// Seeds a documented finalization boundary, not an ingestion/throughput result.
func seedFinalization(t *testing.T, l *lab, n int) (map[string]wildfire.Fire, Config) {
	t.Helper()
	ctx := context.Background()
	config := l.e.Config
	config.PageRows = 32
	config.CacheBytes = 1 << 20
	config.PreparedBytes = 128 << 10
	config.StagingBytes = 64 << 20
	var err error
	l.e, err = New(l.s, config)
	require.NoError(t, err)
	obs := map[string]wildfire.Observation{}
	keys := []string{}
	base := *fixtureOperations(t, "bridge")[0].Detection
	for i := 0; i < n; i++ {
		d := base
		d.Satellite = "single"
		d.Latitude = "0"
		d.Longitude = fmt.Sprintf("%.6f", float64(i)*.007)
		o, err := wildfire.ParseDetection(d, int64(i+1), bytesFixture(i, 8<<10))
		require.NoError(t, err)
		obs[o.Key] = o
		keys = append(keys, o.Key)
	}
	sort.Strings(keys)
	oldKeys := append([]string(nil), keys[:len(keys)-1]...)
	oldFires, _, diagnostics := wildfire.ResolveRegion(config.Namespace, int64(n-1), [][]string{oldKeys}, obs, map[string]wildfire.Fire{}, map[string]string{}, l.e.provinces)
	require.Empty(t, diagnostics)
	// Pin an independently derived pre-existing identity born at input 2.
	var prior wildfire.Fire
	for _, f := range oldFires {
		prior = f
	}
	prior.Derivation.BirthOffset = 2
	prior.ID, err = wildfire.Identity(prior.Derivation)
	require.NoError(t, err)
	prior.LogicalRevision = int64(n - 2)
	prior.Related = []string{prior.ID}
	old := map[string]wildfire.Fire{prior.ID: prior}
	expected, _, diagnostics := wildfire.ResolveRegion(config.Namespace, int64(n), [][]string{keys}, obs, old, map[string]string{}, l.e.provinces)
	require.Empty(t, diagnostics)
	topology, err := l.s.ProcessorStorageBoundary(ctx, "")
	require.NoError(t, err)
	tx, err := l.e.begin(ctx, l.p)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	previous, current := job{Position: int64(n - 1)}, job{Position: int64(n)}
	for key, o := range obs {
		state := &previous
		if key == keys[len(keys)-1] {
			state = &current
		}
		require.NoError(t, l.e.put(ctx, tx, state, "observation", key, o, o.EventTime, true, ""))
		require.NoError(t, l.e.enqueue(ctx, tx, &current, key, keys[0]))
	}
	require.NoError(t, l.e.put(ctx, tx, &previous, "fire", prior.ID, prior, nil, true, ""))
	_, err = tx.Exec(ctx, `UPDATE wildfire_work SET done=true WHERE namespace=$1 AND position=$2`, config.Namespace, n)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO input_sessions VALUES('SATELIT',$1,1,$2,$3,$3)`, config.Namespace, n, n-1)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO wildfire_runs(namespace,config_sha256,prepared_through,applied_through) VALUES($1,$2,$3,$3)`, config.Namespace, l.e.fingerprint, n-1)
	require.NoError(t, err)
	last := obs[keys[len(keys)-1]]
	op := wildfire.Operation{Version: 1, Namespace: config.Namespace, Offset: int64(n), Kind: "detection", Detection: last.Detection, Payload: last.Payload}
	env := envelope(t, op)
	eb, _ := json.Marshal(env)
	payload, _ := json.Marshal(op)
	_, err = tx.Exec(ctx, `INSERT INTO input_history(route_key,source,namespace,position,message_id,fingerprint,envelope,original_payload) VALUES(input_route('SATELIT',$1,$2),'SATELIT',$1,$2,$3,$4,$5,$6)`, config.Namespace, n, env.MessageID, rawinput.Fingerprint(env), eb, payload)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO wildfire_jobs(namespace,position,phase,checkpoint,topology,record_count,staged_bytes,batch_id,finalization_version) VALUES($1,$2,'resolve','{}',$3,$4,$5,$6,2)`, config.Namespace, n, topology, current.Records, current.Bytes, wildfire.BatchIdentity(config.Namespace, int64(n)))
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	return expected, config
}
func bytesFixture(i, n int) []byte {
	out := make([]byte, n)
	for k := range out {
		out[k] = byte((i + k) % 251)
	}
	return out
}

func TestM6HardeningCheckpointRestart(t *testing.T) {
	l := databaseLab(t, true)
	expected, config := seedFinalization(t, &l, 32)
	ctx := context.Background()
	var checkpointBytes []byte
	for steps := 0; ; steps++ {
		require.Less(t, steps, 200)
		require.NoError(t, l.c.QueryRow(ctx, `SELECT checkpoint FROM wildfire_jobs WHERE namespace=$1 AND position=32`, config.Namespace).Scan(&checkpointBytes))
		var cp checkpoint
		require.NoError(t, json.Unmarshal(checkpointBytes, &cp))
		if cp.Final != nil && cp.Final.Phase == "construct" && cp.Final.Member != "" {
			break
		}
		_, err := l.e.Step(ctx, l.p)
		require.NoError(t, err)
	}
	// A committed membership page exists, but activation has not advanced.
	var prepared int64
	require.NoError(t, l.c.QueryRow(ctx, `SELECT prepared_through FROM wildfire_runs WHERE namespace=$1`, config.Namespace).Scan(&prepared))
	require.Equal(t, int64(31), prepared)
	l.e.Fault = func(_ context.Context, point string) error {
		if point == "before_preparation_commit" {
			return context.Canceled
		}
		return nil
	}
	_, err := l.e.Step(ctx, l.p)
	require.ErrorIs(t, err, context.Canceled)
	var retained []byte
	require.NoError(t, l.c.QueryRow(ctx, `SELECT checkpoint FROM wildfire_jobs WHERE namespace=$1 AND position=32`, config.Namespace).Scan(&retained))
	require.Equal(t, checkpointBytes, retained)
	l.e, err = New(l.s, config)
	require.NoError(t, err)
	l.prepare(t, 32)
	got := l.fires(t)
	require.Len(t, got, len(expected))
	for _, fire := range got {
		require.Equal(t, expected[fire.ID], fire)
	}
}

type memoryEvidence struct {
	Size           int
	Peak, Baseline uint64
	Working        int64
	Steps          int
	DurationMS     int64
	Retries        int
}

func TestM6HardeningMemoryHelper(t *testing.T) {
	dsn := os.Getenv("WF_HARDENING_DSN")
	if dsn == "" {
		t.Skip("subprocess only")
	}
	config := fixtureConfig(t)
	require.NoError(t, json.Unmarshal([]byte(os.Getenv("WF_HARDENING_CONFIG")), &config))
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s, err := store.New(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	defer s.Close()
	s.Canonical.Durability = localDurability{s: s}
	p, err := ownership.NewProcessor(s.Pool, 15*time.Second)
	require.NoError(t, err)
	s.Processor = p
	lease, err := p.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "memory-child"})
	require.NoError(t, err)
	renew, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-renew.Done():
				return
			case <-ticker.C:
				_, _ = p.Renew(renew, lease.Ownership)
			}
		}
	}()
	e, err := New(s, config)
	require.NoError(t, err)
	require.NoError(t, e.Check(ctx))
	baseline, err := peakResidentBytes()
	require.NoError(t, err)
	var target int
	_, err = fmt.Sscan(os.Getenv("WF_HARDENING_SIZE"), &target)
	require.NoError(t, err)
	started := time.Now()
	steps, retries := 0, 0
	for {
		var through int
		require.NoError(t, s.Pool.QueryRow(ctx, "SELECT prepared_through FROM wildfire_runs WHERE namespace=$1", config.Namespace).Scan(&through))
		if through == target {
			break
		}
		_, err = e.Step(ctx, lease.Ownership)
		if err != nil {
			if !retryableFixtureError(err) || ctx.Err() != nil {
				require.NoError(t, err)
			}
			retries++
		}
		steps++
		require.Less(t, steps, 3000)
	}
	peak, err := peakResidentBytes()
	require.NoError(t, err)
	report := memoryEvidence{Size: target, Peak: peak, Baseline: baseline, Working: e.workingPeak.Load(), Steps: steps, DurationMS: time.Since(started).Milliseconds(), Retries: retries}
	require.LessOrEqual(t, report.Working, config.CacheBytes)
	raw, _ := json.Marshal(report)
	fmt.Println("HARDENING_MEMORY " + string(raw))
}

func TestM6HardeningScale(t *testing.T) {
	if os.Getenv("M2_DATABASE_URL") == "" || os.Getenv("M2_WORKER1_URL") == "" {
		t.Skip("owned PostgreSQL/Citus scale lab absent")
	}
	var small, large uint64
	for _, n := range []int{32, 128, 512} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			l := databaseLab(t, n != 32)
			expected, config := seedFinalization(t, &l, n)
			require.NoError(t, l.processor.Release(context.Background(), l.p))
			executable, err := os.Executable()
			require.NoError(t, err)
			raw, _ := json.Marshal(config)
			ctx, cancel := context.WithTimeout(context.Background(), 135*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable, "-test.run=^TestM6HardeningMemoryHelper$", "-test.timeout=130s")
			command.Env = append(os.Environ(), "WF_HARDENING_DSN="+l.s.Pool.Config().ConnString(), "WF_HARDENING_CONFIG="+string(raw), fmt.Sprintf("WF_HARDENING_SIZE=%d", n))
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
			var report memoryEvidence
			for _, line := range strings.Split(string(output), "\n") {
				if strings.HasPrefix(line, "HARDENING_MEMORY ") {
					require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "HARDENING_MEMORY ")), &report))
				}
			}
			require.Equal(t, n, report.Size)
			require.LessOrEqual(t, report.Working, int64(1<<20))
			if n == 128 {
				small = report.Peak
			}
			if n == 512 {
				large = report.Peak
			}
			got := l.fires(t)
			require.Len(t, got, len(expected))
			for _, fire := range got {
				require.Equal(t, expected[fire.ID], fire)
			}
			if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
				b, _ := json.MarshalIndent(report, "", "  ")
				require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("finalization-%d.json", n)), b, 0600))
			}
		})
	}
	require.NotZero(t, small)
	require.LessOrEqual(t, large, small+(32<<20), "processor child excludes parent fixture/oracle memory")
}

func TestM6HardeningUpgrade(t *testing.T) {
	for _, citus := range []bool{false, true} {
		t.Run(fmt.Sprint(citus), func(t *testing.T) {
			l := databaseLab(t, citus)
			ctx := context.Background()
			// Reconstruct the exact pre-0011 shape inside this isolated empty fixture.
			_, err := l.c.Exec(ctx, `DROP TABLE wildfire_finalize,wildfire_lineage,wildfire_memberships,wildfire_fire_versions; ALTER TABLE wildfire_jobs DROP COLUMN finalization_version,DROP COLUMN seal_after,DROP COLUMN seal_count,DROP COLUMN seal_hash; DELETE FROM canonical_schema_migrations WHERE version='0011_wildfire_finalization.sql'`)
			require.NoError(t, err)
			f := wildfire.Fire{Source: domain.SourceSatelit, ID: "fixture-retained-fire", Derivation: wildfire.Derivation{Namespace: l.e.Config.Namespace, EarliestKey: "a", BirthOffset: 1}, Members: []string{"a", "b"}, Related: []string{"fixture-retained-fire"}, Active: true, LogicalRevision: 7}
			body, _ := json.Marshal(f)
			_, err = l.c.Exec(ctx, `INSERT INTO wildfire_state VALUES(wildfire_route($1,'fire',$2),$1,'fire',$2,2,$3,$4,NULL,true,'')`, l.e.Config.Namespace, f.ID, body, rawinput.Hash(body))
			require.NoError(t, err)
			sealed := []byte(`[{"operation_id":"original","expected_revision":7}]`)
			_, err = l.c.Exec(ctx, `INSERT INTO wildfire_jobs(namespace,position,phase,topology,batch_id,sealed,output_count,output_sha256,output_bytes,output_chunks,seal_cursor) VALUES($1,2,'active','original','original-batch',true,1,$2,$3,1,1)`, l.e.Config.Namespace, rawinput.Hash(sealed), len(sealed))
			require.NoError(t, err)
			_, err = l.c.Exec(ctx, `INSERT INTO wildfire_output_chunks VALUES(wildfire_route($1,'output','2:0'),$1,2,0,$2,$3)`, l.e.Config.Namespace, sealed, rawinput.Hash(sealed))
			require.NoError(t, err)
			target := "postgres"
			if citus {
				target = "citus"
			}
			require.NoError(t, migrations.Up(ctx, l.c, filepath.Join("..", "..", "..", "store/migrations"), target, ""))
			m, err := l.e.fireMeta(ctx, l.c, f.ID, 2)
			require.NoError(t, err)
			got, err := l.e.completeFire(ctx, l.c, m, 2)
			require.NoError(t, err)
			require.Equal(t, f, got)
			var retained []byte
			require.NoError(t, l.c.QueryRow(ctx, "SELECT payload FROM wildfire_output_chunks").Scan(&retained))
			require.Equal(t, sealed, retained)
			var original []byte
			require.NoError(t, l.c.QueryRow(ctx, "SELECT payload FROM wildfire_state WHERE kind='fire'").Scan(&original))
			require.Equal(t, body, original)
		})
	}
}
