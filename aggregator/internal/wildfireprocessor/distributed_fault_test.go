package wildfireprocessor

import (
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestM6InterruptedParticipantCommitRecoversSameJob(t *testing.T) {
	l := databaseLab(t, true)
	ctx := context.Background()
	ops := fixtureOperations(t, "bridge")[:2]
	first, _ := wildfire.ParseDetection(*ops[0].Detection, 1, nil)
	// Pick a lexical second identity whose two directed edge records actually
	// reside on different Citus groups. Geography and event time stay unchanged.
	var groupA, groupB int
	for i := 0; i < 100; i++ {
		ops[1].Detection.Satellite = "N" + strconv.Itoa(i)
		second, _ := wildfire.ParseDetection(*ops[1].Detection, 2, nil)
		edgeA := string(rawinput.Compact(first.Key, second.Key))
		edgeB := string(rawinput.Compact(second.Key, first.Key))
		require.NoError(t, l.c.QueryRow(ctx, `SELECT groupid FROM pg_dist_placement WHERE shardid=get_shard_id_for_distribution_column('wildfire_state',wildfire_route($1,'edge',$2))`, l.e.Config.Namespace, edgeA).Scan(&groupA))
		require.NoError(t, l.c.QueryRow(ctx, `SELECT groupid FROM pg_dist_placement WHERE shardid=get_shard_id_for_distribution_column('wildfire_state',wildfire_route($1,'edge',$2))`, l.e.Config.Namespace, edgeB).Scan(&groupB))
		if groupA == 2 && groupB == 1 {
			break
		}
	}
	require.Equal(t, 2, groupA)
	require.Equal(t, 1, groupB)
	l.append(t, ops[0])
	l.prepare(t, 1)
	l.deliver(t, 1)
	l.append(t, ops[1])
	for i := 0; i < 100; i++ {
		var phase string
		var cp checkpoint
		var b []byte
		err := l.c.QueryRow(ctx, `SELECT phase,checkpoint FROM wildfire_jobs WHERE position=2`).Scan(&phase, &b)
		if err == nil {
			require.NoError(t, json.Unmarshal(b, &cp))
			if phase == "edges" && cp.Kind == "candidate" {
				break
			}
		}
		_, err = l.e.Step(ctx, l.p)
		require.NoError(t, err)
	}
	// Install the barrier before starting the two-second attempt. The deferred
	// trigger blocks only this unique test database's physical edge shard during
	// PREPARE. No Docker CLI latency or production fault option is involved.
	second, _ := wildfire.ParseDetection(*ops[1].Detection, 2, nil)
	var shard int64
	require.NoError(t, l.c.QueryRow(ctx, `SELECT get_shard_id_for_distribution_column('wildfire_state',wildfire_route($1,'edge',$2))`, l.e.Config.Namespace, string(rawinput.Compact(first.Key, second.Key))).Scan(&shard))
	blockedURL, err := url.Parse(os.Getenv("M2_WORKER2_URL"))
	require.NoError(t, err)
	blockedURL.Path = "/" + l.c.Config().Database
	blocker, err := pgx.Connect(ctx, blockedURL.String())
	require.NoError(t, err)
	defer blocker.Close(ctx)
	// Physical-shard fault DDL is local to this test session/database.
	_, err = blocker.Exec(ctx, `SET citus.enable_ddl_propagation=off`)
	require.NoError(t, err)
	const barrierKey = int64(60821001)
	table := pgx.Identifier{"public", fmt.Sprintf("wildfire_state_%d", shard)}.Sanitize()
	_, err = blocker.Exec(ctx, `CREATE FUNCTION public.m6_commit_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(60821001); RETURN NEW; END $$`)
	require.NoError(t, err)
	_, err = blocker.Exec(ctx, `CREATE CONSTRAINT TRIGGER m6_commit_barrier AFTER INSERT ON `+table+` DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.m6_commit_barrier()`)
	require.NoError(t, err)
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_lock($1)`, barrierKey)
	require.NoError(t, err)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = blocker.Exec(cleanup, `SELECT pg_advisory_unlock($1)`, barrierKey)
		_, _ = blocker.Exec(cleanup, `DROP TRIGGER IF EXISTS m6_commit_barrier ON `+table)
		_, _ = blocker.Exec(cleanup, `DROP FUNCTION IF EXISTS public.m6_commit_barrier()`)
	}()
	worker, err := url.Parse(os.Getenv("M2_WORKER1_URL"))
	require.NoError(t, err)
	worker.Path = "/" + l.c.Config().Database
	peer, err := pgx.Connect(ctx, worker.String())
	require.NoError(t, err)
	defer peer.Close(ctx)
	ready := make(chan int32, 1)
	l.e.transactionFault = func(_ context.Context, tx pgx.Tx) error {
		ready <- int32(tx.Conn().PgConn().PID())
		return nil
	}
	done := make(chan error, 1)
	go func() { _, err := l.e.Step(ctx, l.p); done <- err }()
	var pid int32
	select {
	case pid = <-ready:
	case err = <-done:
		t.Fatalf("attempt failed before fault boundary: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("fault boundary unavailable")
	}
	prepared := false
	observation, stop := context.WithTimeout(ctx, 1500*time.Millisecond)
	for observation.Err() == nil {
		var n int
		if peer.QueryRow(observation, `SELECT count(*) FROM pg_prepared_xacts`).Scan(&n) == nil && n > 0 {
			prepared = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	stop()
	// Always release the barrier, including a failed synchronized observation.
	if !prepared {
		_, _ = blocker.Exec(ctx, `SELECT pg_advisory_unlock($1)`, barrierKey)
		t.Fatalf("participant PREPARE was not observed: %v", <-done)
	}
	_, err = l.c.Exec(ctx, `SELECT pg_terminate_backend($1::integer)`, pid)
	require.NoError(t, err)
	_, err = blocker.Exec(ctx, `SELECT pg_advisory_unlock($1)`, barrierKey)
	require.NoError(t, err)
	require.Error(t, <-done)
	l.e.transactionFault = nil
	deadline := time.Now().Add(30 * time.Second)
	settled := false
	for time.Now().Before(deadline) {
		call, cancel := context.WithTimeout(ctx, time.Second)
		_, err = l.s.ProcessorStorageBoundary(call, "")
		cancel()
		if err == nil {
			settled = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, settled, "distributed decision did not resolve")
	l.prepare(t, 2)
	l.deliver(t, 2)
	m := oracle(t, l.e.Config)
	for _, op := range ops {
		_, err = m.Apply(op)
		require.NoError(t, err)
	}
	compareOracle(t, l, m)
	t.Logf("observed participant PREPARE, terminated coordinator backend %d; original job recovered with edge groups %d/%d", pid, groupA, groupB)
}
