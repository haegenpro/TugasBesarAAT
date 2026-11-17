package store

import (
	"context"
	"fmt"
	"os"
	"testing"

	"aggregator/internal/canonical"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestMultiEntityBackendRequiresPinnedProtocolAndRecovery(t *testing.T) {
	for _, version := range []string{"14.2", "14.2-1", "14.2.0", "14.2.1"} {
		require.NoError(t, validateBatchBackend(version, 128, 1000))
	}
	for _, invalid := range []struct {
		version            string
		prepared, recovery int
	}{
		{"", 128, 1000}, {"13.4.0", 128, 1000}, {"14.20.0", 128, 1000},
		{"14.2.0", 0, 1000}, {"14.2.0", 128, -1}, {"14.2.0", 128, 0},
	} {
		require.Error(t, validateBatchBackend(invalid.version, invalid.prepared, invalid.recovery))
	}
}

type rejectedMultiDurability struct {
	labDurability
	called bool
}

func (d *rejectedMultiDurability) PrepareBatch(context.Context, pgx.Tx) error {
	d.called = true
	return canonical.Unavailable()
}

func TestMultiEntityProviderCannotBeBypassedByFixtureFlag(t *testing.T) {
	dsn, conn := m2Database(t, false)
	m2Schema(t, conn, "postgres", false)
	s := m2Store(t, dsn)
	d := &rejectedMultiDurability{}
	s.Canonical.Durability = d
	s.Canonical.MultiEntityValidated = true
	b := m2Batch("entity-a", 1, 0)
	second := b.Mutations[0]
	second.SourceRefID = "entity-b"
	second.OperationID = "second-op"
	b.Mutations = append(b.Mutations, second)
	_, err := s.ApplyBatch(context.Background(), b)
	require.Error(t, err)
	require.True(t, d.called)
	var count int
	require.NoError(t, s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM canonical_batches WHERE batch_id=$1", b.BatchID).Scan(&count))
	require.Zero(t, count)
}

// This adapter verifies the live Citus protocol in an isolated single-copy lab.
// It deliberately does not claim the production provider's replicated WAL proof.
type protocolFixtureDurability struct{ labDurability }

func (protocolFixtureDurability) PrepareBatch(ctx context.Context, tx pgx.Tx) error {
	if err := checkBatchBackend(ctx, tx); err != nil {
		return err
	}
	var enabled bool
	if err := tx.QueryRow(ctx, `SELECT count(*)=2 AND bool_and(success AND result::integer>0) FROM run_command_on_workers('SELECT setting::integer FROM pg_settings WHERE name=''citus.recover_2pc_interval''')`).Scan(&enabled); err != nil {
		return err
	}
	if !enabled {
		return canonical.Unavailable()
	}
	return nil
}

func TestMultiEntityCitusAtomicBatchAndRetry(t *testing.T) {
	if os.Getenv("M2_WORKER1_URL") == "" || os.Getenv("M2_WORKER2_URL") == "" {
		t.Skip("isolated Citus workers absent; distributed batch proof not executed")
	}
	dsn, conn := m2Database(t, true)
	m2Schema(t, conn, "citus", false)
	s := m2Store(t, dsn)
	s.Canonical.Durability = protocolFixtureDurability{}
	s.Canonical.MultiEntityValidated = false
	ctx := context.Background()
	require.NoError(t, checkBatchBackend(ctx, conn), "actual pinned Citus backend capability")
	probe, probeErr := s.Pool.Begin(ctx)
	require.NoError(t, probeErr)
	probeErr = protocolFixtureDurability{}.PrepareBatch(ctx, probe)
	_ = probe.Rollback(ctx)
	require.NoError(t, probeErr, "live worker capability query")
	group := func(ref string) string {
		var host string
		require.NoError(t, conn.QueryRow(ctx, "SELECT n.nodename FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid) WHERE p.shardid=get_shard_id_for_distribution_column('hazard_events',canonical_route('BMKG',$1)) AND n.noderole='primary'", ref).Scan(&host))
		return host
	}
	b := m2Batch("multi-capability-a", 1, 0)
	other := m2Batch("multi-capability-b", 1, 0)
	first := group(b.Mutations[0].SourceRefID)
	for i := 0; group(other.Mutations[0].SourceRefID) == first; i++ {
		require.Less(t, i, 100)
		other = m2Batch(fmt.Sprintf("multi-capability-b-%d", i), 1, 0)
	}
	b.Mutations = append(b.Mutations, other.Mutations[0])
	result := m2Apply(t, s, b)
	require.Len(t, result.Items, 2)
	duplicate := m2Apply(t, s, b)
	require.Equal(t, "duplicate", duplicate.Outcome)
	require.Equal(t, result.Items, duplicate.Items)
	bad := m2Batch("multi-capability-rollback", 1, 0)
	conflict := b.Mutations[1]
	conflict.OperationID = "new-conflicting-op"
	conflict.SourceOrder.Position = 2
	conflict.ExpectedRevision = 999
	bad.Mutations = append(bad.Mutations, conflict)
	_, err := s.ApplyBatch(ctx, bad)
	m2Error(t, err, "revision_conflict")
	var count int
	require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM canonical_entities WHERE source_ref_id='multi-capability-rollback'").Scan(&count))
	require.Zero(t, count)
}
