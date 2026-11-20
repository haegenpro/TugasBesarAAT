package store

import (
	"aggregator/internal/audit"
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestM4CitusCatalogUsesOneConsistentRoutingSnapshot(t *testing.T) {
	if os.Getenv("M2_WORKER1_URL") == "" {
		t.Skip("run-owned Citus workers required")
	}
	dsn, c := m2Database(t, true)
	m2Schema(t, c, "citus", false)
	s := m2Store(t, dsn)
	want, topology, err := s.readCatalog(context.Background())
	require.NoError(t, err)
	got, current, err := s.readCitusCatalog(context.Background())
	require.NoError(t, err)
	require.Equal(t, topology, current)
	require.Equal(t, want, got)
	// A placement catalog cannot establish a complete read while a required
	// routed primary is marked inactive.
	_, err = c.Exec(context.Background(), `UPDATE pg_dist_node SET isactive=false WHERE nodeid=(SELECT min(nodeid) FROM pg_dist_node WHERE groupid>0)`)
	require.NoError(t, err)
	_, _, err = s.readCitusCatalog(context.Background())
	require.Error(t, err)
}

func TestM2RecoveryAndPublicationFenceIgnoresUnrelatedDatabase(t *testing.T) {
	if os.Getenv("M2_WORKER1_URL") == "" {
		t.Skip("run-owned Citus workers required")
	}
	dsn, c := m2Database(t, true)
	m2Schema(t, c, "citus", false)
	s := m2Store(t, dsn)
	foreignDSN, foreign := m2Database(t, true)
	parsed, err := url.Parse(foreignDSN)
	require.NoError(t, err)
	worker, err := url.Parse(os.Getenv("M2_WORKER1_URL"))
	require.NoError(t, err)
	worker.Path = parsed.Path
	foreignWorker := m2Connection(t, worker.String())
	for _, db := range []*pgx.Conn{foreign, foreignWorker} {
		id := uuid.NewString()
		_, err = db.Exec(context.Background(), "BEGIN; CREATE TABLE unrelated_prepared_marker (id int); PREPARE TRANSACTION '"+id+"'")
		require.NoError(t, err)
		t.Cleanup(func() { _, e := db.Exec(context.Background(), "ROLLBACK PREPARED '"+id+"'"); require.NoError(t, e) })
	}
	topology, _, err := s.primaryTopology(context.Background())
	require.NoError(t, err)
	require.NoError(t, s.distributedSettled(context.Background(), topology), "unrelated databases cannot block the canonical decision")
}

func TestM2VisibilityLiveCutChecksFinalAuthority(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	m4ReadSetup(t, s, dsn, false)
	m2Apply(t, s, m2Batch("live-cut", 1, 0))
	anchor, err := readReplication(context.Background(), s.Pool)
	require.NoError(t, err)
	lease, calls := true, 0
	k := KubernetesInventory{replication: &replicatedDurability{cfg: ReplicationConfig{Formation: anchor.Formation, Groups: []ReplicationGroup{{ID: 0, SystemID: anchor.SystemID, Members: []ReplicationMember{{Name: anchor.Member, Host: "fixture.example"}}}}}}, Patroni: &http.Client{Transport: inventoryTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		status := 200
		if !lease {
			status = 503
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
	})}}
	s.Reads.Cut = k.ReadCut
	page, err := s.AuditEvents(context.Background(), audit.Query{Limit: 5})
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Equal(t, 1, calls, "the final cut must verify the current leader lease")
	inventory := s.Reads.Inventory
	s.Reads.Inventory = func(ctx context.Context) (audit.Inventory, error) { lease = false; return inventory(ctx) }
	_, err = s.AuditEvents(context.Background(), audit.Query{Limit: 5})
	require.Error(t, err, "losing the lease during rows must invalidate an otherwise stable generation")
	lease = true
	s.Reads.Inventory = inventory
	s.Reads.Cut = func(ctx context.Context, q readQuerier, final bool) (int64, bool, replicationStatus, error) {
		g, p, physical, e := k.ReadCut(ctx, q, final)
		if final {
			physical.Started = "replacement-postmaster"
		}
		return g, p, physical, e
	}
	_, err = s.AuditEvents(context.Background(), audit.Query{Limit: 5})
	require.Error(t, err, "a replacement physical primary must invalidate the cut")
}
