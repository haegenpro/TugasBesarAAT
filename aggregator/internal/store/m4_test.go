package store

import (
	"aggregator/internal/audit"
	"aggregator/internal/pagecursor"
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestM4UnicodeByteOrderAndCrossSourceDuplicates(t *testing.T) {
	dsn, c := m2Database(t, false)
	m2Schema(t, c, "postgres", false)
	s := m2Store(t, dsn)
	m4ReadSetup(t, s, dsn, false)
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "role1-m3-m4-v1.json"))
	require.NoError(t, err)
	var fixture struct {
		Checksum struct {
			Refs  []string `json:"source_ref_ids"`
			Count int64    `json:"count"`
			Hash  string   `json:"sha256"`
		} `json:"unicode_checksum"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	for i, ref := range fixture.Checksum.Refs {
		b := m2Batch(ref, int64(i+1), 0)
		if i == 4 {
			b.Mutations[0].Source = "PVMBG"
		}
		m2Apply(t, s, b)
	}
	checksum, err := s.AuditChecksum(context.Background(), "")
	require.NoError(t, err)
	require.Nil(t, checksum.Source)
	require.Equal(t, fixture.Checksum.Count, checksum.Count)
	require.Equal(t, fixture.Checksum.Hash, checksum.Checksum)
}

func m4ReadSetup(t *testing.T, s *Store, dsn string, citus bool) {
	t.Helper()
	s.Reads.Signer = pagecursor.Signer{Key: []byte(strings.Repeat("s", 32))}
	s.Reads.Groups = map[int]*pgxpool.Pool{0: s.Pool}
	inventory := audit.Inventory{Units: []audit.Unit{{Name: "postgres-primary", Node: "fixture-node-a", Group: 0, Available: true, Primary: true, RoleKnown: true}, {Name: "postgres-replica", Node: "fixture-node-b", Group: 0, Available: true, RoleKnown: true}}}
	if citus {
		inventory.Units = nil
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		for i, key := range []string{"M2_WORKER1_URL", "M2_WORKER2_URL"} {
			worker, err := url.Parse(os.Getenv(key))
			require.NoError(t, err)
			worker.Path = u.Path
			pool, err := pgxpool.New(context.Background(), worker.String())
			require.NoError(t, err)
			t.Cleanup(pool.Close)
			s.Reads.Groups[i+1] = pool
			inventory.Units = append(inventory.Units, audit.Unit{Name: []string{"worker1", "worker2"}[i], Node: []string{"fixture-node-b", "fixture-node-c"}[i], Group: i + 1, Available: true, Primary: true, RoleKnown: true})
		}
	}
	s.Reads.Inventory = func(ctx context.Context) (audit.Inventory, error) { return inventory, nil }
}
func TestM4OrderChecksumAndInactivePopulation(t *testing.T) {
	for _, target := range []string{"postgres", "citus"} {
		t.Run(target, func(t *testing.T) {
			if target == "citus" && os.Getenv("M2_WORKER1_URL") == "" {
				t.Skip("Citus workers unavailable")
			}
			dsn, c := m2Database(t, target == "citus")
			m2Schema(t, c, target, false)
			s := m2Store(t, dsn)
			m4ReadSetup(t, s, dsn, target == "citus")
			ctx := context.Background()
			first := m2Apply(t, s, m2Batch("a", 1, 0))
			second := m2Apply(t, s, m2Batch("b", 2, 0))
			_ = first
			_ = second
			at := time.Date(2026, 11, 13, 0, 0, 0, 123456000, time.UTC)
			_, err := c.Exec(ctx, `UPDATE hazard_events SET ingested_at=$1`, at)
			require.NoError(t, err)
			q := audit.Query{Source: "BMKG", Since: at, Limit: 1, Scope: "audit"}
			page, err := s.AuditEvents(ctx, q)
			require.NoError(t, err)
			require.Len(t, page.Items, 1)
			require.NotNil(t, page.NextCursor)
			firstID := page.Items[0].HazardID
			p, err := s.Reads.Signer.Decode(*page.NextCursor, "audit/events", q.Scope, q.Filters())
			require.NoError(t, err)
			require.Equal(t, at, p.Time)
			q.After = p
			page, err = s.AuditEvents(ctx, q)
			require.NoError(t, err)
			require.Len(t, page.Items, 1)
			require.Greater(t, page.Items[0].HazardID, firstID)
			require.Nil(t, page.NextCursor)
			sum, err := s.AuditChecksum(ctx, "BMKG")
			require.NoError(t, err)
			require.Equal(t, int64(2), sum.Count)
			require.Equal(t, "7e18f737311b2dc3b2f269dd78396b0351f14fb66efa879f768cb23181883c78", sum.Checksum)
			require.False(t, sum.Partial)
			sum, err = s.AuditChecksum(ctx, "SATELIT")
			require.NoError(t, err)
			require.Zero(t, sum.Count)
			require.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", sum.Checksum)
			location, err := s.AuditLocation(ctx, firstID)
			require.NoError(t, err)
			require.NotEmpty(t, location.Locations)
			_, err = c.Exec(ctx, `UPDATE hazard_events SET active=false WHERE source_ref_id='a'`)
			require.NoError(t, err)
			sum, err = s.AuditChecksum(ctx, "BMKG")
			require.NoError(t, err)
			require.Equal(t, int64(1), sum.Count)
			require.Equal(t, "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d", sum.Checksum)
		})
	}
}
func TestM4MissingWorkerPreservesContinuation(t *testing.T) {
	if os.Getenv("M2_EVIDENCE_DIR") == "" {
		t.Skip("run-owned synchronized fault controller required")
	}
	dsn, c := m2Database(t, true)
	m2Schema(t, c, "citus", false)
	s := m2Store(t, dsn)
	m4ReadSetup(t, s, dsn, true)
	ctx := context.Background()
	for i := int64(0); i < 16; i++ {
		m2Apply(t, s, m2Batch("read-"+strconv.FormatInt(i, 10), i+1, 0))
	}
	m2Fault(t, "stop", "worker2")
	t.Cleanup(func() { m2Fault(t, "start", "worker2") })
	q := audit.Query{Limit: 100, Scope: "audit"}
	started := time.Now()
	page, err := s.AuditEvents(ctx, q)
	require.NoError(t, err)
	require.True(t, page.Partial)
	require.Empty(t, page.Items)
	require.NotNil(t, page.NextCursor)
	require.Contains(t, page.Unavailable, "worker2")
	require.Less(t, time.Since(started), 2250*time.Millisecond)
	q.After, err = s.Reads.Signer.Decode(*page.NextCursor, "audit/events", q.Scope, q.Filters())
	require.NoError(t, err)
	require.Empty(t, q.After.ID)
	_, _, err = s.GetByIDWithMetadata(ctx, "00000000-0000-4000-8000-000000000099")
	m2Error(t, err, "temporarily_unavailable")
	location, err := s.AuditLocation(ctx, "00000000-0000-4000-8000-000000000099")
	require.NoError(t, err)
	require.True(t, location.Partial)
	require.Empty(t, location.Locations)
	checksum, err := s.AuditChecksum(ctx, "BMKG")
	require.NoError(t, err)
	require.True(t, checksum.Partial)
	require.Less(t, checksum.Count, int64(16))
	m2Fault(t, "start", "worker2")
	m2Eventually(t, 30*time.Second, func() bool {
		var n int
		return c.QueryRow(ctx, `SELECT count(*) FROM hazard_events`).Scan(&n) == nil && n == 16
	})
	page, err = s.AuditEvents(ctx, q)
	require.NoError(t, err)
	require.False(t, page.Partial)
	require.Len(t, page.Items, 16)
	require.Nil(t, page.NextCursor)
}
