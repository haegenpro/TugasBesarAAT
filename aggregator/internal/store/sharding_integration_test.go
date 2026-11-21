package store_test

import (
	"aggregator/internal/domain"
	"aggregator/internal/ownership"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// This requires a healthy coordinator and both physical copies of both workers.
// It verifies physical rows, rather than treating Citus catalog entries as copies.
func TestM2P1PhysicalShardsAndGlobalQueries(t *testing.T) {
	s := m2Store(t)
	ctx := context.Background()
	const total = 256
	area := "p1-hotspot-" + uuid.NewString()
	base := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	expected := make([]domain.HazardEvent, 0, total)
	counts := map[int64]int{}
	types := map[string]int64{"SEISMIC": 0, "VOLCANIC": 0, "WILDFIRE": 0, "COASTAL": 0}
	authority := ownership.New(s.Pool)
	started := time.Now()
	require.NoError(t, authority.Run(ctx, "test:"+area, func(owned context.Context) error {
		for i := range total {
			h := sampleEvent(domain.SourceBMKG, fmt.Sprintf("%s-%04d", area, i))
			h.AreaName = area // Every record has the same geographic hotspot.
			h.OccurredAt = timeutil.New(base.Add(time.Duration(i%9) * time.Second))
			switch i % 5 {
			case 1:
				h.Source, h.HazardType = domain.SourcePVMBG, domain.HazardVolcanic
			case 2:
				h.Source, h.HazardType = domain.SourceSatelit, domain.HazardWildfire
			case 3:
				h.Source, h.HazardType = domain.SourceSensorPantai, domain.HazardCoastal
			case 4:
				h.Attributes["warning_id"] = h.SourceRefID
			}
			inserted, err := s.SaveDedup(owned, h, "p1-physical-shards")
			if err != nil {
				return err
			}
			require.True(t, inserted)
			var shard int64
			if err = s.Pool.QueryRow(owned, `SELECT get_shard_id_for_distribution_column('hazard_events',$1::text)`, h.SourceRefID).Scan(&shard); err != nil {
				return err
			}
			counts[shard]++
			types[string(h.HazardType)]++
			expected = append(expected, h)
		}
		return nil
	}))
	writeDuration := time.Since(started)
	require.Len(t, counts, 8, "all eight shards must contain actual fixture rows")
	type member struct {
		Group int
		Host  string
		Port  int
		Role  string
	}
	rows, err := s.Pool.Query(ctx, `SELECT groupid,nodename,nodeport,noderole::text FROM pg_dist_node WHERE groupid>0 AND isactive ORDER BY groupid,noderole`)
	require.NoError(t, err)
	var members []member
	for rows.Next() {
		var m member
		require.NoError(t, rows.Scan(&m.Group, &m.Host, &m.Port, &m.Role))
		members = append(members, m)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Len(t, members, 4)
	placements := map[int64]int{}
	rows, err = s.Pool.Query(ctx, `SELECT s.shardid,p.groupid FROM pg_dist_shard s JOIN pg_dist_placement p USING(shardid) WHERE s.logicalrelid='hazard_events'::regclass AND p.shardstate=1`)
	require.NoError(t, err)
	for rows.Next() {
		var shard int64
		var group int
		require.NoError(t, rows.Scan(&shard, &group))
		placements[shard] = group
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Len(t, placements, 8)
	primaryNodes := map[string]bool{}
	physical := []map[string]any{}
	for _, m := range members {
		cfg := s.Pool.Config().ConnConfig.Copy()
		cfg.Host, cfg.Port = m.Host, uint16(m.Port)
		connection, err := pgx.ConnectConfig(ctx, cfg)
		require.NoError(t, err)
		var recovery bool
		require.NoError(t, connection.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&recovery))
		require.Equal(t, m.Role == "secondary", recovery)
		node := "a"
		if strings.Contains(m.Host, "-b-0.") {
			node = "b"
		}
		if !recovery {
			primaryNodes[node] = true
		}
		memberTotal := 0
		for shard, group := range placements {
			if group != m.Group {
				continue
			}
			var count int
			table := pgx.Identifier{fmt.Sprintf("hazard_events_%d", shard)}.Sanitize()
			require.Eventually(t, func() bool {
				err = connection.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count)
				return err == nil && count == counts[shard]
			}, 5*time.Second, 25*time.Millisecond)
			require.Positive(t, count)
			require.Less(t, count, total, "no shard may contain the entire dataset")
			memberTotal += count
			physical = append(physical, map[string]any{"shard": shard, "group": group, "node": node, "replica": recovery, "rows": count})
		}
		require.Positive(t, memberTotal)
		require.Less(t, memberTotal, total, "both groups must bear data")
		require.NoError(t, connection.Close(ctx))
	}
	require.Len(t, primaryNodes, 2, "healthy primary write roles must span A and B")
	require.Len(t, physical, 16, "every shard must have a physical primary and replica")
	sort.Slice(expected, func(i, j int) bool {
		a, b := expected[i], expected[j]
		if a.OccurredAt.Equal(b.OccurredAt.Time) {
			return a.HazardID > b.HazardID
		}
		return a.OccurredAt.After(b.OccurredAt.Time)
	})
	var ids []string
	for _, workerReads := range []bool{false, true} {
		s.WorkerReads = workerReads
		ids = nil
		cursor := ""
		for {
			page, err := s.QueryPage(ctx, store.Filters{AreaName: area}, store.PageRequest{Limit: 17, Cursor: cursor})
			require.NoError(t, err)
			require.False(t, page.Partial)
			require.Empty(t, page.UnavailableUnits)
			for _, h := range page.Events {
				ids = append(ids, h.HazardID)
			}
			if page.NextCursor == nil {
				break
			}
			cursor = *page.NextCursor
			require.LessOrEqual(t, len(ids), total, "pagination must make progress without duplicate pages")
		}
		require.Len(t, ids, total)
		for i, h := range expected {
			require.Equal(t, h.HazardID, ids[i])
		}
	}
	t.Run("unreachable worker returns the surviving group as partial", func(t *testing.T) {
		base := s.Pool.Config()
		originalDial := base.ConnConfig.DialFunc
		var blocked string
		expectedRemaining := 0
		for _, m := range members {
			if m.Group == 2 && m.Role == "primary" {
				blocked = net.JoinHostPort(m.Host, fmt.Sprint(m.Port))
			}
		}
		require.NotEmpty(t, blocked)
		for shard, group := range placements {
			if group == 1 {
				expectedRemaining += counts[shard]
			}
		}
		partialStore, err := store.New(ctx, base.ConnConfig.ConnString(), slog.New(slog.NewJSONHandler(io.Discard, nil)), func(cfg *pgxpool.Config) {
			cfg.ConnConfig = base.ConnConfig.Copy()
			cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				if address == blocked {
					return nil, errors.New("injected worker connection failure")
				}
				return originalDial(ctx, network, address)
			}
		})
		require.NoError(t, err)
		defer partialStore.Close()
		partialStore.Distributed, partialStore.WorkerReads = true, true
		page, err := partialStore.QueryPage(ctx, store.Filters{AreaName: area}, store.PageRequest{Limit: 1000})
		require.NoError(t, err)
		require.True(t, page.Partial)
		require.NotEmpty(t, page.UnavailableUnits)
		require.Len(t, page.Events, expectedRemaining)
		require.Nil(t, page.NextCursor)
	})
	analytics, err := s.Analytics(ctx, base.Add(-time.Second), base.Add(time.Minute), "hour", "")
	require.NoError(t, err)
	require.EqualValues(t, total, analytics["total"])
	require.False(t, analytics["partial"].(bool))
	require.Equal(t, types, analytics["by_type"])
	require.EqualValues(t, total/5, analytics["tsunami_warning_overlay"])
	checksum, err := s.AuditChecksum(ctx, "")
	require.NoError(t, err)
	require.EqualValues(t, total, checksum["count"])
	result := map[string]any{"rows": total, "shards": counts, "physical_copies": physical, "primary_nodes": primaryNodes, "hotspot_write_seconds": writeDuration.Seconds(), "pagination_rows": len(ids), "analytics_total": analytics["total"], "audit_count": checksum["count"]}
	raw, err := json.Marshal(result)
	require.NoError(t, err)
	t.Log("P1_EVIDENCE " + string(raw))
	if path := os.Getenv("M2_P1_EVIDENCE"); path != "" {
		require.NoError(t, os.WriteFile(path, raw, 0600))
	}
}
