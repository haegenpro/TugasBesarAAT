package store

import (
	"aggregator/internal/audit"
	"aggregator/internal/canonical"
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestM4ConcurrentCorrectionsValidateCoordinatorAndUnits(t *testing.T) {
	if os.Getenv("M2_WORKER1_URL") == "" {
		t.Skip("run-owned Citus workers required")
	}
	for _, units := range []bool{false, true} {
		t.Run(strconv.FormatBool(units), func(t *testing.T) {
			dsn, c := m2Database(t, true)
			m2Schema(t, c, "citus", false)
			s := m2Store(t, dsn)
			m4ReadSetup(t, s, dsn, true)
			ctx := context.Background()
			refs := map[int]string{}
			for i := 0; len(refs) < 2 && i < 100; i++ {
				ref := "visibility-" + strconv.Itoa(i)
				route, _ := canonical.Route("BMKG", ref)
				var group int
				require.NoError(t, c.QueryRow(ctx, `SELECT groupid FROM pg_dist_placement WHERE shardid=get_shard_id_for_distribution_column('hazard_events',$1::text)`, route).Scan(&group))
				refs[group] = ref
			}
			require.Len(t, refs, 2)
			for _, ref := range refs {
				m2Apply(t, s, m2Batch(ref, 1, 0))
			}
			correction := m2Batch(refs[1], 2, 1)
			second := m2Batch(refs[2], 2, 1).Mutations[0]
			correction.Mutations = append(correction.Mutations, second)
			for i := range correction.Mutations {
				correction.Mutations[i].Event.Attributes = map[string]any{"version": 2}
			}
			// Stage both worker writes first. The injected overlap tests commit
			// exclusion, independent of how long the test host takes to stage SQL.
			tx, err := s.beginMutation(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)
			require.NoError(t, s.Canonical.Fence.Protect(ctx, tx, correction.Ownership, correction.Mutations))
			for _, mutation := range correction.Mutations {
				_, err := s.applyMutation(ctx, tx, correction.BatchID, mutation)
				require.NoError(t, err)
			}
			oldRead := make(chan struct{})
			writerError := make(chan error, 1)
			var once sync.Once
			waitForWriter := func(call context.Context) error {
				once.Do(func() { close(oldRead) })
				for {
					var waiting bool
					err := c.QueryRow(call, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=0 AND objid=$1::bigint::oid AND NOT granted)`, visibilityKey).Scan(&waiting)
					if waiting || err != nil {
						return err
					}
					select {
					case <-call.Done():
						return call.Err()
					case <-time.After(5 * time.Millisecond):
					}
				}
			}
			inventory := s.Reads.Inventory
			s.Reads.testUnitReads = units
			if units {
				s.Reads.testUnitReadFault = func(call context.Context, phase string, group int) error {
					if phase == "after_rows" && group == 1 {
						return waitForWriter(call)
					}
					return nil
				}
			}
			s.Reads.Inventory = func(call context.Context) (audit.Inventory, error) {
				if !units {
					if err := waitForWriter(call); err != nil {
						return audit.Inventory{}, err
					}
				}
				return inventory(call)
			}
			go func() { <-oldRead; writerError <- tx.Commit(ctx) }()
			started := time.Now()
			page, err := s.AuditEvents(ctx, audit.Query{Source: "BMKG", Limit: 100, Scope: "audit"})
			once.Do(func() { close(oldRead) })
			commitErr := <-writerError
			require.NoError(t, err)
			require.False(t, page.Partial)
			require.Len(t, page.Items, 2)
			for _, event := range page.Items {
				require.NotContains(t, event.Attributes, "version")
			}
			require.NoError(t, commitErr)
			s.Reads.testUnitReadFault = nil
			s.Reads.Inventory = inventory
			page, err = s.AuditEvents(ctx, audit.Query{Source: "BMKG", Limit: 100, Scope: "audit"})
			require.NoError(t, err)
			require.False(t, page.Partial)
			require.Len(t, page.Items, 2)
			for _, event := range page.Items {
				value, _ := json.Marshal(event.Attributes)
				require.JSONEq(t, `{"version":2}`, string(value))
			}
			t.Logf("unit_reads=%v elapsed_ms=%d", units, time.Since(started).Milliseconds())
		})
	}
}
