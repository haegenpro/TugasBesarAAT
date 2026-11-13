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
	"sync/atomic"
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
			oldRead, writerDone := make(chan struct{}), make(chan struct{})
			writerError := make(chan error, 1)
			var once sync.Once
			var inventoryCalls atomic.Int64
			inventory := s.Reads.Inventory
			s.Reads.testUnitReads = units
			if units {
				s.Reads.testUnitReadFault = func(call context.Context, phase string, group int) error {
					if phase == "after_rows" && group == 1 {
						once.Do(func() { close(oldRead) })
					}
					if phase == "before" && group == 2 {
						select {
						case <-writerDone:
							return nil
						case <-call.Done():
							return call.Err()
						}
					}
					return nil
				}
			}
			s.Reads.Inventory = func(call context.Context) (audit.Inventory, error) {
				inventoryCalls.Add(1)
				if !units {
					once.Do(func() { close(oldRead) })
					select {
					case <-writerDone:
					case <-call.Done():
						return audit.Inventory{}, call.Err()
					}
				}
				return inventory(call)
			}
			go func() { <-oldRead; _, err := m2Retry(s, correction); writerError <- err; close(writerDone) }()
			started := time.Now()
			page, err := s.AuditEvents(ctx, audit.Query{Source: "BMKG", Limit: 100, Scope: "audit"})
			require.NoError(t, <-writerError)
			if err != nil {
				page, err = s.AuditEvents(ctx, audit.Query{Source: "BMKG", Limit: 100, Scope: "audit"})
			}
			require.NoError(t, err)
			require.False(t, page.Partial)
			require.Len(t, page.Items, 2)
			for _, event := range page.Items {
				value, _ := json.Marshal(event.Attributes)
				require.JSONEq(t, `{"version":2}`, string(value))
			}
			require.GreaterOrEqual(t, inventoryCalls.Load(), int64(2))
			t.Logf("unit_reads=%v inventory_attempts=%d elapsed_ms=%d", units, inventoryCalls.Load(), time.Since(started).Milliseconds())
		})
	}
}
