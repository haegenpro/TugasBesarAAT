package store_test

import (
	"aggregator/internal/domain"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestKeysetPaginationWithEqualTimestampsAndChangedFilters(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	area := "pagination-" + uuid.NewString()
	when := time.Now().UTC().Truncate(time.Millisecond)
	for range 7 {
		e := sampleEvent(domain.SourceBMKG, uuid.NewString())
		e.AreaName = area
		e.OccurredAt = timeutil.New(when)
		_, err := s.SaveDedup(ctx, e, "pagination")
		require.NoError(t, err)
	}
	f := store.Filters{AreaName: area}
	seen := map[string]bool{}
	cursor := ""
	last := ""
	for {
		p, err := s.QueryPage(ctx, f, store.PageRequest{Limit: 2, Cursor: cursor})
		require.NoError(t, err)
		require.False(t, p.Partial)
		for _, e := range p.Events {
			require.False(t, seen[e.HazardID])
			if last != "" {
				require.Less(t, e.HazardID, last)
			}
			last = e.HazardID
			seen[e.HazardID] = true
		}
		if p.NextCursor == nil {
			break
		}
		cursor = *p.NextCursor
	}
	require.Len(t, seen, 7)
	// A modified cursor must fail at validation, not become a PostgreSQL cast error.
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	require.NoError(t, err)
	var modified map[string]any
	require.NoError(t, json.Unmarshal(raw, &modified))
	modified["id"] = "urn:uuid:" + modified["id"].(string)
	raw, err = json.Marshal(modified)
	require.NoError(t, err)
	_, err = s.QueryPage(ctx, f, store.PageRequest{Limit: 2, Cursor: base64.RawURLEncoding.EncodeToString(raw)})
	require.ErrorIs(t, err, store.ErrCursor)

	_, err = s.QueryPage(ctx, store.Filters{AreaName: "different"}, store.PageRequest{Limit: 2, Cursor: cursor})
	require.ErrorIs(t, err, store.ErrCursor)
}

// ingested_at is stored at the millisecond precision the API returns, so events saved within one
// millisecond are ordered by hazard_id, which is what a client verifying (ingested_at, hazard_id)
// order from the response expects.
func TestIngestedAtIsStoredAtResponsePrecision(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Millisecond)
	refs := []string{}
	for _, micros := range []int{900, 100, 500} {
		e := sampleEvent(domain.SourceBMKG, "precision-"+uuid.NewString())
		e.IngestedAt = timeutil.New(base.Add(time.Duration(micros) * time.Microsecond))
		_, err := s.SaveDedup(ctx, e, "precision")
		require.NoError(t, err)
		refs = append(refs, e.SourceRefID)
	}
	rows, err := s.Pool.Query(ctx, `SELECT hazard_id::text,ingested_at FROM hazard_events WHERE source='BMKG' AND source_ref_id=ANY($1) ORDER BY ingested_at,hazard_id`, refs)
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id string
		var at time.Time
		require.NoError(t, rows.Scan(&id, &at))
		require.True(t, at.Equal(base), "stored %s, want %s", at.Format(time.RFC3339Nano), base.Format(time.RFC3339Nano))
		got = append(got, id)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 3)
	require.IsIncreasing(t, got, "events within one millisecond are ordered by hazard_id")
}

// Warm opens the requested number of distinct connections, not one connection reused.
func TestWarmOpensDistinctConnections(t *testing.T) {
	s := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	before := s.Pool.Stat().TotalConns()
	require.NoError(t, s.Warm(ctx, 4))
	require.GreaterOrEqual(t, s.Pool.Stat().TotalConns()-before, int32(3), "warm-up opened distinct connections")
	require.GreaterOrEqual(t, s.Pool.Stat().TotalConns(), int32(4))
}
