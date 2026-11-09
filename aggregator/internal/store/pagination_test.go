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
