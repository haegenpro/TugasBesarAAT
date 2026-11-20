package store

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type inventoryTransport func(*http.Request) (*http.Response, error)

func (f inventoryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestInventoryMemberUsesOneLeaseAwareResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body     string
		status         int
		primary, known bool
	}{
		{"leader", `{"role":"primary","state":"running"}`, 200, true, true},
		{"replica", `{"role":"replica","state":"running"}`, 503, false, true},
		{"leader-without-lock", `{"role":"primary","state":"running"}`, 503, true, false},
		{"stopped", `{"role":"replica","state":"stopped"}`, 503, false, false},
		{"invalid", `unavailable`, 503, false, false},
		{"proxy-error", `{"role":"primary","state":"running"}`, 502, false, false},
		{"inconsistent", `{"role":"replica","state":"running"}`, 200, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			k := KubernetesInventory{Patroni: &http.Client{Transport: inventoryTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				require.Equal(t, "/primary", r.URL.Path)
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body)), Header: make(http.Header)}, nil
			})}}
			u := k.memberUnit(context.Background(), 2, ReplicationMember{Name: "member-a", Host: "member-a.example"})
			require.Equal(t, 1, calls)
			require.Equal(t, tc.primary, u.Primary)
			require.Equal(t, tc.known, u.RoleKnown)
			require.False(t, u.Available, "placement and SQL identity must still be verified by Discover")
		})
	}
}

func TestPlacementRefreshDoesNotExtendReadBudgetAndExpires(t *testing.T) {
	release := make(chan struct{})
	cache := newPodPlacementCache(func(ctx context.Context) (map[string]string, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return map[string]string{"member-a": "node-a"}, nil
		}
	})
	defer cache.close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, err := cache.read(ctx)
	cancel()
	require.Error(t, err, "a slow metadata refresh cannot extend a live request")
	require.False(t, cache.current())
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	nodes, err := cache.read(ctx)
	require.NoError(t, err)
	require.Equal(t, "node-a", nodes["member-a"])
	nodes["member-a"] = "untrusted-mutation"
	unchanged, err := cache.read(ctx)
	require.NoError(t, err)
	require.Equal(t, "node-a", unchanged["member-a"], "callers cannot mutate shared placement")
	cache.close()
	cache.mu.Lock()
	cache.updated = time.Now().Add(-placementMaxAge - time.Second)
	cache.mu.Unlock()
	require.False(t, cache.current())
	_, err = cache.read(ctx)
	require.Error(t, err, "an unavailable metadata service cannot leave an indefinitely valid placement")
}
