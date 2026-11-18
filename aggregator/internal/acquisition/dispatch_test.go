package acquisition

import (
	"context"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// This isolates the durability boundary from the real PostgreSQL tests. The
// fake can commit a receipt and deliberately lose its acknowledgement.
type lostReceiptReply struct {
	reserves   atomic.Int64
	receipts   map[string]Receipt
	failPermit bool
}

func (r *lostReceiptReply) ReserveDispatch(_ context.Context, d Dispatch) (Receipt, error) {
	r.reserves.Add(1)
	if r.failPermit {
		return Receipt{}, ErrDispatchPending
	}
	if cached, ok := r.receipts[d.Key]; ok {
		return cached, nil
	}
	return Receipt{ID: d.ID, State: "pending", StartedAt: time.Now().UTC()}, nil
}
func (r *lostReceiptReply) CompleteDispatch(_ context.Context, d Dispatch, receipt Receipt) error {
	receipt.State = "complete"
	r.receipts[d.Key] = receipt
	return ErrDispatchPending
}
func (r *lostReceiptReply) CloseDispatchStream(context.Context, Dispatch) error { return nil }
func TestDispatchLostReceiptAckReusesSavedResponse(t *testing.T) {
	repo := &repositoryFixture{}
	c := fixtureCoordinator(t, repo)
	var calls atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); _, _ = w.Write([]byte("original")) }))
	defer source.Close()
	journal := &lostReceiptReply{receipts: map[string]Receipt{}}
	client, err := Client(journal, "BMKG", source.URL, time.Second, 4096)
	require.NoError(t, err)
	c.Run(context.Background(), "BMKG", func(owned context.Context) {
		makeRequest := func() (*http.Response, error) {
			req, err := http.NewRequestWithContext(WithRequestKey(owned, "namespace:0"), "GET", source.URL+"/seismic-events", nil)
			require.NoError(t, err)
			return client.Do(req)
		}
		_, err := makeRequest()
		require.ErrorIs(t, err, ErrDispatchPending)
		resp, err := makeRequest()
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "original", string(body))
		require.EqualValues(t, 1, calls.Load())
	})
}
func TestDispatchUnconfirmedIntentNeverReachesNetwork(t *testing.T) {
	repo := &repositoryFixture{}
	c := fixtureCoordinator(t, repo)
	var calls atomic.Int64
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer source.Close()
	journal := &lostReceiptReply{failPermit: true}
	client, err := Client(journal, "BMKG", source.URL, time.Second, 4096)
	require.NoError(t, err)
	c.Run(context.Background(), "BMKG", func(owned context.Context) {
		req, err := http.NewRequestWithContext(WithRequestKey(owned, "namespace:0"), "GET", source.URL+"/seismic-events", nil)
		require.NoError(t, err)
		_, err = client.Do(req)
		require.ErrorIs(t, err, ErrDispatchPending)
		require.EqualValues(t, 0, calls.Load())
	})
}
