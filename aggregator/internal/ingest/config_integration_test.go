package ingest

import (
	"aggregator/internal/coastal"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestM2ConfiguredDatabaseTimeoutsAreEnforced(t *testing.T) {
	fixture := testStore(t)
	t.Setenv("DB_STATEMENT_TIMEOUT", "150ms")
	t.Setenv("DB_LOCK_TIMEOUT", "80ms")
	t.Setenv("DB_IDLE_TRANSACTION_TIMEOUT", "4s")
	tuned, err := New(context.Background(), fixture.Pool.Config().ConnString())
	require.NoError(t, err)
	defer tuned.Pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for key, want := range map[string]string{"statement_timeout": "150ms", "lock_timeout": "80ms", "idle_in_transaction_session_timeout": "4s"} {
		var got string
		require.NoError(t, tuned.Pool.QueryRow(ctx, "SELECT current_setting($1)", key).Scan(&got))
		require.Equal(t, want, got)
	}
	_, err = tuned.Pool.Exec(ctx, "SELECT pg_sleep(1)")
	var failure *pgconn.PgError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, "57014", failure.Code, "statement timeout should cancel the query")
	_, err = fixture.Pool.Exec(ctx, "CREATE TABLE timeout_probe (id integer)")
	require.NoError(t, err)
	holder, err := fixture.Pool.Begin(ctx)
	require.NoError(t, err)
	defer holder.Rollback(context.Background())
	_, err = holder.Exec(ctx, "LOCK TABLE timeout_probe IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)
	_, err = tuned.Pool.Exec(ctx, "SELECT * FROM timeout_probe")
	require.ErrorAs(t, err, &failure)
	require.Equal(t, "55P03", failure.Code, "lock timeout should fire before statement timeout")
}

func TestM2CoastalConfiguredDeadlineCancelsUpstream(t *testing.T) {
	s := testStore(t)
	t.Setenv("PANTAI_REQUEST_TIMEOUT_MS", "40")
	cancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(cancelled)
	}))
	defer server.Close()
	c := &coastalClient{cfg: CoastalConfig{URL: server.URL, Namespace: "deadline-test"}, http: server.Client(), s: s,
		limiter: coastal.Limiter{Rate: 1000, Burst: 1}, observations: coastal.Breaker{Name: "observations", Threshold: 3}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	require.NoError(t, s.Leases.Run(ctx, "acquire:deadline-test", func(owned context.Context) error {
		var out any
		_, err := c.get(owned, "/v1/observations", &c.observations, &out)
		require.Error(t, err)
		return nil
	}))
	require.Less(t, time.Since(started), time.Second, "configured timeout should precede the former 1.2-second deadline")
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("upstream request was not cancelled")
	}
}
