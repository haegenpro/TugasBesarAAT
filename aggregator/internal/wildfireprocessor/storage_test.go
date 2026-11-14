package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/migrations"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"aggregator/internal/store"
	"aggregator/internal/wildfire"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This provider exists only in _test.go. It proves single-copy component
// semantics and cannot be selected by the production binary/configuration.
type localDurability struct{ s *store.Store }

func (d localDurability) Prepare(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL synchronous_commit=on")
	return err
}
func (d localDurability) Confirm(context.Context, string) error { return nil }
func (d localDurability) ConfirmRecord(ctx context.Context, q string, args ...any) error {
	var found bool
	if err := d.s.Pool.QueryRow(ctx, q, args...).Scan(&found); err != nil {
		return err
	}
	if !found {
		return canonical.Unavailable()
	}
	return nil
}

type lab struct {
	s                   *store.Store
	c                   *pgx.Conn
	e                   *Engine
	p, d                canonical.Ownership
	processor, delivery canonical.OwnershipRepository
	identities          map[string]canonical.Entity
}

func databaseLab(t *testing.T, citus bool) lab {
	t.Helper()
	raw := os.Getenv("M2_DATABASE_URL")
	if raw == "" {
		t.Skip("isolated run-owned database is absent")
	}
	u, err := url.Parse(raw)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, u.Hostname())
	ctx := context.Background()
	hosts := []string{raw}
	if citus {
		if os.Getenv("M2_WORKER1_URL") == "" {
			t.Skip("isolated Citus workers absent")
		}
		hosts = append(hosts, os.Getenv("M2_WORKER1_URL"), os.Getenv("M2_WORKER2_URL"))
	}
	name := "wf_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	nodes := []*pgx.Conn{}
	for _, dsn := range hosts {
		admin, err := pgx.Connect(ctx, dsn)
		require.NoError(t, err)
		_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
		require.NoError(t, err)
		admin.Close(ctx)
		v, err := url.Parse(dsn)
		require.NoError(t, err)
		v.Path = "/" + name
		node, err := pgx.Connect(ctx, v.String())
		require.NoError(t, err)
		t.Cleanup(func() { node.Close(context.Background()) })
		nodes = append(nodes, node)
	}
	c := nodes[0]
	target := "postgres"
	if citus {
		target = "citus"
		for _, node := range nodes {
			_, err = node.Exec(ctx, "CREATE EXTENSION citus")
			require.NoError(t, err)
			pw, _ := u.User.Password()
			_, err = node.Exec(ctx, "INSERT INTO pg_dist_authinfo(nodeid,rolename,authinfo) VALUES(0,'postgres',$1)", "password="+pw)
			require.NoError(t, err)
		}
		for i := 1; i < len(nodes); i++ {
			_, err = c.Exec(ctx, "SELECT citus_add_node($1,5432)", fmt.Sprintf("worker%d", i))
			require.NoError(t, err)
		}
	}
	require.NoError(t, migrations.Up(ctx, c, filepath.Join("..", "..", "..", "store", "migrations"), target, ""))
	require.NoError(t, migrations.Up(ctx, c, filepath.Join("..", "..", "..", "store", "migrations"), target, ""))
	u.Path = "/" + name
	s, err := store.New(ctx, u.String(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(s.Close)
	s.Canonical = store.CanonicalOptions{Durability: localDurability{s}, MultiEntityValidated: true}
	proc, err := ownership.NewProcessor(s.Pool, 15*time.Second)
	require.NoError(t, err)
	delivery, err := ownership.New(s.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	s.Processor = proc
	s.Canonical.Fence = delivery
	p, err := proc.Acquire(ctx, canonical.AcquireOwnership{Scope: "processor:SATELIT", Holder: "processor-" + uuid.NewString()})
	require.NoError(t, err)
	d, err := delivery.Acquire(ctx, canonical.AcquireOwnership{Scope: "canonical:SATELIT", Holder: "delivery-" + uuid.NewString()})
	require.NoError(t, err)
	renewCtx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	t.Cleanup(func() { cancel(); <-stopped })
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				call, stop := context.WithTimeout(renewCtx, canonical.Deadline)
				_, _ = proc.Renew(call, p.Ownership)
				_, _ = delivery.Renew(call, d.Ownership)
				stop()
			}
		}
	}()
	config := fixtureConfig(t)
	s.Inputs.Starts = map[string]int64{store.InputSession("SATELIT", config.Namespace): *config.InitialOffset}
	s.Inputs.PayloadQuota = config.CacheBytes / 4
	e, err := New(s, config)
	require.NoError(t, err)
	require.NoError(t, e.Check(ctx))
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		f, err := os.OpenFile(filepath.Join(dir, "wildfire-databases.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		require.NoError(t, err)
		b, _ := json.Marshal(map[string]any{"test": t.Name(), "database": name, "citus": citus, "durability": "single-copy test provider"})
		_, err = f.Write(append(b, '\n'))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	return lab{s: s, c: c, e: e, p: p.Ownership, d: d.Ownership, processor: proc, delivery: delivery, identities: map[string]canonical.Entity{}}
}
func (l lab) append(t *testing.T, op wildfire.Operation) {
	t.Helper()
	request := rawinput.AppendRequest{Version: 1, Ownership: l.p, Messages: []rawinput.Envelope{envelope(t, op)}}
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := l.s.AppendHistory(context.Background(), request)
		if err == nil {
			return
		}
		require.True(t, retryableFixtureError(err) && time.Now().Before(deadline), "receipt failed: %v", err)
		t.Logf("receipt offset=%d retrying identical persisted identity: %v", op.Offset, err)
		time.Sleep(50 * time.Millisecond)
	}
}

// Match production retries without increasing an individual adapter deadline or
// treating quota/integrity/stale-owner failures as successful progress.
func retryableFixtureError(err error) bool {
	var pending *Pending
	if errors.As(err, &pending) {
		return false
	}
	var failure *canonical.Error
	if errors.As(err, &failure) {
		return failure.Status == 503
	}
	var network net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &network)
}
func (l lab) prepare(t *testing.T, pos int64) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	steps, retries := 0, 0
	for time.Now().Before(deadline) {
		var through int64
		err := l.s.Pool.QueryRow(context.Background(), `SELECT prepared_through FROM input_sessions WHERE source='SATELIT' AND namespace=$1`, l.e.Config.Namespace).Scan(&through)
		require.NoError(t, err)
		if through >= pos {
			t.Logf("prepared=%d steps=%d retries=%d", pos, steps, retries)
			return
		}
		worked, err := l.e.Step(context.Background(), l.p)
		if err != nil && retryableFixtureError(err) {
			retries++
			t.Logf("preparation offset=%d retry: %v", pos, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		require.NoError(t, err)
		require.True(t, worked)
		steps++
	}
	t.Fatal("bounded preparation did not finish")
}
func (l lab) deliver(t *testing.T, pos int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var through int64
		err := l.s.Pool.QueryRow(context.Background(), `SELECT applied_through FROM input_sessions WHERE source='SATELIT' AND namespace=$1`, l.e.Config.Namespace).Scan(&through)
		require.NoError(t, err)
		if through >= pos {
			return
		}
		_, err = l.e.StepDelivery(context.Background(), l.p, l.d)
		if err != nil && retryableFixtureError(err) {
			t.Logf("delivery offset=%d retrying sealed request: %v", pos, err)
			// Recovery proves backend fencing and participant settlement; elapsed
			// time alone never clears an ambiguous canonical attempt.
			call, cancel := context.WithTimeout(context.Background(), canonical.Deadline)
			recoveryErr := l.s.RecoverAttempts(call)
			cancel()
			if recoveryErr != nil {
				require.True(t, retryableFixtureError(recoveryErr), "%v", recoveryErr)
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		require.NoError(t, err)
	}
	t.Fatal("delivery did not finish")
}
func (l lab) fires(t *testing.T) []wildfire.Fire {
	t.Helper()
	all := []wildfire.Fire{}
	after := ""
	for {
		page, err := l.e.Fires(context.Background(), after, l.e.Config.PageRows)
		require.NoError(t, err)
		all = append(all, page...)
		if len(page) < l.e.Config.PageRows {
			return all
		}
		after = page[len(page)-1].ID
	}
}
func compareOracle(t *testing.T, l lab, m *wildfire.Model) {
	t.Helper()
	actual := l.fires(t)
	expected := m.Snapshot().Fires
	require.Equal(t, expected, actual)
	for _, fire := range expected {
		entity, err := l.s.Reconcile(context.Background(), "SATELIT", fire.ID)
		require.NoError(t, err)
		require.Equal(t, fire.Active, entity.Active)
		if prior, exists := l.identities[fire.ID]; exists {
			require.Equal(t, prior.HazardID, entity.HazardID)
			if prior.Event != nil && entity.Event != nil {
				require.True(t, prior.Event.IngestedAt.Equal(entity.Event.IngestedAt.Time))
			}
		}
		if entity.Event != nil {
			l.identities[fire.ID] = entity
		}
		if fire.Active {
			want, _ := json.Marshal(fire.Event.Attributes)
			got, _ := json.Marshal(entity.Event.Attributes)
			require.JSONEq(t, string(want), string(got))
			require.Equal(t, fire.Event.Latitude, entity.Event.Latitude)
			require.Equal(t, fire.Event.Longitude, entity.Event.Longitude)
		}
	}
}

func TestM6DurableReplayMatchesOracle(t *testing.T) {
	for _, citus := range []bool{false, true} {
		t.Run(fmt.Sprint(citus), func(t *testing.T) {
			l := databaseLab(t, citus)
			c := fixtureConfig(t)
			m, err := wildfire.New(wildfire.Config{Version: 1, AlgorithmVersion: 2, Namespace: c.Namespace, InitialOffset: c.InitialOffset, AllowedLatenessSeconds: c.AllowedLatenessSeconds, ProvinceSHA256: c.ProvinceSHA256, ProvinceJSON: c.ProvinceJSON})
			require.NoError(t, err)
			ops := fixtureOperations(t, "mixed-merge-split")
			for _, op := range ops {
				l.append(t, op)
				_, err = m.Apply(op)
				require.NoError(t, err)
				l.prepare(t, op.Offset)
				l.deliver(t, op.Offset)
				compareOracle(t, l, m)
			}
			// A new engine is constructed from only pinned configuration and its SQL
			// checkpoints; neither Model nor complete history is passed to recovery.
			l.e, err = New(l.s, c)
			require.NoError(t, err)
			compareOracle(t, l, m)
			if citus {
				var n int
				require.NoError(t, l.c.QueryRow(context.Background(), `SELECT count(*) FROM pg_dist_partition WHERE logicalrelid IN ('wildfire_state'::regclass,'wildfire_work'::regclass,'wildfire_output_chunks'::regclass)`).Scan(&n))
				require.Equal(t, 3, n)
			}
		})
	}
}
