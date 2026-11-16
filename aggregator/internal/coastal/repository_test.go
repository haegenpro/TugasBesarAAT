package coastal

import (
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/ownership"
	"aggregator/internal/rawinput"
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func repositoryLab(t *testing.T) (*Repository, canonical.Ownership, *ownership.Authority) {
	db := database(t)
	a, err := ownership.NewAcquisition(db.Pool, 15*time.Second, canonical.Deadline)
	require.NoError(t, err)
	lease, err := a.Acquire(context.Background(), canonical.AcquireOwnership{Scope: "acquisition:" + Source, Holder: "leader-a"})
	require.NoError(t, err)
	return &Repository{Source: &ingestion.SourceStore{DB: db, Durability: labDurability{pool: db.Pool}, Fence: a, Source: Source, Namespace: "coastal-fixture"}, StaleAfter: time.Minute}, lease.Ownership, a
}

func TestSharedStatusServesStandbyAndRejectsStaleLeader(t *testing.T) {
	repo, owner, a := repositoryLab(t)
	ctx := context.Background()
	now, err := repo.Now(ctx)
	require.NoError(t, err)
	leader := &Status{StaleAfter: time.Minute, Observations: &Breaker{}, Stations: &Breaker{}}
	leader.AttemptAt(now, nil)
	leader.PageAt(now, now, now, 1)
	require.NoError(t, repo.SaveStatus(ctx, owner, leader.SnapshotAt(now)))
	standby := &Status{StaleAfter: time.Minute, Observations: &Breaker{}, Stations: &Breaker{}}
	require.Equal(t, "unavailable", standby.Snapshot().State)
	router := gin.New()
	RegisterStatus(router, standby, "internal-token", repo)
	request := httptest.NewRequest("GET", "/internal/coastal/status", nil)
	request.Header.Set("Authorization", "Bearer internal-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	require.Equal(t, 200, response.Code)
	var v Snapshot
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &v))
	require.Equal(t, "fresh", v.State)
	require.NoError(t, a.Release(ctx, owner))
	lease, err := a.Acquire(ctx, canonical.AcquireOwnership{Scope: owner.Scope, Holder: "leader-b"})
	require.NoError(t, err)
	require.Error(t, repo.SaveStatus(ctx, owner, Snapshot{State: "unavailable"}))
	standby.Restore(v)
	standby.PageAt(now.Add(-2*time.Minute), now, now, 0)
	// Persist a previously fresh snapshot that has aged out while no leader ran.
	old := now.Add(-2 * time.Minute)
	v.LastNewObservation = &old
	require.NoError(t, repo.SaveStatus(ctx, lease.Ownership, v))
	expired, err := repo.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "stale", expired.State)
	now, err = repo.Now(ctx)
	require.NoError(t, err)
	standby.Restore(expired)
	standby.AttemptAt(now, nil)
	standby.PageAt(now, now, now, 1)
	require.NoError(t, repo.SaveStatus(ctx, lease.Ownership, standby.SnapshotAt(now)))
	recovered, err := repo.Status(ctx)
	require.NoError(t, err)
	require.Equal(t, "fresh", recovered.State)
}

func TestExpectedWarningsPrecedeDeliveryAndRetainFirstReceipt(t *testing.T) {
	repo, owner, _ := repositoryLab(t)
	ctx := context.Background()
	require.NoError(t, repo.StartCoverage(ctx, owner))
	now, err := repo.Now(ctx)
	require.NoError(t, err)
	e := envelope(observation)
	e.ReceivedAt = now.Format(time.RFC3339Nano)
	require.NoError(t, repo.Expect(ctx, owner, e))
	e.ReceivedAt = now.Add(time.Minute).Format(time.RFC3339Nano)
	require.NoError(t, repo.Expect(ctx, owner, e))
	require.NoError(t, repo.Reject(ctx, e, "unknown station"))
	page, err := repo.Expectations(ctx, now.Add(-time.Hour), 0, 0, time.Time{})
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.True(t, page.Items[0].Rejected)
	require.WithinDuration(t, now, page.Items[0].FirstReceivedAt, time.Microsecond)
	require.NotNil(t, page.CoverageSince)
	stale := owner
	stale.Epoch++
	require.Error(t, repo.Expect(ctx, stale, e))
	router := gin.New()
	RegisterExpectations(router, repo, "slo-only")
	unauth := httptest.NewRecorder()
	router.ServeHTTP(unauth, httptest.NewRequest("GET", "/internal/coastal/warnings/expected?since="+now.Add(-time.Hour).Format(time.RFC3339Nano), nil))
	require.Equal(t, http.StatusUnauthorized, unauth.Code)
	invalid := envelope(strings.Replace(observation, `"observation_id":"obs-000120"`, `"observation_id":123`, 1))
	invalid.MessageID = rawinput.ID(invalid)
	_, ok := Candidate(invalid)
	require.True(t, ok, "invalid critical observations must remain countable")
}
