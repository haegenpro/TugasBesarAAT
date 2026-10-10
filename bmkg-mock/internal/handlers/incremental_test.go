package handlers

import (
	"bmkg-mock/internal/domain"
	"bmkg-mock/internal/history"
	"bmkg-mock/internal/timeutil"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func readDelta(t *testing.T, fn gin.HandlerFunc, since string) []map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/?since="+since, nil)
	fn(c)
	require.Equal(t, 200, w.Code, w.Body.String())
	var records []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &records))
	return records
}
func TestSinceUsesInclusiveOccurrenceForEventAndWarning(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	ev := domain.SeismicEvent{EventID: "event", OccurredAt: timeutil.New(at), Magnitude: 5, PotentialTsunami: true}
	warning := domain.TsunamiWarning{WarningID: "warning", RelatedEventID: ev.EventID, ThreatLevel: domain.ThreatWaspada, EstimatedArrival: timeutil.New(time.Now().Add(time.Hour)), AffectedZones: []string{"Coast"}}
	h := New(nil, nil, 0)
	a, err := history.Open(":memory:")
	require.NoError(t, err)
	defer a.DB.Close()
	require.NoError(t, h.AttachArchive(a))
	h.AddEvent(ev, &warning)
	for _, fn := range []gin.HandlerFunc{h.SeismicEvents, h.TsunamiWarnings} {
		rows := readDelta(t, fn, timeutil.FormatISOMilli(at))
		require.Len(t, rows, 1)
		require.NotContains(t, rows[0], "updated_at")
		require.Empty(t, readDelta(t, fn, timeutil.FormatISOMilli(at.Add(time.Millisecond))))
	}
	head, err := a.Head()
	require.NoError(t, err)
	changed := ev
	changed.Magnitude = 7
	warning.ThreatLevel = domain.ThreatAwas
	h.AddEvent(changed, &warning)
	require.Equal(t, ev, h.Events[0])
	require.Equal(t, domain.ThreatWaspada, h.Warnings[0].ThreatLevel)
	after, err := a.Head()
	require.NoError(t, err)
	require.Equal(t, head, after)
}
func TestRejectsOrphanWarningAndFalseToTrueMutation(t *testing.T) {
	h := New(nil, nil, 0)
	ev := domain.SeismicEvent{EventID: "event", PotentialTsunami: true}
	w := domain.TsunamiWarning{WarningID: "warning", RelatedEventID: "another"}
	h.AddEvent(ev, &w)
	require.Empty(t, h.Events)
	require.Empty(t, h.Warnings)
	ev.PotentialTsunami = false
	h.AddEvent(ev, nil)
	ev.PotentialTsunami = true
	w.RelatedEventID = ev.EventID
	h.AddEvent(ev, &w)
	require.False(t, h.Events[0].PotentialTsunami)
	require.Empty(t, h.Warnings)
}
func TestFailedArchiveWriteExposesNeitherEventNorWarning(t *testing.T) {
	h := New(nil, nil, 0)
	a, err := history.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, h.AttachArchive(a))
	_, err = a.DB.Exec(`CREATE TRIGGER reject_warning BEFORE INSERT ON history WHEN NEW.entity='tsunami-warnings' BEGIN SELECT RAISE(ABORT,'test failure'); END`)
	require.NoError(t, err)
	defer a.DB.Close()
	ev := domain.SeismicEvent{EventID: "event", PotentialTsunami: true}
	w := domain.TsunamiWarning{WarningID: "warning", RelatedEventID: ev.EventID}
	h.AddEvent(ev, &w)
	require.Empty(t, h.Events)
	require.Empty(t, h.Warnings)
	head, err := a.Head()
	require.NoError(t, err)
	require.Zero(t, head)
}

func TestHistoryDoesNotExposeLegacyUpdatedAt(t *testing.T) {
	h := New(nil, nil, 0)
	a, err := history.Open(":memory:")
	require.NoError(t, err)
	defer a.DB.Close()
	require.NoError(t, h.AttachArchive(a))
	legacy := map[string]any{"event_id": "legacy", "magnitude": 5.0, "depth_km": 10.0, "epicenter_lat": -6.0, "epicenter_lon": 106.0, "region_name": "Coast", "occurred_at": "2026-10-10T00:00:00Z", "potential_tsunami": false, "updated_at": "2026-10-10T01:00:00Z"}
	require.NoError(t, a.Append(history.Record{Entity: "seismic-events", ID: "legacy", Payload: legacy}))
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/history?entity=seismic-events", nil)
	h.History(c)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "legacy")
	require.NotContains(t, w.Body.String(), "updated_at")
}
