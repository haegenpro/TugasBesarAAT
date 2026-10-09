package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bmkg-mock/internal/domain"
	"bmkg-mock/internal/history"
	"bmkg-mock/internal/timeutil"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
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

func TestSinceUsesUpdatesRatherThanOccurrenceOrArrival(t *testing.T) {
	old := time.Now().Add(-24 * time.Hour)
	ev := domain.SeismicEvent{EventID: "old-event", OccurredAt: timeutil.New(old), Magnitude: 5}
	warning := domain.TsunamiWarning{WarningID: "future-warning", RelatedEventID: ev.EventID, ThreatLevel: domain.ThreatWaspada, EstimatedArrival: timeutil.New(time.Now().Add(time.Hour)), AffectedZones: []string{"Coast"}}
	h := New([]domain.SeismicEvent{ev}, []domain.TsunamiWarning{warning}, 0)
	a, err := history.Open(":memory:")
	require.NoError(t, err)
	defer a.DB.Close()
	require.NoError(t, h.AttachArchive(a))
	cursor := timeutil.FormatISOMilli(h.Events[0].UpdatedAt.Time)
	require.Empty(t, readDelta(t, h.SeismicEvents, cursor))
	require.Empty(t, readDelta(t, h.TsunamiWarnings, cursor), "a future arrival must not make an unchanged warning repeat forever")
	ev.Magnitude = 6
	warning.ThreatLevel = domain.ThreatAwas
	h.AddEvent(ev, &warning)
	seismic := readDelta(t, h.SeismicEvents, cursor)
	warnings := readDelta(t, h.TsunamiWarnings, cursor)
	require.Len(t, seismic, 1)
	require.Len(t, warnings, 1)
	require.Equal(t, float64(6), seismic[0]["magnitude"])
	require.Equal(t, "Awas", warnings[0]["threat_level"])
	require.Equal(t, timeutil.FormatISOMilli(old), seismic[0]["occurred_at"])
	require.Len(t, h.Events, 1)
	require.Len(t, h.Warnings, 1)
	head, err := a.Head()
	require.NoError(t, err)
	h.AddEvent(ev, &warning)
	after, err := a.Head()
	require.NoError(t, err)
	require.Equal(t, head, after, "unchanged input must not create an update or history revision")
	require.Empty(t, readDelta(t, h.SeismicEvents, timeutil.FormatISOMilli(h.Events[0].UpdatedAt.Time)))
	// A newly received warning may itself describe an old arrival time.
	warning.WarningID = "late-warning"
	warning.EstimatedArrival = timeutil.New(old)
	before := timeutil.FormatISOMilli(h.Warnings[0].UpdatedAt.Time)
	h.AddEvent(ev, &warning)
	require.Len(t, readDelta(t, h.TsunamiWarnings, before), 1)
}

func TestFailedArchiveWriteDoesNotExposeUpdatedTimestamp(t *testing.T) {
	h := New([]domain.SeismicEvent{{EventID: "event", OccurredAt: timeutil.New(time.Now()), Magnitude: 5}}, nil, 0)
	a, err := history.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, h.AttachArchive(a))
	before := h.Events[0]
	require.NoError(t, a.DB.Close())
	changed := before
	changed.Magnitude = 7
	h.AddEvent(changed, nil)
	require.Equal(t, before, h.Events[0])
}
