package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"pvmbg-mock/internal/domain"
	"pvmbg-mock/internal/history"
	"pvmbg-mock/internal/state"
	"pvmbg-mock/internal/timeutil"
)

func invoke(t *testing.T, fn gin.HandlerFunc, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, path, nil)
	fn(c)
	return w
}
func readDelta(t *testing.T, h *Handler, since string) []domain.VolcanicReport {
	t.Helper()
	w := invoke(t, h.VolcanicReports, http.MethodGet, "/volcanic-reports?since="+since)
	require.Equal(t, 200, w.Code, w.Body.String())
	var rows []domain.VolcanicReport
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rows))
	return rows
}
func testArchivedSource(t *testing.T) (*Handler, *history.Archive) {
	t.Helper()
	confidence := 0.8
	h := New([]domain.VolcanicReport{{ReportID: "old", VolcanoID: "merapi", ReportedAt: timeutil.New(time.Now().Add(-24 * time.Hour)), AlertLevel: domain.AlertWaspada, ConfidenceLevel: &confidence}}, state.New(), 0, 0)
	a, err := history.Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { a.DB.Close() })
	require.NoError(t, h.AttachArchive(a))
	return h, a
}

func TestSinceIncludesOldReportUpdatesAndSkipsUnchangedReports(t *testing.T) {
	h, a := testArchivedSource(t)
	before := h.Reports[0]
	cursor := timeutil.FormatISOMilli(before.UpdatedAt.Time)
	require.Empty(t, readDelta(t, h, cursor))
	changed := before
	changed.AshColumnHeightM = 1500
	h.AddReport(changed)
	delta := readDelta(t, h, cursor)
	require.Len(t, delta, 1)
	require.Equal(t, before.ReportID, delta[0].ReportID)
	require.Equal(t, timeutil.FormatISOMilli(before.ReportedAt.Time), timeutil.FormatISOMilli(delta[0].ReportedAt.Time))
	require.True(t, delta[0].UpdatedAt.After(before.UpdatedAt.Time))
	require.Len(t, h.Reports, 1)
	head, err := a.Head()
	require.NoError(t, err)
	h.AddReport(changed)
	after, err := a.Head()
	require.NoError(t, err)
	require.Equal(t, head, after)
	require.Empty(t, readDelta(t, h, timeutil.FormatISOMilli(h.Reports[0].UpdatedAt.Time)))
}

func TestSchemaTogglePublishesLiveAndEvictedReportUpdates(t *testing.T) {
	h, a := testArchivedSource(t)
	confidence := 0.9
	for n := 0; n < 101; n++ {
		h.AddReport(domain.VolcanicReport{ReportID: fmt.Sprintf("live-%d", n), VolcanoID: "merapi", ReportedAt: timeutil.New(time.Now()), ConfidenceLevel: &confidence})
	}
	for _, r := range h.Reports {
		require.NotEqual(t, "live-0", r.ReportID)
	}
	cursor := timeutil.FormatISOMilli(h.updatedAt)
	head, err := a.Head()
	require.NoError(t, err)
	w := invoke(t, h.AdminSchemaVersion, http.MethodPost, "/admin/schema-version")
	require.Equal(t, 200, w.Code, w.Body.String())
	delta := readDelta(t, h, cursor)
	require.NotEmpty(t, delta)
	found := false
	for _, r := range delta {
		if r.ReportID == "old" {
			require.NotNil(t, r.ConfidenceLevel)
			found = true
		}
	}
	require.True(t, found, "schema changes to old reports must appear in the ordinary delta")
	page, err := a.Read("volcanic-reports", head, 1000)
	require.NoError(t, err)
	found = false
	for _, item := range page.Data {
		var r domain.VolcanicReport
		require.NoError(t, json.Unmarshal(item.Payload, &r))
		if r.ReportID == "live-0" {
			found = true
			require.True(t, r.UpdatedAt.After(timeutilParse(t, cursor)))
		}
	}
	require.True(t, found, "history-only reports need a new revision, not a silent view change")
	// Disabling the field must also be observable without a full fetch.
	cursor = timeutil.FormatISOMilli(h.updatedAt)
	w = invoke(t, h.AdminSchemaVersion, http.MethodPost, "/admin/schema-version")
	require.Equal(t, 200, w.Code)
	delta = readDelta(t, h, cursor)
	require.NotEmpty(t, delta)
	for _, r := range delta {
		require.Nil(t, r.ConfidenceLevel)
	}
}

func timeutilParse(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := timeutil.ParseISO(s)
	require.NoError(t, err)
	return v
}

func TestFailedSchemaPublicationKeepsPreviousViewAndTimestamps(t *testing.T) {
	h, a := testArchivedSource(t)
	before := h.Reports[0]
	// Reads still succeed, but fail the actual publication transaction.
	_, err := a.DB.Exec(`CREATE TRIGGER reject_publication BEFORE INSERT ON history BEGIN SELECT RAISE(ABORT,'test publication failure'); END`)
	require.NoError(t, err)
	w := invoke(t, h.AdminSchemaVersion, http.MethodPost, "/admin/schema-version")
	require.Equal(t, 503, w.Code)
	require.False(t, h.Toggles.ShowConfidence())
	require.Equal(t, before, h.Reports[0])
	changed := before
	changed.AshColumnHeightM = 1000
	h.AddReport(changed)
	require.Equal(t, before, h.Reports[0])
}
