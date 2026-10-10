package handlers

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"pvmbg-mock/internal/domain"
	"pvmbg-mock/internal/history"
	"pvmbg-mock/internal/state"
	"pvmbg-mock/internal/timeutil"
	"testing"
	"time"
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
	h := New([]domain.VolcanicReport{{ReportID: "old", VolcanoID: "merapi", ReportedAt: timeutil.New(time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Millisecond)), AlertLevel: domain.AlertWaspada, ConfidenceLevel: &confidence}}, state.New(), 0, 0)
	a, err := history.Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { a.DB.Close() })
	require.NoError(t, h.AttachArchive(a))
	return h, a
}
func TestSinceIsInclusiveReportedAtAndReportsAreImmutable(t *testing.T) {
	h, a := testArchivedSource(t)
	before := h.Reports[0]
	cursor := timeutil.FormatISOMilli(before.ReportedAt.Time)
	require.Len(t, readDelta(t, h, cursor), 1)
	require.Empty(t, readDelta(t, h, timeutil.FormatISOMilli(before.ReportedAt.Add(time.Millisecond))))
	changed := before
	changed.AshColumnHeightM = 1500
	h.AddReport(changed)
	require.Equal(t, before, h.Reports[0])
	head, err := a.Head()
	require.NoError(t, err)
	require.Equal(t, int64(1), head)
	w := invoke(t, h.VolcanicReports, http.MethodGet, "/volcanic-reports")
	require.NotContains(t, w.Body.String(), "updated_at")
}
func TestSchemaToggleDoesNotReviseHistoricReports(t *testing.T) {
	h, a := testArchivedSource(t)
	before := h.Reports[0]
	cursor := time.Now().UTC().Add(-time.Second)
	head, err := a.Head()
	require.NoError(t, err)
	w := invoke(t, h.AdminSchemaVersion, http.MethodPost, "/admin/schema-version")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, before, h.Reports[0])
	delta := readDelta(t, h, timeutil.FormatISOMilli(cursor))
	require.Len(t, delta, 1)
	require.NotNil(t, delta[0].ConfidenceLevel)
	page, err := a.Read("volcanic-reports", head, 1000)
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	var added domain.VolcanicReport
	require.NoError(t, json.Unmarshal(page.Data[0].Payload, &added))
	require.NotEqual(t, before.ReportID, added.ReportID)
	head, err = a.Head()
	require.NoError(t, err)
	w = invoke(t, h.AdminSchemaVersion, http.MethodPost, "/admin/schema-version")
	require.Equal(t, 200, w.Code)
	after, err := a.Head()
	require.NoError(t, err)
	require.Equal(t, head, after)
	require.Equal(t, before, h.Reports[0])
	for _, r := range readDelta(t, h, "") {
		require.Nil(t, r.ConfidenceLevel)
	}
}
func TestFailedSchemaPublicationKeepsPreviousView(t *testing.T) {
	h, a := testArchivedSource(t)
	before := h.Reports[0]
	_, err := a.DB.Exec(`CREATE TRIGGER reject_publication BEFORE INSERT ON history BEGIN SELECT RAISE(ABORT,'test publication failure'); END`)
	require.NoError(t, err)
	w := invoke(t, h.AdminSchemaVersion, http.MethodPost, "/admin/schema-version")
	require.Equal(t, 503, w.Code)
	require.False(t, h.Toggles.ShowConfidence())
	require.Equal(t, before, h.Reports[0])
	newReport := before
	newReport.ReportID = "new"
	h.AddReport(newReport)
	require.Len(t, h.Reports, 1)
}
