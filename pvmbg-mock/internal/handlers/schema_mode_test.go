package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
	"pvmbg-mock/internal/avrowire"
	"pvmbg-mock/internal/history"
	"pvmbg-mock/internal/seed"
	"pvmbg-mock/internal/state"
)

func TestBreakingSchemaModeAndOriginalHistory(t *testing.T) {
	h := New(seed.Build(), state.New(), 0, 0)
	a, err := history.Open(":memory:")
	require.NoError(t, err)
	defer a.DB.Close()
	require.NoError(t, h.AttachArchive(a))
	r := gin.New()
	r.POST("/admin/schema-version", h.AdminSchemaVersion)
	r.GET("/volcanic-reports", h.VolcanicReports)
	r.GET("/history", h.History)
	call := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	require.Equal(t, 400, call("POST", "/admin/schema-version", `{"mode":"typo"}`).Code)
	require.Equal(t, 200, call("POST", "/admin/schema-version", `{"mode":"breaking"}`).Code)
	w := call("GET", "/volcanic-reports", "")
	require.Equal(t, 200, w.Code)
	var records []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &records))
	require.NotEmpty(t, records)
	for _, record := range records {
		require.Contains(t, record, "level")
		require.NotContains(t, record, "alert_level")
		require.NotContains(t, record, "ash_column_height_m")
		require.IsType(t, "", record["eruption_count_24h"])
	}
	w = call("GET", "/history?entity=volcanic-reports", "")
	require.Equal(t, 200, w.Code)
	var page map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &page))
	text, err := avrowire.HistorySchema(volcanicBreaking, "VolcanicHistoryPage")
	require.NoError(t, err)
	codec, err := goavro.NewCodec(text)
	require.NoError(t, err)
	jsonCodec, err := goavro.NewCodecForStandardJSON(text)
	require.NoError(t, err)
	_, err = avrowire.EncodeJSON(&avrowire.Schema{ID: 123, Text: text, Codec: codec, JSONCodec: jsonCodec}, page)
	require.NoError(t, err, "the incompatible history must still be valid under its writer schema")
	before := records[len(records)-1]["report_id"]
	require.Equal(t, 200, call("POST", "/admin/schema-version", `{"mode":"v1"}`).Code)
	w = call("GET", "/volcanic-reports", "")
	require.Equal(t, http.StatusOK, w.Code)
	records = nil
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &records))
	for _, record := range records {
		require.Contains(t, record, "alert_level")
		require.NotContains(t, record, "level")
	}
	require.Equal(t, before, records[len(records)-1]["report_id"], "changing the wire contract must not rewrite source identity")
}
