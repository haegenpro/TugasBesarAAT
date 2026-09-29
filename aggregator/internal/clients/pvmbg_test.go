package clients

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeVolcanicReport_WithoutConfidenceLevel(t *testing.T) {
	raw := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(`{
		"report_id": "rep-1", "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": 2, "ash_column_height_m": 1500,
		"reported_at": "2026-09-01T00:00:00.000Z"
	}`), &raw))

	r, err := decodeVolcanicReport(raw)
	require.NoError(t, err)
	assert.Equal(t, "rep-1", r.ReportID)
	assert.Equal(t, "merapi", r.VolcanoID)
	assert.NotContains(t, r.Extra, "confidence_level")
}

func TestDecodeVolcanicReport_WithConfidenceLevel(t *testing.T) {
	raw := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(`{
		"report_id": "rep-2", "volcano_id": "merapi", "alert_level": "Awas",
		"eruption_count_24h": 5, "ash_column_height_m": 3000,
		"reported_at": "2026-09-01T00:00:00.000Z", "confidence_level": 0.93
	}`), &raw))

	r, err := decodeVolcanicReport(raw)
	require.NoError(t, err)
	assert.Equal(t, 0.93, r.Extra["confidence_level"])
}

func TestDecodeVolcanicReport_UnknownFieldSurvives(t *testing.T) {
	raw := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(`{
		"report_id": "rep-3", "volcano_id": "merapi", "alert_level": "Normal",
		"eruption_count_24h": 0, "ash_column_height_m": 0,
		"reported_at": "2026-09-01T00:00:00.000Z", "seismic_tremor_count": 12
	}`), &raw))

	r, err := decodeVolcanicReport(raw)
	require.NoError(t, err)
	assert.Equal(t, float64(12), r.Extra["seismic_tremor_count"], "genuinely unknown fields must pass through, not be dropped")
}
