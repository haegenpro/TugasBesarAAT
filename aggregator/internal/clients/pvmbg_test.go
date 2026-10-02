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
	assert.Equal(t, "Siaga", r.AlertLevel)
	assert.NotContains(t, r.Attributes, "confidence_level")
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
	assert.Equal(t, "rep-2", r.ReportID)
	assert.Equal(t, "Awas", r.AlertLevel)
	assert.Equal(t, 0.93, r.Attributes["confidence_level"])
}

func TestDecodeVolcanicReport_UnmappedFieldBecomesAttribute(t *testing.T) {
	raw := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(`{
		"report_id": "rep-3", "volcano_id": "merapi", "alert_level": "Normal",
		"eruption_count_24h": 0, "ash_column_height_m": 0,
		"reported_at": "2026-09-01T00:00:00.000Z", "seismic_tremor_count": 12
	}`), &raw))

	r, err := decodeVolcanicReport(raw)
	require.NoError(t, err)
	assert.Equal(t, "rep-3", r.ReportID)
	assert.Equal(t, "Normal", r.AlertLevel)
	assert.Equal(t, float64(12), r.Attributes["seismic_tremor_count"])
}

func TestDecodeVolcanicReport_FractionalEruptionCountFails(t *testing.T) {
	raw := map[string]interface{}{
		"report_id": "rep-5", "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": float64(1.5), "ash_column_height_m": float64(1500),
		"reported_at": "2026-09-01T00:00:00.000Z",
	}
	_, err := decodeVolcanicReport(raw)
	require.ErrorContains(t, err, `"eruption_count_24h"`)
}

func TestDecodeVolcanicReport_InvalidAlertLevelFails(t *testing.T) {
	raw := map[string]interface{}{
		"report_id": "rep-5", "volcano_id": "merapi", "alert_level": "Unknown",
		"eruption_count_24h": float64(1), "ash_column_height_m": float64(1500),
		"reported_at": "2026-09-01T00:00:00.000Z",
	}
	_, err := decodeVolcanicReport(raw)
	require.ErrorContains(t, err, "invalid alert_level")
}

func TestDecodeVolcanicReport_RenamedRequiredFieldFails(t *testing.T) {
	raw := map[string]interface{}{
		"report_code": "rep-4", "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": float64(2), "ash_column_height_m": float64(1500),
		"reported_at": "2026-09-01T00:00:00.000Z",
	}
	_, err := decodeVolcanicReport(raw)
	require.ErrorContains(t, err, `"report_id"`)
}
