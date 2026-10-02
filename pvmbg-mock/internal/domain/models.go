package domain

import "pvmbg-mock/internal/timeutil"

// AlertLevel preserves PVMBG's own source-domain spelling on the wire.
type AlertLevel string

const (
	AlertNormal  AlertLevel = "Normal"
	AlertWaspada AlertLevel = "Waspada"
	AlertSiaga   AlertLevel = "Siaga"
	AlertAwas    AlertLevel = "Awas"
)

// VolcanicReport is the PVMBG source record. ConfidenceLevel is the
// schema-evolution field: it may already be populated in stored data, but is
// only serialized onto the wire once the admin schema-version toggle is on.
type VolcanicReport struct {
	ReportID         string        `json:"report_id"`
	VolcanoID        string        `json:"volcano_id"`
	AlertLevel       AlertLevel    `json:"alert_level"`
	EruptionCount24h int           `json:"eruption_count_24h"`
	AshColumnHeightM float64       `json:"ash_column_height_m"`
	ReportedAt       timeutil.Time `json:"reported_at"`
	ConfidenceLevel  *float64      `json:"confidence_level,omitempty"`
}

// WireCopy returns a copy of the report with ConfidenceLevel hidden unless
// showConfidence is true, so JSON marshaling naturally omits the field.
func (v VolcanicReport) WireCopy(showConfidence bool) VolcanicReport {
	if !showConfidence {
		v.ConfidenceLevel = nil
	}
	return v
}
