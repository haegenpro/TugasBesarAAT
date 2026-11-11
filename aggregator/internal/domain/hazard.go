package domain

import (
	"time"

	"aggregator/internal/timeutil"
)

type Source string
type HazardType string
type Severity string

const (
	SourceBMKG         Source = "BMKG"
	SourcePVMBG        Source = "PVMBG"
	SourceSatelit      Source = "SATELIT"
	SourceSensorPantai Source = "SENSOR_PANTAI"

	HazardSeismic  HazardType = "SEISMIC"
	HazardVolcanic HazardType = "VOLCANIC"
	HazardWildfire HazardType = "WILDFIRE"
	HazardCoastal  HazardType = "COASTAL"

	SeverityNormal  Severity = "NORMAL"
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

// HazardEvent is the Aggregator's canonical object, per the wire format spec.
type HazardEvent struct {
	HazardID    string                 `json:"hazard_id"`
	Source      Source                 `json:"source"`
	SourceRefID string                 `json:"source_ref_id"`
	HazardType  HazardType             `json:"hazard_type"`
	Severity    Severity               `json:"severity"`
	AreaName    string                 `json:"area_name"`
	Latitude    float64                `json:"latitude"`
	Longitude   float64                `json:"longitude"`
	OccurredAt  timeutil.Time          `json:"occurred_at"`
	IngestedAt  timeutil.Time          `json:"ingested_at"`
	Attributes  map[string]interface{} `json:"attributes"`
}

func Now() time.Time { return time.Now().UTC() }

// Empty values represent an omitted filter. Each service owns its contract checks.
func ValidHazardType(v string) bool {
	return v == "" || v == string(HazardSeismic) || v == string(HazardVolcanic) || v == string(HazardWildfire) || v == string(HazardCoastal)
}
func ValidSeverity(v string) bool {
	return v == "" || v == string(SeverityNormal) || v == string(SeverityWaspada) || v == string(SeveritySiaga) || v == string(SeverityAwas)
}

func ValidSource(v string) bool {
	return v == string(SourceBMKG) || v == string(SourcePVMBG) || v == string(SourceSatelit) || v == string(SourceSensorPantai)
}
