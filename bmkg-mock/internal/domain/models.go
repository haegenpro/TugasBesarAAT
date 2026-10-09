package domain

import "bmkg-mock/internal/timeutil"

// ThreatLevel preserves BMKG's own source-domain spelling on the wire.
type ThreatLevel string

const (
	ThreatWaspada ThreatLevel = "Waspada"
	ThreatSiaga   ThreatLevel = "Siaga"
	ThreatAwas    ThreatLevel = "Awas"
)

type SeismicEvent struct {
	EventID          string        `json:"event_id"`
	Magnitude        float64       `json:"magnitude"`
	DepthKM          float64       `json:"depth_km"`
	EpicenterLat     float64       `json:"epicenter_lat"`
	EpicenterLon     float64       `json:"epicenter_lon"`
	RegionName       string        `json:"region_name"`
	OccurredAt       timeutil.Time `json:"occurred_at"`
	UpdatedAt        timeutil.Time `json:"updated_at"`
	PotentialTsunami bool          `json:"potential_tsunami"`
}

type TsunamiWarning struct {
	WarningID        string        `json:"warning_id"`
	RelatedEventID   string        `json:"related_event_id"`
	ThreatLevel      ThreatLevel   `json:"threat_level"`
	AffectedZones    []string      `json:"affected_zones"`
	EstimatedArrival timeutil.Time `json:"estimated_arrival"`
	UpdatedAt        timeutil.Time `json:"updated_at"`
}
