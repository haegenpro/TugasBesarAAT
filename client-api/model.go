package main

type principal struct {
	ClientID string `json:"client_id"`
	Role     string `json:"role"`
	Scope    string `json:"scope"`
}

type hazard struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  string         `json:"occurred_at"`
	IngestedAt  string         `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

type summaryHazard struct {
	HazardID   string `json:"hazard_id"`
	Source     string `json:"source"`
	HazardType string `json:"hazard_type"`
	Severity   string `json:"severity"`
	AreaName   string `json:"area_name"`
	OccurredAt string `json:"occurred_at"`
	IngestedAt string `json:"ingested_at"`
}

func (h hazard) summary() summaryHazard {
	return summaryHazard{HazardID: h.HazardID, Source: h.Source, HazardType: h.HazardType, Severity: h.Severity, AreaName: h.AreaName, OccurredAt: h.OccurredAt, IngestedAt: h.IngestedAt}
}

type sourceStatus struct {
	State      string `json:"state"`
	StaleSince string `json:"stale_since,omitempty"`
}

type aggregatorList struct {
	Data         []hazard                `json:"data"`
	Count        int                     `json:"count"`
	AsOf         string                  `json:"as_of"`
	SourceStatus map[string]sourceStatus `json:"source_status"`
}

func detailStatus(h hazard) sourceStatus {
	status := sourceStatus{State: "fresh"}
	if h.Attributes["stale"] == true {
		status.State = "stale"
		if value, ok := h.Attributes["stale_since"].(string); ok {
			status.StaleSince = value
		}
	}
	return status
}
