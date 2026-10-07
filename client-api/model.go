package main

import (
	"encoding/json"
	"fmt"
)

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

func requireJSONFields(data []byte, fields ...string) error {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(data, &record); err != nil {
		return err
	}
	for _, field := range fields {
		value, ok := record[field]
		if !ok || string(value) == "null" {
			return fmt.Errorf("missing required field %q", field)
		}
	}
	return nil
}

func (h *hazard) UnmarshalJSON(data []byte) error {
	if err := requireJSONFields(data, "hazard_id", "source", "source_ref_id", "hazard_type", "severity", "area_name", "latitude", "longitude", "occurred_at", "ingested_at", "attributes"); err != nil {
		return fmt.Errorf("invalid aggregator hazard: %w", err)
	}
	type plain hazard
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.HazardID == "" || decoded.Source == "" || decoded.SourceRefID == "" || decoded.HazardType == "" || decoded.Severity == "" || decoded.OccurredAt == "" || decoded.IngestedAt == "" {
		return fmt.Errorf("invalid aggregator hazard: required string is empty")
	}
	*h = hazard(decoded)
	return nil
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
	State       string `json:"state"`
	LastSuccess string `json:"last_success,omitempty"`
	LastAttempt string `json:"last_attempt,omitempty"`
	StaleSince  string `json:"stale_since,omitempty"`
}

func (status *sourceStatus) UnmarshalJSON(data []byte) error {
	if err := requireJSONFields(data, "state"); err != nil {
		return fmt.Errorf("invalid source status: %w", err)
	}
	type plain sourceStatus
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.State != "fresh" && decoded.State != "stale" && decoded.State != "unavailable" {
		return fmt.Errorf("invalid source status: state %q", decoded.State)
	}
	if decoded.State == "stale" && decoded.StaleSince == "" {
		return fmt.Errorf("invalid source status: stale_since is missing")
	}
	*status = sourceStatus(decoded)
	return nil
}

type aggregatorList struct {
	Data         []hazard                  `json:"data"`
	Count        int                       `json:"count"`
	AsOf         string                    `json:"as_of"`
	SourceStatus map[string]sourceStatus   `json:"source_status"`
	Metadata     map[string]recordMetadata `json:"metadata,omitempty"`
}

func (list *aggregatorList) UnmarshalJSON(data []byte) error {
	if err := requireJSONFields(data, "data", "count", "as_of", "source_status"); err != nil {
		return fmt.Errorf("invalid aggregator list: %w", err)
	}
	type plain aggregatorList
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.AsOf == "" {
		return fmt.Errorf("invalid aggregator list: as_of is empty")
	}
	*list = aggregatorList(decoded)
	return nil
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
