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
	var decoded hazardWire
	if err := unmarshalWire(data, &decoded); err != nil {
		return err
	}
	value, err := decoded.validate()
	if err != nil {
		return err
	}
	*h = value
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
	var decoded sourceStatusWire
	if err := unmarshalWire(data, &decoded); err != nil {
		return err
	}
	value, err := decoded.validate()
	if err != nil {
		return err
	}
	*status = value
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
	var decoded aggregatorListWire
	if err := unmarshalWire(data, &decoded); err != nil {
		return err
	}
	value, err := decoded.validate()
	if err != nil {
		return err
	}
	*list = value
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
