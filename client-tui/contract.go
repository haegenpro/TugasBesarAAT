package main

import (
	"encoding/json"
	"fmt"
)

type hazardSummary struct {
	HazardID   string `json:"hazard_id"`
	Source     string `json:"source"`
	HazardType string `json:"hazard_type"`
	Severity   string `json:"severity"`
	AreaName   string `json:"area_name"`
}

type hazardList struct {
	Data         []hazardSummary `json:"data"`
	Count        int             `json:"count"`
	AsOf         string          `json:"as_of"`
	SourceStatus json.RawMessage `json:"source_status"`
}

func requireResponseFields(data []byte, fields ...string) error {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(data, &record); err != nil {
		return err
	}
	for _, field := range fields {
		if value, ok := record[field]; !ok || string(value) == "null" {
			return fmt.Errorf("missing required field %q", field)
		}
	}
	return nil
}

func decodeHazardList(data []byte) (hazardList, error) {
	var result hazardList
	if err := requireResponseFields(data, "data", "count", "as_of", "source_status"); err != nil {
		return result, err
	}
	var envelope struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return result, err
	}
	for i, item := range envelope.Data {
		if err := requireResponseFields(item, "hazard_id", "source", "hazard_type", "severity", "area_name"); err != nil {
			return result, fmt.Errorf("hazard %d: %w", i, err)
		}
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	if result.AsOf == "" || result.Count != len(result.Data) {
		return result, fmt.Errorf("invalid hazard list metadata")
	}
	return result, nil
}
