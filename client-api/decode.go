package main

import (
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
)

// Keep the existing JSON value semantics, but reject ambiguous duplicate keys.
// Otherwise repeated data arrays can reuse struct elements and let an incomplete
// record inherit required fields from a preceding array during typed decoding.
func unmarshalWire(data []byte, output any) error {
	return jsonv2.Unmarshal(data, output, json.DefaultOptionsV1(), jsontext.AllowDuplicateNames(false))
}

// Wire types have no custom unmarshaling methods: the list and its nested
// records are decoded together, then validated without parsing JSON again.
// Pointers distinguish absent/null fields from valid empty strings and zeroes.
// Go 1.27's case:strict retains the exact required names checked by the old
// raw-message maps; additive unknown fields remain ignored.
type hazardWire struct {
	HazardID    *string        `json:"hazard_id,case:strict"`
	Source      *string        `json:"source,case:strict"`
	SourceRefID *string        `json:"source_ref_id,case:strict"`
	HazardType  *string        `json:"hazard_type,case:strict"`
	Severity    *string        `json:"severity,case:strict"`
	AreaName    *string        `json:"area_name,case:strict"`
	Latitude    *float64       `json:"latitude,case:strict"`
	Longitude   *float64       `json:"longitude,case:strict"`
	OccurredAt  *string        `json:"occurred_at,case:strict"`
	IngestedAt  *string        `json:"ingested_at,case:strict"`
	Attributes  map[string]any `json:"attributes,case:strict"`
}

type sourceStatusWire struct {
	State       *string `json:"state,case:strict"`
	LastSuccess string  `json:"last_success,omitempty"`
	LastAttempt string  `json:"last_attempt,omitempty"`
	StaleSince  string  `json:"stale_since,omitempty"`
}

type aggregatorListWire struct {
	Data         []hazardWire                `json:"data,case:strict"`
	Count        *int                        `json:"count,case:strict"`
	AsOf         *string                     `json:"as_of,case:strict"`
	SourceStatus map[string]sourceStatusWire `json:"source_status,case:strict"`
	Metadata     map[string]recordMetadata   `json:"metadata,omitempty"`
}

type requiredField struct {
	name    string
	present bool
}

func checkRequiredFields(fields ...requiredField) error {
	for _, field := range fields {
		if !field.present {
			return fmt.Errorf("missing required field %q", field.name)
		}
	}
	return nil
}

func (w hazardWire) validate() (hazard, error) {
	if err := checkRequiredFields(
		requiredField{"hazard_id", w.HazardID != nil}, requiredField{"source", w.Source != nil},
		requiredField{"source_ref_id", w.SourceRefID != nil}, requiredField{"hazard_type", w.HazardType != nil},
		requiredField{"severity", w.Severity != nil}, requiredField{"area_name", w.AreaName != nil},
		requiredField{"latitude", w.Latitude != nil}, requiredField{"longitude", w.Longitude != nil},
		requiredField{"occurred_at", w.OccurredAt != nil}, requiredField{"ingested_at", w.IngestedAt != nil},
		requiredField{"attributes", w.Attributes != nil},
	); err != nil {
		return hazard{}, fmt.Errorf("invalid aggregator hazard: %w", err)
	}
	h := hazard{HazardID: *w.HazardID, Source: *w.Source, SourceRefID: *w.SourceRefID,
		HazardType: *w.HazardType, Severity: *w.Severity, AreaName: *w.AreaName,
		Latitude: *w.Latitude, Longitude: *w.Longitude, OccurredAt: *w.OccurredAt,
		IngestedAt: *w.IngestedAt, Attributes: w.Attributes}
	if h.HazardID == "" || h.Source == "" || h.SourceRefID == "" || h.HazardType == "" || h.Severity == "" || h.OccurredAt == "" || h.IngestedAt == "" {
		return hazard{}, fmt.Errorf("invalid aggregator hazard: required string is empty")
	}
	return h, nil
}

func (w sourceStatusWire) validate() (sourceStatus, error) {
	if w.State == nil {
		return sourceStatus{}, fmt.Errorf("invalid source status: missing required field %q", "state")
	}
	s := sourceStatus{State: *w.State, LastSuccess: w.LastSuccess, LastAttempt: w.LastAttempt, StaleSince: w.StaleSince}
	if s.State != "fresh" && s.State != "stale" && s.State != "unavailable" {
		return sourceStatus{}, fmt.Errorf("invalid source status: state %q", s.State)
	}
	if s.State == "stale" && s.StaleSince == "" {
		return sourceStatus{}, fmt.Errorf("invalid source status: stale_since is missing")
	}
	return s, nil
}

func (w aggregatorListWire) validate() (aggregatorList, error) {
	if err := checkRequiredFields(requiredField{"data", w.Data != nil}, requiredField{"count", w.Count != nil},
		requiredField{"as_of", w.AsOf != nil}, requiredField{"source_status", w.SourceStatus != nil}); err != nil {
		return aggregatorList{}, fmt.Errorf("invalid aggregator list: %w", err)
	}
	if *w.AsOf == "" {
		return aggregatorList{}, fmt.Errorf("invalid aggregator list: as_of is empty")
	}
	list := aggregatorList{Data: make([]hazard, len(w.Data)), Count: *w.Count, AsOf: *w.AsOf,
		SourceStatus: make(map[string]sourceStatus, len(w.SourceStatus)), Metadata: w.Metadata}
	for i, wire := range w.Data {
		var err error
		if list.Data[i], err = wire.validate(); err != nil {
			return aggregatorList{}, err
		}
	}
	for name, wire := range w.SourceStatus {
		value, err := wire.validate()
		if err != nil {
			return aggregatorList{}, err
		}
		list.SourceStatus[name] = value
	}
	return list, nil
}
