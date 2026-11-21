package main

import "fmt"

type summaryHazardWire struct {
	HazardID   *string `json:"hazard_id,case:strict"`
	Source     *string `json:"source,case:strict"`
	HazardType *string `json:"hazard_type,case:strict"`
	Severity   *string `json:"severity,case:strict"`
	AreaName   *string `json:"area_name,case:strict"`
	OccurredAt *string `json:"occurred_at,case:strict"`
	IngestedAt *string `json:"ingested_at,case:strict"`
}

func (w summaryHazardWire) validate() (summaryHazard, error) {
	if err := checkRequiredFields(requiredField{"hazard_id", w.HazardID != nil},
		requiredField{"source", w.Source != nil}, requiredField{"hazard_type", w.HazardType != nil},
		requiredField{"severity", w.Severity != nil}, requiredField{"area_name", w.AreaName != nil},
		requiredField{"occurred_at", w.OccurredAt != nil}, requiredField{"ingested_at", w.IngestedAt != nil}); err != nil {
		return summaryHazard{}, fmt.Errorf("invalid aggregator summary: %w", err)
	}
	h := summaryHazard{*w.HazardID, *w.Source, *w.HazardType, *w.Severity,
		*w.AreaName, *w.OccurredAt, *w.IngestedAt}
	if h.HazardID == "" || h.Source == "" || h.HazardType == "" || h.Severity == "" || h.OccurredAt == "" || h.IngestedAt == "" ||
		!validSource(h.Source) || !validHazardType(h.HazardType) || !validSeverity(h.Severity) {
		return summaryHazard{}, fmt.Errorf("invalid aggregator summary identity, time or enum")
	}
	return h, nil
}

type aggregatorSummaryList struct {
	Data             []summaryHazard
	Count            int
	AsOf             string
	NextCursor       *string
	Partial          bool
	UnavailableUnits []string
	SourceStatus     map[string]sourceStatus
	Metadata         map[string]recordMetadata
}

func (list *aggregatorSummaryList) UnmarshalJSON(data []byte) error {
	var wire struct {
		Data             []summaryHazardWire         `json:"data,case:strict"`
		Count            *int                        `json:"count,case:strict"`
		AsOf             *string                     `json:"as_of,case:strict"`
		SourceStatus     map[string]sourceStatusWire `json:"source_status,case:strict"`
		Metadata         map[string]recordMetadata   `json:"metadata,omitempty"`
		NextCursor       *string                     `json:"next_cursor"`
		Partial          bool                        `json:"partial"`
		UnavailableUnits []string                    `json:"unavailable_units"`
	}
	if err := unmarshalWire(data, &wire); err != nil {
		return err
	}
	if wire.Data == nil {
		return fmt.Errorf("invalid aggregator summary: missing data")
	}
	// Reuse envelope/status validation without constructing or decoding raw records.
	envelope, err := (aggregatorListWire{Data: []hazardWire{}, Count: wire.Count,
		AsOf: wire.AsOf, SourceStatus: wire.SourceStatus, Metadata: wire.Metadata,
		NextCursor: wire.NextCursor, Partial: wire.Partial, UnavailableUnits: wire.UnavailableUnits}).validate()
	if err != nil {
		return err
	}
	if envelope.Count != len(wire.Data) {
		return fmt.Errorf("invalid aggregator summary count")
	}
	value := aggregatorSummaryList{Data: make([]summaryHazard, len(wire.Data)), Count: envelope.Count,
		AsOf: envelope.AsOf, SourceStatus: envelope.SourceStatus, Metadata: envelope.Metadata,
		NextCursor: envelope.NextCursor, Partial: envelope.Partial, UnavailableUnits: envelope.UnavailableUnits}
	for i, record := range wire.Data {
		if value.Data[i], err = record.validate(); err != nil {
			return err
		}
	}
	*list = value
	return nil
}
