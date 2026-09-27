package mapping_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/mapping"
	"aggregator/internal/volcano"
)

type fakeLookup struct {
	ref   volcano.Reference
	found bool
}

func (f fakeLookup) Get(ctx context.Context, volcanoID string) (volcano.Reference, bool, error) {
	return f.ref, f.found, nil
}

func TestVolcanicToHazard_Mapping(t *testing.T) {
	lookup := fakeLookup{ref: volcano.Reference{AreaName: "Gunung Merapi", Latitude: -7.54, Longitude: 110.44}, found: true}
	reportedAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	report := clients.VolcanicReport{
		ReportID: "rep-1", VolcanoID: "merapi", AlertLevel: "Siaga",
		EruptionCount24h: 2, AshColumnHeightM: 1500, ReportedAt: reportedAt,
		Extra: map[string]interface{}{},
	}

	hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
	require.NoError(t, err)

	assert.Equal(t, domain.SourcePVMBG, hazard.Source)
	assert.Equal(t, domain.HazardVolcanic, hazard.HazardType)
	assert.Equal(t, "rep-1", hazard.SourceRefID)
	assert.Equal(t, "Gunung Merapi", hazard.AreaName)
	assert.Equal(t, -7.54, hazard.Latitude)
	assert.Equal(t, 110.44, hazard.Longitude)
	assert.Equal(t, domain.SeveritySiaga, hazard.Severity)
	assert.Equal(t, float64(2), hazard.Attributes["eruption_count_24h"])
	assert.Equal(t, float64(1500), hazard.Attributes["ash_column_height_m"])
}

func TestVolcanicToHazard_AlertLevelMapping(t *testing.T) {
	lookup := fakeLookup{ref: volcano.Reference{}, found: true}
	cases := map[string]domain.Severity{
		"Normal": domain.SeverityNormal, "Waspada": domain.SeverityWaspada,
		"Siaga": domain.SeveritySiaga, "Awas": domain.SeverityAwas,
	}
	for source, want := range cases {
		report := clients.VolcanicReport{ReportID: "r", VolcanoID: "merapi", AlertLevel: source, Extra: map[string]interface{}{}}
		hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
		require.NoError(t, err)
		assert.Equal(t, want, hazard.Severity, "alert level %s", source)
	}
}

func TestVolcanicToHazard_ConfidenceLevelSurvivesIntoAttributes(t *testing.T) {
	lookup := fakeLookup{found: true}
	report := clients.VolcanicReport{
		ReportID: "rep-2", VolcanoID: "merapi", AlertLevel: "Awas",
		Extra: map[string]interface{}{"confidence_level": 0.93},
	}
	hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
	require.NoError(t, err)
	assert.Equal(t, 0.93, hazard.Attributes["confidence_level"])
}

func TestVolcanicToHazard_UnknownFieldSurvivesIntoAttributes(t *testing.T) {
	lookup := fakeLookup{found: true}
	report := clients.VolcanicReport{
		ReportID: "rep-3", VolcanoID: "merapi", AlertLevel: "Normal",
		Extra: map[string]interface{}{"seismic_tremor_count": float64(12)},
	}
	hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
	require.NoError(t, err)
	assert.Equal(t, float64(12), hazard.Attributes["seismic_tremor_count"],
		"unknown fields the Aggregator has never seen before must still reach attributes without a decode failure")
}

func TestVolcanicToHazard_UsesReferenceTableNotHardcodedCoords(t *testing.T) {
	lookup := fakeLookup{ref: volcano.Reference{AreaName: "Gunung Semeru", Latitude: -8.11, Longitude: 112.92}, found: true}
	report := clients.VolcanicReport{ReportID: "rep-4", VolcanoID: "semeru", AlertLevel: "Normal", Extra: map[string]interface{}{}}
	hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
	require.NoError(t, err)
	assert.Equal(t, "Gunung Semeru", hazard.AreaName)
	assert.Equal(t, -8.11, hazard.Latitude)
}
