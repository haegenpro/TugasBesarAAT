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
	assert.Equal(t, 2, hazard.Attributes["eruption_count_24h"])
	assert.Equal(t, float64(1500), hazard.Attributes["ash_column_height_m"])
}

func TestVolcanicToHazard_AlertLevelMapping(t *testing.T) {
	lookup := fakeLookup{ref: volcano.Reference{}, found: true}
	cases := map[string]domain.Severity{
		"Normal": domain.SeverityNormal, "Waspada": domain.SeverityWaspada,
		"Siaga": domain.SeveritySiaga, "Awas": domain.SeverityAwas,
	}
	for source, want := range cases {
		report := clients.VolcanicReport{ReportID: "r", VolcanoID: "merapi", AlertLevel: source}
		hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
		require.NoError(t, err)
		assert.Equal(t, want, hazard.Severity, "alert level %s", source)
	}
}

func TestVolcanicToHazard_ContainsOnlyMappedAttributes(t *testing.T) {
	lookup := fakeLookup{found: true}
	report := clients.VolcanicReport{
		ReportID: "rep-2", VolcanoID: "merapi", AlertLevel: "Awas",
		EruptionCount24h: 3, AshColumnHeightM: 1200,
	}
	hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"eruption_count_24h": 3, "ash_column_height_m": float64(1200)}, hazard.Attributes)
}

func TestVolcanicToHazard_MapsConfidenceWhenPresent(t *testing.T) {
	lookup := fakeLookup{found: true}
	report := clients.VolcanicReport{
		ReportID: "rep-3", VolcanoID: "merapi", AlertLevel: "Awas",
		EruptionCount24h: 3, AshColumnHeightM: 1200,
		Attributes: map[string]any{"confidence_level": 0.93},
	}
	hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
	require.NoError(t, err)
	assert.Equal(t, 0.93, hazard.Attributes["confidence_level"])
}

func TestVolcanicToHazard_PreservesUnmappedAttributes(t *testing.T) {
	lookup := fakeLookup{found: true}
	report := clients.VolcanicReport{
		ReportID: "rep-3", VolcanoID: "merapi", AlertLevel: "Awas",
		Attributes: map[string]any{"seismic_tremor_count": float64(12)},
	}
	hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
	require.NoError(t, err)
	assert.Equal(t, float64(12), hazard.Attributes["seismic_tremor_count"])
}

func TestVolcanicToHazard_UnknownVolcanoFails(t *testing.T) {
	lookup := fakeLookup{found: false}
	report := clients.VolcanicReport{ReportID: "rep-4", VolcanoID: "unknown", AlertLevel: "Normal"}
	_, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
	require.ErrorContains(t, err, "unknown volcano_id")
}

func TestVolcanicToHazard_UsesReferenceTableNotHardcodedCoords(t *testing.T) {
	lookup := fakeLookup{ref: volcano.Reference{AreaName: "Gunung Semeru", Latitude: -8.11, Longitude: 112.92}, found: true}
	report := clients.VolcanicReport{ReportID: "rep-4", VolcanoID: "semeru", AlertLevel: "Normal"}
	hazard, err := mapping.VolcanicToHazard(context.Background(), lookup, report)
	require.NoError(t, err)
	assert.Equal(t, "Gunung Semeru", hazard.AreaName)
	assert.Equal(t, -8.11, hazard.Latitude)
}
