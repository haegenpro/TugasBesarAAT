package mapping_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/mapping"
)

func TestSeismicSeverity_BelowFive(t *testing.T) {
	assert.Equal(t, domain.SeverityNormal, mapping.SeismicSeverity(4.9, nil))
}

func TestSeismicSeverity_ExactlyFive(t *testing.T) {
	assert.Equal(t, domain.SeverityWaspada, mapping.SeismicSeverity(5.0, nil))
}

func TestSeismicSeverity_BelowSixFive(t *testing.T) {
	assert.Equal(t, domain.SeverityWaspada, mapping.SeismicSeverity(6.4, nil))
}

func TestSeismicSeverity_ExactlySixFive(t *testing.T) {
	assert.Equal(t, domain.SeveritySiaga, mapping.SeismicSeverity(6.5, nil))
}

func TestSeismicSeverity_WarningWaspada(t *testing.T) {
	w := []clients.TsunamiWarning{{ThreatLevel: "Waspada"}}
	assert.Equal(t, domain.SeverityWaspada, mapping.SeismicSeverity(3.0, w))
}

func TestSeismicSeverity_WarningSiaga(t *testing.T) {
	w := []clients.TsunamiWarning{{ThreatLevel: "Siaga"}}
	assert.Equal(t, domain.SeveritySiaga, mapping.SeismicSeverity(3.0, w))
}

func TestSeismicSeverity_WarningAwas(t *testing.T) {
	w := []clients.TsunamiWarning{{ThreatLevel: "Awas"}}
	assert.Equal(t, domain.SeverityAwas, mapping.SeismicSeverity(3.0, w))
}

func TestSeismicSeverity_AwasOverridesMagnitude(t *testing.T) {
	// Magnitude alone would derive SIAGA (>=6.5), but an Awas warning
	// must win outright per Rule 1.
	w := []clients.TsunamiWarning{{ThreatLevel: "Awas"}}
	assert.Equal(t, domain.SeverityAwas, mapping.SeismicSeverity(7.5, w))
}

func TestSeismicSeverity_HighestOfMultipleWarnings(t *testing.T) {
	w := []clients.TsunamiWarning{{ThreatLevel: "Waspada"}, {ThreatLevel: "Siaga"}}
	assert.Equal(t, domain.SeveritySiaga, mapping.SeismicSeverity(3.0, w))
}

func TestSeismicToHazard_Mapping(t *testing.T) {
	occurred := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	ev := clients.SeismicEvent{
		EventID: "evt-1", Magnitude: 6.9, DepthKM: 12.5,
		EpicenterLat: -6.5, EpicenterLon: 106.5, RegionName: "Kab. Cianjur",
		OccurredAt: occurred, PotentialTsunami: true,
	}
	warnings := []clients.TsunamiWarning{{
		WarningID: "warn-1", RelatedEventID: "evt-1", ThreatLevel: "Siaga",
		AffectedZones: []string{"Zone A"}, EstimatedArrival: occurred.Add(30 * time.Minute),
	}}

	hazard := mapping.SeismicToHazard(ev, warnings)

	assert.Equal(t, domain.SourceBMKG, hazard.Source)
	assert.Equal(t, domain.HazardSeismic, hazard.HazardType)
	assert.Equal(t, "evt-1", hazard.SourceRefID)
	assert.Equal(t, "Kab. Cianjur", hazard.AreaName)
	assert.Equal(t, -6.5, hazard.Latitude)
	assert.Equal(t, 106.5, hazard.Longitude)
	assert.Equal(t, domain.SeveritySiaga, hazard.Severity)
	assert.Equal(t, 6.9, hazard.Attributes["magnitude"])
	assert.Equal(t, 12.5, hazard.Attributes["depth_km"])
	assert.Equal(t, true, hazard.Attributes["potential_tsunami"])
	assert.Equal(t, "warn-1", hazard.Attributes["warning_id"])
	assert.Equal(t, "Siaga", hazard.Attributes["threat_level"])

	_, err := uuid.Parse(hazard.HazardID)
	require.NoError(t, err, "hazard_id must be a valid UUID")
	assert.Equal(t, 4, int(uuid.MustParse(hazard.HazardID).Version()))
}

func TestSeismicToHazard_NoWarning_MagnitudeDerived(t *testing.T) {
	ev := clients.SeismicEvent{EventID: "evt-2", Magnitude: 3.5, OccurredAt: time.Now()}
	hazard := mapping.SeismicToHazard(ev, nil)
	assert.Equal(t, domain.SeverityNormal, hazard.Severity)
	_, hasWarningID := hazard.Attributes["warning_id"]
	assert.False(t, hasWarningID, "warning fields must be omitted when no warning exists")
}
