// Package mapping implements the BMKG/PVMBG -> canonical HazardEvent
// transformation, including severity rules and the schema-evolution
// passthrough policy for unknown PVMBG fields.
package mapping

import (
	"context"

	"github.com/google/uuid"

	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/timeutil"
	"aggregator/internal/volcano"
)

// SeismicSeverity applies the priority-ordered severity rules: an Awas
// tsunami warning wins outright; otherwise the highest warning severity
// applies; otherwise severity is derived from magnitude.
func SeismicSeverity(magnitude float64, warnings []clients.TsunamiWarning) domain.Severity {
	if len(warnings) == 0 {
		switch {
		case magnitude >= 6.5:
			return domain.SeveritySiaga
		case magnitude >= 5.0:
			return domain.SeverityWaspada
		default:
			return domain.SeverityNormal
		}
	}

	best := domain.SeverityWaspada
	for _, w := range warnings {
		switch w.ThreatLevel {
		case "Awas":
			return domain.SeverityAwas
		case "Siaga":
			if best != domain.SeverityAwas {
				best = domain.SeveritySiaga
			}
		case "Waspada":
			// no-op, Waspada is the floor when any warning exists
		}
	}
	return best
}

// highestWarning picks the warning with the highest threat level, to source
// the warning-specific attributes from when multiple warnings exist.
func highestWarning(warnings []clients.TsunamiWarning) (clients.TsunamiWarning, bool) {
	if len(warnings) == 0 {
		return clients.TsunamiWarning{}, false
	}
	rank := map[string]int{"Waspada": 1, "Siaga": 2, "Awas": 3}
	best := warnings[0]
	for _, w := range warnings[1:] {
		if rank[w.ThreatLevel] > rank[best.ThreatLevel] {
			best = w
		}
	}
	return best, true
}

func warningsForEvent(eventID string, all []clients.TsunamiWarning) []clients.TsunamiWarning {
	out := make([]clients.TsunamiWarning, 0, 1)
	for _, w := range all {
		if w.RelatedEventID == eventID {
			out = append(out, w)
		}
	}
	return out
}

// SeismicToHazard maps a BMKG SeismicEvent (plus any related tsunami
// warnings) to a canonical HazardEvent. hazard_id is a fresh UUIDv4.
func SeismicToHazard(event clients.SeismicEvent, allWarnings []clients.TsunamiWarning) domain.HazardEvent {
	related := warningsForEvent(event.EventID, allWarnings)
	severity := SeismicSeverity(event.Magnitude, related)

	attrs := map[string]interface{}{
		"magnitude":          event.Magnitude,
		"depth_km":           event.DepthKM,
		"potential_tsunami":  event.PotentialTsunami,
	}
	if w, ok := highestWarning(related); ok {
		attrs["warning_id"] = w.WarningID
		attrs["related_event_id"] = w.RelatedEventID
		attrs["threat_level"] = w.ThreatLevel
		attrs["affected_zones"] = w.AffectedZones
		attrs["estimated_arrival"] = timeutil.FormatISOMilli(w.EstimatedArrival)
	}

	return domain.HazardEvent{
		HazardID:    uuid.NewString(),
		Source:      domain.SourceBMKG,
		SourceRefID: event.EventID,
		HazardType:  domain.HazardSeismic,
		Severity:    severity,
		AreaName:    event.RegionName,
		Latitude:    event.EpicenterLat,
		Longitude:   event.EpicenterLon,
		OccurredAt:  timeutil.New(event.OccurredAt),
		IngestedAt:  timeutil.New(domain.Now()),
		Attributes:  attrs,
	}
}

var alertSeverity = map[string]domain.Severity{
	"Normal":  domain.SeverityNormal,
	"Waspada": domain.SeverityWaspada,
	"Siaga":   domain.SeveritySiaga,
	"Awas":    domain.SeverityAwas,
}

// VolcanicToHazard maps a PVMBG VolcanicReport to a canonical HazardEvent,
// resolving area_name/lat/lon via the volcano reference lookup. Unknown
// fields (including confidence_level, once PVMBG emits it) pass through into
// attributes under their original name.
func VolcanicToHazard(ctx context.Context, lookup volcano.Lookuper, report clients.VolcanicReport) (domain.HazardEvent, error) {
	ref, found, err := lookup.Get(ctx, report.VolcanoID)
	if err != nil {
		return domain.HazardEvent{}, err
	}
	var areaName string
	var lat, lon float64
	if found {
		areaName = ref.AreaName
		lat, lon = ref.Latitude, ref.Longitude
	} else {
		areaName = report.VolcanoID
	}

	severity, ok := alertSeverity[report.AlertLevel]
	if !ok {
		severity = domain.SeverityNormal
	}

	attrs := map[string]interface{}{
		"eruption_count_24h":  report.EruptionCount24h,
		"ash_column_height_m": report.AshColumnHeightM,
	}
	for k, v := range report.Extra {
		attrs[k] = v
	}

	return domain.HazardEvent{
		HazardID:    uuid.NewString(),
		Source:      domain.SourcePVMBG,
		SourceRefID: report.ReportID,
		HazardType:  domain.HazardVolcanic,
		Severity:    severity,
		AreaName:    areaName,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  timeutil.New(report.ReportedAt),
		IngestedAt:  timeutil.New(domain.Now()),
		Attributes:  attrs,
	}, nil
}
