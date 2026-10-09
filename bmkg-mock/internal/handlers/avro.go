package handlers

import (
	"context"

	"bmkg-mock/internal/avrowire"
	"bmkg-mock/internal/domain"

	"github.com/linkedin/goavro/v2"
)

const seismicSchema = `{"type":"array","items":{"type":"record","name":"SeismicEvent","namespace":"id.go.bmkg","fields":[{"name":"event_id","type":"string"},{"name":"magnitude","type":"double"},{"name":"depth_km","type":"double"},{"name":"epicenter_lat","type":"double"},{"name":"epicenter_lon","type":"double"},{"name":"region_name","type":"string"},{"name":"occurred_at","type":"string"},{"name":"potential_tsunami","type":"boolean"},{"name":"updated_at","type":["null","string"],"default":null}]}}`
const warningSchema = `{"type":"array","items":{"type":"record","name":"TsunamiWarning","namespace":"id.go.bmkg","fields":[{"name":"warning_id","type":"string"},{"name":"related_event_id","type":"string"},{"name":"threat_level","type":"string"},{"name":"affected_zones","type":{"type":"array","items":"string"}},{"name":"estimated_arrival","type":"string"},{"name":"updated_at","type":["null","string"],"default":null}]}}`

func (h *Handler) encodeSeismic(ctx context.Context, events []domain.SeismicEvent) ([]byte, error) {
	schema, err := h.Registry.Register(ctx, "bmkg.seismic-events-value", seismicSchema)
	if err != nil {
		return nil, err
	}
	native := make([]any, 0, len(events))
	for _, e := range events {
		native = append(native, map[string]any{"event_id": e.EventID, "magnitude": e.Magnitude, "depth_km": e.DepthKM, "epicenter_lat": e.EpicenterLat, "epicenter_lon": e.EpicenterLon, "region_name": e.RegionName, "occurred_at": e.OccurredAt.Time.UTC().Format("2006-01-02T15:04:05.000Z"), "potential_tsunami": e.PotentialTsunami, "updated_at": goavro.Union("string", e.UpdatedAt.Time.UTC().Format("2006-01-02T15:04:05.000Z"))})
	}
	return avrowire.Encode(schema, native)
}

func (h *Handler) encodeWarnings(ctx context.Context, warnings []domain.TsunamiWarning) ([]byte, error) {
	schema, err := h.Registry.Register(ctx, "bmkg.tsunami-warnings-value", warningSchema)
	if err != nil {
		return nil, err
	}
	native := make([]any, 0, len(warnings))
	for _, w := range warnings {
		zones := make([]any, 0, len(w.AffectedZones))
		for _, zone := range w.AffectedZones {
			zones = append(zones, zone)
		}
		native = append(native, map[string]any{"warning_id": w.WarningID, "related_event_id": w.RelatedEventID, "threat_level": string(w.ThreatLevel), "affected_zones": zones, "estimated_arrival": w.EstimatedArrival.Time.UTC().Format("2006-01-02T15:04:05.000Z"), "updated_at": goavro.Union("string", w.UpdatedAt.Time.UTC().Format("2006-01-02T15:04:05.000Z"))})
	}
	return avrowire.Encode(schema, native)
}
