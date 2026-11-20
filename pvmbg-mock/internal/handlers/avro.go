package handlers

import (
	"context"
	"strings"

	"github.com/linkedin/goavro/v2"

	"pvmbg-mock/internal/avrowire"
	"pvmbg-mock/internal/domain"
)

const volcanicSubject = "pvmbg.volcanic-reports-value"
const volcanicV1 = `{"type":"array","items":{"type":"record","name":"VolcanicReport","namespace":"id.go.pvmbg","fields":[{"name":"report_id","type":"string"},{"name":"volcano_id","type":"string"},{"name":"alert_level","type":"string"},{"name":"eruption_count_24h","type":"int"},{"name":"ash_column_height_m","type":"double"},{"name":"reported_at","type":"string"}]}}`

func volcanicV2() string {
	return strings.TrimSuffix(volcanicV1, `]}}`) + `,{"name":"confidence_level","type":["null","double"],"default":null,"x-validation":{"minimum":0,"maximum":1}}]}}`
}

func (h *Handler) encodeVolcanic(ctx context.Context, reports []domain.VolcanicReport, showConfidence bool) ([]byte, error) {
	schemaText := volcanicV1
	if showConfidence {
		schemaText = volcanicV2()
	}
	schema, err := h.Registry.Register(ctx, volcanicSubject, schemaText)
	if err != nil {
		return nil, err
	}
	native := make([]any, 0, len(reports))
	for _, r := range reports {
		item := map[string]any{"report_id": r.ReportID, "volcano_id": r.VolcanoID, "alert_level": string(r.AlertLevel), "eruption_count_24h": int32(r.EruptionCount24h), "ash_column_height_m": r.AshColumnHeightM, "reported_at": r.ReportedAt.Time.UTC().Format("2006-01-02T15:04:05.000Z")}
		if showConfidence {
			item["confidence_level"] = goavro.Union("null", nil)
			if r.ConfidenceLevel != nil {
				item["confidence_level"] = goavro.Union("double", *r.ConfidenceLevel)
			}
		}
		native = append(native, item)
	}
	return avrowire.Encode(schema, native)
}

func volcanicHistorySchema(confidence bool) (string, error) {
	text := volcanicV1
	if confidence {
		text = volcanicV2()
	}
	return avrowire.HistorySchema(text, "VolcanicHistoryPage")
}

const volcanicHistorySubject = "pvmbg.volcanic-reports-history-value"
