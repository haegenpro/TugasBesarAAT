package handlers

import (
	"context"
	"strconv"
	"strings"

	"github.com/linkedin/goavro/v2"

	"pvmbg-mock/internal/avrowire"
	"pvmbg-mock/internal/domain"
	"pvmbg-mock/internal/state"
)

const volcanicSubject = "pvmbg.volcanic-reports-value"
const volcanicV1 = `{"type":"array","items":{"type":"record","name":"VolcanicReport","namespace":"id.go.pvmbg","fields":[{"name":"report_id","type":"string"},{"name":"volcano_id","type":"string"},{"name":"alert_level","type":"string"},{"name":"eruption_count_24h","type":"int"},{"name":"ash_column_height_m","type":"double"},{"name":"reported_at","type":"string"}]}}`

const incompatibleSubject = "pvmbg.volcanic-reports-incompatible-value"
const incompatibleHistorySubject = "pvmbg.volcanic-reports-incompatible-history-value"

func volcanicV2() string {
	return strings.TrimSuffix(volcanicV1, `]}}`) + `,{"name":"confidence_level","type":["null","double"],"default":null,"x-validation":{"minimum":0,"maximum":1}}]}}`
}

func liveSchema(mode state.Mode) string {
	switch mode {
	case state.ModeV2:
		return volcanicV2()
	case state.ModeRename:
		return strings.Replace(volcanicV1, `{"name":"alert_level","type":"string"}`, `{"name":"level","type":"string"}`, 1)
	case state.ModeTypeChange:
		return strings.Replace(volcanicV1, `{"name":"eruption_count_24h","type":"int"}`, `{"name":"eruption_count_24h","type":"string"}`, 1)
	case state.ModeMissingRequired:
		return strings.Replace(volcanicV1, `{"name":"ash_column_height_m","type":"double"},`, ``, 1)
	}
	return volcanicV1
}

func liveSubject(mode state.Mode) string {
	if mode.Incompatible() {
		return incompatibleSubject
	}
	return volcanicSubject
}

func historySubject(mode state.Mode) string {
	if mode.Incompatible() {
		return incompatibleHistorySubject
	}
	return volcanicHistorySubject
}

func compatibility(mode state.Mode) string {
	if mode.Incompatible() {
		return "NONE"
	}
	return "FULL_TRANSITIVE"
}

func wireRecord(r domain.VolcanicReport, mode state.Mode) map[string]any {
	item := map[string]any{"report_id": r.ReportID, "volcano_id": r.VolcanoID, "alert_level": string(r.AlertLevel), "eruption_count_24h": r.EruptionCount24h, "ash_column_height_m": r.AshColumnHeightM, "reported_at": r.ReportedAt.Time.UTC().Format("2006-01-02T15:04:05.000Z")}
	switch mode {
	case state.ModeRename:
		item["level"] = item["alert_level"]
		delete(item, "alert_level")
	case state.ModeTypeChange:
		item["eruption_count_24h"] = strconv.Itoa(r.EruptionCount24h)
	case state.ModeMissingRequired:
		delete(item, "ash_column_height_m")
	}
	return item
}

func (h *Handler) encodeVolcanic(ctx context.Context, reports []domain.VolcanicReport, mode state.Mode) ([]byte, error) {
	schema, err := h.Registry.RegisterWith(ctx, liveSubject(mode), liveSchema(mode), compatibility(mode))
	if err != nil {
		return nil, err
	}
	native := make([]any, 0, len(reports))
	for _, r := range reports {
		item := wireRecord(r, mode)
		if count, ok := item["eruption_count_24h"].(int); ok {
			item["eruption_count_24h"] = int32(count)
		}
		if mode == state.ModeV2 {
			item["confidence_level"] = goavro.Union("null", nil)
			if r.ConfidenceLevel != nil {
				item["confidence_level"] = goavro.Union("double", *r.ConfidenceLevel)
			}
		}
		native = append(native, item)
	}
	return avrowire.Encode(schema, native)
}

func volcanicHistorySchema(mode state.Mode) (string, error) {
	return avrowire.HistorySchema(liveSchema(mode), "VolcanicHistoryPage")
}

const volcanicHistorySubject = "pvmbg.volcanic-reports-history-value"
