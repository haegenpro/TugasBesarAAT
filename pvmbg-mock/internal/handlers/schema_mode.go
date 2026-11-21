package handlers

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"pvmbg-mock/internal/avrowire"
	"pvmbg-mock/internal/domain"
	"pvmbg-mock/internal/history"
	"pvmbg-mock/internal/platform"
	"pvmbg-mock/internal/timeutil"
)

const volcanicBreaking = `{"type":"array","items":{"type":"record","name":"VolcanicReport","namespace":"id.go.pvmbg","fields":[{"name":"report_id","type":"string"},{"name":"volcano_id","type":"string"},{"name":"level","type":"string"},{"name":"eruption_count_24h","type":"string"},{"name":"reported_at","type":"string"}]}}`

// A separate major-contract subject makes the intentionally incompatible writer
// available without weakening FULL_TRANSITIVE on the original M1 subjects.
const volcanicBreakingSubject = "pvmbg.volcanic-reports-breaking-value"
const volcanicBreakingHistorySubject = "pvmbg.volcanic-reports-breaking-history-value"

func (h *Handler) wireReport(r domain.VolcanicReport, confidence bool) any {
	if !h.breakingSchema {
		return r.WireCopy(confidence)
	}
	return map[string]any{"report_id": r.ReportID, "volcano_id": r.VolcanoID, "level": string(r.AlertLevel), "eruption_count_24h": strconv.Itoa(r.EruptionCount24h), "reported_at": r.ReportedAt.Time.UTC().Format("2006-01-02T15:04:05.000Z")}
}

// Called with schemaMu held; failed registration/archive writes leave the
// currently visible source contract unchanged.
func (h *Handler) setSchemaMode(c *gin.Context, mode string) {
	if mode != "v1" && mode != "compatible" && mode != "breaking" {
		c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "mode must be v1, compatible or breaking")})
		return
	}
	text, subject, historySubject := volcanicV1, volcanicSubject, volcanicHistorySubject
	if mode == "compatible" {
		text = volcanicV2()
	}
	if mode == "breaking" {
		text, subject, historySubject = volcanicBreaking, volcanicBreakingSubject, volcanicBreakingHistorySubject
	}
	if h.Registry != nil {
		if _, err := h.Registry.Register(c.Request.Context(), subject, text); err != nil {
			c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "schema registration failed")})
			return
		}
		historyText, err := avrowire.HistorySchema(text, "VolcanicHistoryPage")
		if err == nil {
			_, err = h.Registry.Register(c.Request.Context(), historySubject, historyText)
		}
		if err != nil {
			c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "history schema registration failed")})
			return
		}
	}
	if mode == "breaking" && !h.breakingSchema && len(h.Reports) > 0 {
		r := h.Reports[0]
		r.ReportID = fmt.Sprintf("rep-schema-breaking-%d-%05d", time.Now().UnixNano(), h.activations+1)
		r.ReportedAt = timeutil.New(time.Now().UTC().Truncate(time.Millisecond))
		if h.Archive != nil {
			if err := h.Archive.AppendImmutable(history.Record{Entity: "volcanic-reports", ID: r.ReportID, Payload: r}); err != nil {
				c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "archive unavailable")})
				return
			}
		}
		h.Reports = append(h.Reports, r)
		h.activations++
	}
	h.breakingSchema = mode == "breaking"
	if h.Toggles.ShowConfidence() != (mode == "compatible") {
		h.Toggles.ToggleSchema()
	}
	c.JSON(200, gin.H{"schema_version": mode, "confidence_level_visible": h.Toggles.ShowConfidence(), "breaking": h.breakingSchema})
}
