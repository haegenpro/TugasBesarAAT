package handlers

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"pvmbg-mock/internal/domain"
	"pvmbg-mock/internal/state"
	"pvmbg-mock/internal/timeutil"
)

type Handler struct {
	Reports    []domain.VolcanicReport
	Toggles    *state.Toggles
	MinDelayMs int
	MaxDelayMs int
	rng        *rand.Rand
}

func New(reports []domain.VolcanicReport, toggles *state.Toggles, minDelayMs, maxDelayMs int) *Handler {
	return &Handler{
		Reports:    reports,
		Toggles:    toggles,
		MinDelayMs: minDelayMs,
		MaxDelayMs: maxDelayMs,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (h *Handler) randomDelay() {
	lo, hi := h.MinDelayMs, h.MaxDelayMs
	if hi <= lo {
		time.Sleep(time.Duration(lo) * time.Millisecond)
		return
	}
	d := lo + h.rng.Intn(hi-lo+1)
	time.Sleep(time.Duration(d) * time.Millisecond)
}

func parseSince(c *gin.Context) time.Time {
	raw := c.Query("since")
	if raw == "" {
		return time.Time{}
	}
	t, err := timeutil.ParseISO(raw)
	if err != nil {
		return time.Time{}
	}
	return t
}

func (h *Handler) VolcanicReports(c *gin.Context) {
	if h.Toggles.Outage() {
		// Fail fast and deterministically instead of hanging or sleeping
		// through the full random delay window.
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "pvmbg outage in progress"})
		return
	}

	h.randomDelay()

	// Re-check: outage may have been enabled while we were delaying.
	if h.Toggles.Outage() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "pvmbg outage in progress"})
		return
	}

	since := parseSince(c)
	showConfidence := h.Toggles.ShowConfidence()
	out := make([]domain.VolcanicReport, 0, len(h.Reports))
	for _, r := range h.Reports {
		if since.IsZero() || r.ReportedAt.Time.After(since) {
			out = append(out, r.WireCopy(showConfidence))
		}
	}
	c.JSON(http.StatusOK, out)
}

func Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "pvmbg-mock"})
}

type schemaVersionResponse struct {
	SchemaVersion          string `json:"schema_version"`
	ConfidenceLevelVisible bool   `json:"confidence_level_visible"`
}

func AdminSchemaVersion(toggles *state.Toggles) gin.HandlerFunc {
	return func(c *gin.Context) {
		visible := toggles.ToggleSchema()
		version := "v1"
		if visible {
			version = "v2"
		}
		c.JSON(http.StatusOK, schemaVersionResponse{SchemaVersion: version, ConfidenceLevelVisible: visible})
	}
}

type outageRequest struct {
	Enabled *bool `json:"enabled"`
}

func AdminOutage(toggles *state.Toggles) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body outageRequest
		_ = c.ShouldBindJSON(&body)

		var outage bool
		if body.Enabled != nil {
			outage = toggles.SetOutage(*body.Enabled)
		} else {
			outage = toggles.ToggleOutage()
		}
		c.JSON(http.StatusOK, gin.H{"outage": outage})
	}
}
