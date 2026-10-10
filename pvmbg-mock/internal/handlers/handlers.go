package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"pvmbg-mock/internal/avrowire"
	"pvmbg-mock/internal/domain"
	"pvmbg-mock/internal/history"
	"pvmbg-mock/internal/middleware"
	"pvmbg-mock/internal/seed"
	"pvmbg-mock/internal/state"
	"pvmbg-mock/internal/timeutil"
)

type Handler struct {
	Logger      *slog.Logger
	Archive     *history.Archive
	Reports     []domain.VolcanicReport
	Toggles     *state.Toggles
	MinDelayMs  int
	MaxDelayMs  int
	rngMu       sync.Mutex // guards rng: requests run concurrently and *rand.Rand is not goroutine-safe
	rng         *rand.Rand
	Registry    *avrowire.Client
	schemaMu    sync.RWMutex // guards Reports, activations, liveIDs
	activations int
	liveIDs     []string
}

func New(reports []domain.VolcanicReport, toggles *state.Toggles, minDelayMs, maxDelayMs int) *Handler {
	reports = slices.Clone(reports)
	for i := range reports {
		if reports[i].ConfidenceLevel != nil {
			confidence := *reports[i].ConfidenceLevel
			reports[i].ConfidenceLevel = &confidence
		}
	}
	return &Handler{
		Logger: middleware.NewLogger(), Reports: reports,
		Toggles:    toggles,
		MinDelayMs: minDelayMs,
		MaxDelayMs: maxDelayMs,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// maxLive bounds the live window. Durable history retains evicted records.
// Seed and schema-toggle reports are never evicted.
const maxLive = 100

// AddReport publishes an immutable report. Reusing an existing ID is a no-op.
func (h *Handler) AddReport(r domain.VolcanicReport) {
	h.schemaMu.Lock()
	defer h.schemaMu.Unlock()
	if r.ReportID == "" || slices.ContainsFunc(h.Reports, func(old domain.VolcanicReport) bool { return old.ReportID == r.ReportID }) {
		return
	}
	if r.ConfidenceLevel != nil {
		confidence := *r.ConfidenceLevel
		r.ConfidenceLevel = &confidence
	}
	if h.Archive != nil {
		if err := h.Archive.AppendImmutable(history.Record{Entity: "volcanic-reports", ID: r.ReportID, Payload: r}); err != nil {
			h.Logger.Error("archive write failed; observation not exposed", "service", "pvmbg-mock", "error", err)
			return
		}
	}
	h.Reports = append(h.Reports, r)
	h.liveIDs = append(h.liveIDs, r.ReportID)
	if len(h.liveIDs) <= maxLive {
		return
	}
	oldest := h.liveIDs[0]
	h.liveIDs = h.liveIDs[1:]
	h.Reports = slices.DeleteFunc(h.Reports, func(r domain.VolcanicReport) bool { return r.ReportID == oldest })
}

// RunGenerator adds one new volcanic report per interval until ctx is
// cancelled. It keeps running during an outage, which only blocks the data
// endpoint, so the Aggregator catches up once the outage ends.
func (h *Handler) RunGenerator(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	boot := time.Now().UnixNano()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r := seed.NextReport(boot, n, now.UTC(), rng)
			h.AddReport(r)
			logger.Info("generated volcanic report", "service", "pvmbg-mock",
				"report_id", r.ReportID, "volcano_id", r.VolcanoID)
		}
	}
}

func (h *Handler) randomDelay() {
	lo, hi := h.MinDelayMs, h.MaxDelayMs
	if hi <= lo {
		time.Sleep(time.Duration(lo) * time.Millisecond)
		return
	}
	h.rngMu.Lock()
	d := lo + h.rng.Intn(hi-lo+1)
	h.rngMu.Unlock()
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

	h.schemaMu.RLock()
	defer h.schemaMu.RUnlock()
	if h.Archive != nil {
		head, err := h.Archive.Head()
		if err != nil {
			c.JSON(503, gin.H{"error": "archive unavailable"})
			return
		}
		c.Header("X-Source-Sequence", strconv.FormatInt(head, 10))
	}
	since := parseSince(c)
	showConfidence := h.Toggles.ShowConfidence()
	out := make([]domain.VolcanicReport, 0, len(h.Reports))
	for _, r := range h.Reports {
		if since.IsZero() || !r.ReportedAt.Time.Before(since) {
			out = append(out, r.WireCopy(showConfidence))
		}
	}
	if c.GetHeader("Accept") == avrowire.MediaType {
		if h.Registry == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "schema registry unavailable"})
			return
		}
		data, err := h.encodeVolcanic(avrowire.WithCorrelationID(c.Request.Context(), middleware.GetCorrelationID(c)), out, showConfidence)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "schema registry unavailable"})
			return
		}
		c.Data(http.StatusOK, avrowire.MediaType, data)
		return
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

func (h *Handler) AdminSchemaVersion(c *gin.Context) {
	h.schemaMu.Lock()
	defer h.schemaMu.Unlock()
	if h.Registry != nil {
		// Register v1 first so the registry checks v2 against it. A failed
		// registration leaves the source's visible schema unchanged.
		registryCtx := avrowire.WithCorrelationID(c.Request.Context(), middleware.GetCorrelationID(c))
		if _, err := h.Registry.Register(registryCtx, volcanicSubject, volcanicV1); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "schema registry unavailable"})
			return
		}
		if _, err := h.Registry.Register(registryCtx, volcanicSubject, volcanicV2()); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "schema registration failed"})
			return
		}
		for _, visible := range []bool{false, true} {
			text, err := volcanicHistorySchema(visible)
			if err == nil {
				_, err = h.Registry.Register(registryCtx, volcanicHistorySubject, text)
			}
			if err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "history schema registration failed"})
				return
			}
		}
	}
	visible := !h.Toggles.ShowConfidence()
	version := "v1"
	if visible {
		version = "v2"
	}
	// A schema toggle changes serialization, not the source records. A new
	// report makes v2 observable immediately without rewriting historic IDs.
	var added *domain.VolcanicReport
	if visible && len(h.Reports) > 0 {
		r := h.Reports[0]
		r.ReportID = fmt.Sprintf("rep-schema-%d-%05d", time.Now().UnixNano(), h.activations+1)
		r.ReportedAt = timeutil.New(time.Now().UTC().Truncate(time.Millisecond))
		confidence := 0.88
		r.ConfidenceLevel = &confidence
		added = &r
	}
	if added != nil && h.Archive != nil {
		if err := h.Archive.AppendImmutable(history.Record{Entity: "volcanic-reports", ID: added.ReportID, Payload: *added}); err != nil {
			c.JSON(503, gin.H{"error": "archive unavailable"})
			return
		}
	}
	if added != nil {
		h.Reports = append(h.Reports, *added)
		h.activations++
	}
	h.Toggles.ToggleSchema()
	c.JSON(http.StatusOK, schemaVersionResponse{SchemaVersion: version, ConfidenceLevelVisible: visible})
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
