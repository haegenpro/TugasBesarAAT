package handlers

import (
	"context"
	"log/slog"
	"math/rand"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"bmkg-mock/internal/avrowire"
	"bmkg-mock/internal/domain"
	"bmkg-mock/internal/history"
	"bmkg-mock/internal/middleware"
	"bmkg-mock/internal/seed"
	"bmkg-mock/internal/timeutil"
)

// maxLive bounds the live window. Durable history retains evicted records.
const maxLive = 100

type Handler struct {
	Logger   *slog.Logger
	Archive  *history.Archive
	Events   []domain.SeismicEvent
	Warnings []domain.TsunamiWarning
	DelayMs  int
	Registry *avrowire.Client

	mu      sync.RWMutex // guards Events, Warnings, liveIDs
	liveIDs []string
}

func New(events []domain.SeismicEvent, warnings []domain.TsunamiWarning, delayMs int) *Handler {
	events = slices.Clone(events)
	warnings = slices.Clone(warnings)
	for i := range warnings {
		warnings[i].AffectedZones = slices.Clone(warnings[i].AffectedZones)
	}
	return &Handler{Logger: middleware.NewLogger(), Events: events, Warnings: warnings, DelayMs: delayMs}
}

// AddEvent publishes an immutable event and its warning atomically. Reusing an
// event ID never changes a published record or attaches a late warning.
func (h *Handler) AddEvent(ev domain.SeismicEvent, warning *domain.TsunamiWarning) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if slices.ContainsFunc(h.Events, func(e domain.SeismicEvent) bool { return e.EventID == ev.EventID }) {
		return
	}
	if ev.EventID == "" || (warning != nil && (!ev.PotentialTsunami || warning.RelatedEventID != ev.EventID || warning.WarningID == "")) {
		return
	}
	if warning != nil && slices.ContainsFunc(h.Warnings, func(w domain.TsunamiWarning) bool { return w.WarningID == warning.WarningID }) {
		return
	}
	records := []history.Record{{Entity: "seismic-events", ID: ev.EventID, Payload: ev}}
	var w domain.TsunamiWarning
	if warning != nil {
		w = *warning
		w.AffectedZones = slices.Clone(w.AffectedZones)
		records = append(records, history.Record{Entity: "tsunami-warnings", ID: w.WarningID, Payload: w})
	}
	if h.Archive != nil {
		if err := h.Archive.AppendImmutable(records...); err != nil {
			h.Logger.Error("archive write failed; observation not exposed", "service", "bmkg-mock", "error", err)
			return
		}
	}
	h.Events = append(h.Events, ev)
	if warning != nil {
		h.Warnings = append(h.Warnings, w)
	}
	h.liveIDs = append(h.liveIDs, ev.EventID)
	if len(h.liveIDs) <= maxLive {
		return
	}
	oldest := h.liveIDs[0]
	h.liveIDs = h.liveIDs[1:]
	h.Events = slices.DeleteFunc(h.Events, func(e domain.SeismicEvent) bool { return e.EventID == oldest })
	h.Warnings = slices.DeleteFunc(h.Warnings, func(w domain.TsunamiWarning) bool { return w.RelatedEventID == oldest })
}

// RunGenerator adds one new seismic event per interval until ctx is cancelled.
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
			ev, warning := seed.NextEvent(boot, n, now.UTC(), rng)
			h.AddEvent(ev, warning)
			logger.Info("generated seismic event", "service", "bmkg-mock",
				"event_id", ev.EventID, "potential_tsunami", ev.PotentialTsunami)
		}
	}
}

func (h *Handler) delay() {
	if h.DelayMs > 0 {
		time.Sleep(time.Duration(h.DelayMs) * time.Millisecond)
	}
}

// parseSince parses the optional ?since= query param. Missing/invalid since
// returns the zero time, meaning "return the current available dataset".
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

func (h *Handler) SeismicEvents(c *gin.Context) {
	h.delay()
	since := parseSince(c)
	h.mu.RLock()
	if h.Archive != nil {
		head, err := h.Archive.Head()
		if err != nil {
			h.mu.RUnlock()
			c.JSON(503, gin.H{"error": "archive unavailable"})
			return
		}
		c.Header("X-Source-Sequence", strconv.FormatInt(head, 10))
	}
	out := make([]domain.SeismicEvent, 0, len(h.Events))
	for _, e := range h.Events {
		if since.IsZero() || !e.OccurredAt.Time.Before(since) {
			out = append(out, e)
		}
	}
	h.mu.RUnlock()
	if c.GetHeader("Accept") == avrowire.MediaType {
		if h.Registry == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "schema registry unavailable"})
			return
		}
		data, err := h.encodeSeismic(avrowire.WithCorrelationID(c.Request.Context(), middleware.GetCorrelationID(c)), out)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "schema registry unavailable"})
			return
		}
		c.Data(http.StatusOK, avrowire.MediaType, data)
		return
	}
	c.JSON(http.StatusOK, out)
}

func (h *Handler) TsunamiWarnings(c *gin.Context) {
	h.delay()
	since := parseSince(c)
	h.mu.RLock()
	if h.Archive != nil {
		head, err := h.Archive.Head()
		if err != nil {
			h.mu.RUnlock()
			c.JSON(503, gin.H{"error": "archive unavailable"})
			return
		}
		c.Header("X-Source-Sequence", strconv.FormatInt(head, 10))
	}
	eventTimes := make(map[string]time.Time, len(h.Events))
	for _, e := range h.Events {
		eventTimes[e.EventID] = e.OccurredAt.Time
	}
	out := make([]domain.TsunamiWarning, 0, len(h.Warnings))
	for _, w := range h.Warnings {
		if occurred, exists := eventTimes[w.RelatedEventID]; exists && (since.IsZero() || !occurred.Before(since)) {
			out = append(out, w)
		}
	}
	h.mu.RUnlock()
	if c.GetHeader("Accept") == avrowire.MediaType {
		if h.Registry == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "schema registry unavailable"})
			return
		}
		data, err := h.encodeWarnings(avrowire.WithCorrelationID(c.Request.Context(), middleware.GetCorrelationID(c)), out)
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
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "bmkg-mock"})
}
