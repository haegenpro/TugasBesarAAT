package handlers

import (
	"context"
	"log/slog"
	"math/rand"
	"net/http"
	"reflect"
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

	mu        sync.RWMutex // guards Events, Warnings, liveIDs
	liveIDs   []string
	updatedAt time.Time
}

func New(events []domain.SeismicEvent, warnings []domain.TsunamiWarning, delayMs int) *Handler {
	now := time.Now().UTC().Truncate(time.Millisecond)
	events = slices.Clone(events)
	warnings = slices.Clone(warnings)
	for i := range events {
		events[i].UpdatedAt = timeutil.New(now)
	}
	for i := range warnings {
		warnings[i].UpdatedAt = timeutil.New(now)
		warnings[i].AffectedZones = slices.Clone(warnings[i].AffectedZones)
	}
	return &Handler{Logger: middleware.NewLogger(), Events: events, Warnings: warnings, DelayMs: delayMs, updatedAt: now}
}

// AddEvent inserts or updates an event and its optional warning. Changed
// records receive a source-owned updated_at only after durable publication.
// New generated events evict the oldest generated one past maxLive.
func (h *Handler) AddEvent(ev domain.SeismicEvent, warning *domain.TsunamiWarning) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ei := slices.IndexFunc(h.Events, func(e domain.SeismicEvent) bool { return e.EventID == ev.EventID })
	changed := ei < 0
	if ei >= 0 {
		ev.UpdatedAt = h.Events[ei].UpdatedAt
		changed = !reflect.DeepEqual(ev, h.Events[ei])
	}
	wi := -1
	var w domain.TsunamiWarning
	warningChanged := false
	if warning != nil {
		w = *warning
		w.AffectedZones = slices.Clone(w.AffectedZones)
		wi = slices.IndexFunc(h.Warnings, func(old domain.TsunamiWarning) bool { return old.WarningID == w.WarningID })
		warningChanged = wi < 0
		if wi >= 0 {
			w.UpdatedAt = h.Warnings[wi].UpdatedAt
			warningChanged = !reflect.DeepEqual(w, h.Warnings[wi])
		}
	}
	if !changed && !warningChanged {
		return
	}
	// Timestamp precision matches JSON/Avro milliseconds. Ensure successive
	// mutations remain visible even within the same millisecond.
	now := time.Now().UTC().Truncate(time.Millisecond)
	if !now.After(h.updatedAt) {
		now = h.updatedAt.Add(time.Millisecond)
	}
	if changed {
		ev.UpdatedAt = timeutil.New(now)
	}
	if warningChanged {
		w.UpdatedAt = timeutil.New(now)
	}
	if h.Archive != nil {
		records := []history.Record{}
		if changed {
			records = append(records, history.Record{Entity: "seismic-events", ID: ev.EventID, Payload: ev})
		}
		if warningChanged {
			records = append(records, history.Record{Entity: "tsunami-warnings", ID: w.WarningID, Payload: w})
		}
		if err := h.Archive.Append(records...); err != nil {
			h.Logger.Error("archive write failed; observation not exposed", "service", "bmkg-mock", "error", err)
			return
		}
	}
	h.updatedAt = now
	if changed {
		if ei >= 0 {
			h.Events[ei] = ev
		} else {
			h.Events = append(h.Events, ev)
		}
	}
	if warningChanged {
		if wi >= 0 {
			h.Warnings[wi] = w
		} else {
			h.Warnings = append(h.Warnings, w)
		}
	}
	if ei >= 0 {
		return
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
		if since.IsZero() || e.UpdatedAt.Time.After(since) {
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
	out := make([]domain.TsunamiWarning, 0, len(h.Warnings))
	for _, w := range h.Warnings {
		if since.IsZero() || w.UpdatedAt.Time.After(since) {
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
