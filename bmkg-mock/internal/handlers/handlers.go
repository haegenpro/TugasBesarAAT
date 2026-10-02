package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"bmkg-mock/internal/avrowire"
	"bmkg-mock/internal/domain"
	"bmkg-mock/internal/middleware"
	"bmkg-mock/internal/timeutil"
)

type Handler struct {
	Events   []domain.SeismicEvent
	Warnings []domain.TsunamiWarning
	DelayMs  int
	Registry *avrowire.Client
}

func New(events []domain.SeismicEvent, warnings []domain.TsunamiWarning, delayMs int) *Handler {
	return &Handler{Events: events, Warnings: warnings, DelayMs: delayMs}
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
	out := make([]domain.SeismicEvent, 0, len(h.Events))
	for _, e := range h.Events {
		if since.IsZero() || e.OccurredAt.Time.After(since) {
			out = append(out, e)
		}
	}
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
	out := make([]domain.TsunamiWarning, 0, len(h.Warnings))
	for _, w := range h.Warnings {
		if since.IsZero() || w.EstimatedArrival.Time.After(since) {
			out = append(out, w)
		}
	}
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
