package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"bmkg-mock/internal/domain"
	"bmkg-mock/internal/timeutil"
)

type Handler struct {
	Events   []domain.SeismicEvent
	Warnings []domain.TsunamiWarning
	DelayMs  int
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
	c.JSON(http.StatusOK, out)
}

func Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "bmkg-mock"})
}
