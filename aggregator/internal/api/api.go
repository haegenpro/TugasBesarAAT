// Package api implements the Aggregator's internal read API, called by the
// future Client-API — never by end clients directly.
package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"aggregator/internal/domain"
	"aggregator/internal/poller"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
)

type API struct {
	Store  *store.Store
	Status *poller.PVMBGStatus
}

func New(s *store.Store, status *poller.PVMBGStatus) *API {
	return &API{Store: s, Status: status}
}

type listResponse struct {
	Data  []domain.HazardEvent `json:"data"`
	Count int                  `json:"count"`
	AsOf  timeutil.Time        `json:"as_of"`
}

// enrichStale adds attributes.stale / attributes.stale_since to volcanic
// events at response time when PVMBG is currently known unreachable. The
// stored row itself is never mutated for this.
func (a *API) enrichStale(events []domain.HazardEvent) []domain.HazardEvent {
	if a.Status.IsHealthy() {
		return events
	}
	staleSince := timeutil.FormatISOMilli(a.Status.StaleSince())
	for i, e := range events {
		if e.HazardType != domain.HazardVolcanic {
			continue
		}
		attrs := map[string]interface{}{}
		for k, v := range e.Attributes {
			attrs[k] = v
		}
		attrs["stale"] = true
		attrs["stale_since"] = staleSince
		events[i].Attributes = attrs
	}
	return events
}

func (a *API) ListHazards(c *gin.Context) {
	f := store.Filters{
		HazardType: c.Query("hazard_type"),
		Severity:   c.Query("severity"),
		AreaName:   c.Query("area_name"),
	}
	if raw := c.Query("since"); raw != "" {
		if t, err := timeutil.ParseISO(raw); err == nil {
			f.Since = t
		}
	}

	events, err := a.Store.Query(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	events = a.enrichStale(events)

	c.JSON(http.StatusOK, listResponse{
		Data:  events,
		Count: len(events),
		AsOf:  timeutil.New(domain.Now()),
	})
}

func (a *API) GetHazard(c *gin.Context) {
	id := c.Param("id")
	event, err := a.Store.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "hazard not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	enriched := a.enrichStale([]domain.HazardEvent{event})
	c.JSON(http.StatusOK, enriched[0])
}

func Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "aggregator"})
}
