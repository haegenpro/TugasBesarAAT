// Package api implements the Aggregator's internal read API, called by the
// future Client-API — never by end clients directly.
package api

import (
	"errors"
	"github.com/google/uuid"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"aggregator/internal/domain"
	"aggregator/internal/poller"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
)

type API struct {
	Store          *store.Store
	Status         *poller.PVMBGStatus
	Deps           *Dependencies
	BMKGFreshness  time.Duration
	PVMBGFreshness time.Duration
}

func New(s *store.Store, status *poller.PVMBGStatus, deps *Dependencies, bmkgFreshness, pvmbgFreshness time.Duration) *API {
	return &API{Store: s, Status: status, Deps: deps, BMKGFreshness: bmkgFreshness, PVMBGFreshness: pvmbgFreshness}
}

type listResponse struct {
	Data         []domain.HazardEvent             `json:"data"`
	Count        int                              `json:"count"`
	AsOf         timeutil.Time                    `json:"as_of"`
	SourceStatus map[string]sourceStatus          `json:"source_status"`
	Metadata     map[string]domain.RecordMetadata `json:"metadata"`
}

type sourceStatus struct {
	State       string     `json:"state"`
	LastSuccess *time.Time `json:"last_success,omitempty"`
	LastAttempt *time.Time `json:"last_attempt,omitempty"`
	StaleSince  string     `json:"stale_since,omitempty"`
}

func (a *API) ListHazards(c *gin.Context) {
	if !a.Deps.StoreReady() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "store unavailable"})
		return
	}
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
		a.Deps.SetStoreReady(false)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "store unavailable"})
		return
	}
	status, err := a.statuses(c.Request.Context())
	if err != nil {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}
	metadata, err := a.recordMetadata(c.Request.Context(), events, status)
	if err != nil {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}

	c.JSON(http.StatusOK, listResponse{
		Data:         events,
		Count:        len(events),
		AsOf:         timeutil.New(domain.Now()),
		SourceStatus: status,
		Metadata:     metadata,
	})
}

func (a *API) GetHazard(c *gin.Context) {
	if !a.Deps.StoreReady() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "store unavailable"})
		return
	}
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(400, gin.H{"error": "invalid hazard ID"})
		return
	}
	event, err := a.Store.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "hazard not found"})
			return
		}
		a.Deps.SetStoreReady(false)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "store unavailable"})
		return
	}
	status, err := a.statuses(c.Request.Context())
	if err != nil {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}
	metadata, err := a.recordMetadata(c.Request.Context(), []domain.HazardEvent{event}, status)
	if err != nil {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": event, "source_status": status, "metadata": metadata[event.HazardID], "as_of": timeutil.New(domain.Now())})
}

func Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "aggregator"})
}

func (a *API) Ready(c *gin.Context) {
	storeReady := a.Deps.StoreReady()
	brokerReady := a.Deps.BrokerReady() && a.Deps.LifecycleReady()
	status := "ready"
	code := http.StatusOK
	if !storeReady {
		status = "unavailable"
		code = http.StatusServiceUnavailable
	} else if !brokerReady {
		status = "degraded"
	}
	c.JSON(code, gin.H{"status": status, "store": storeReady, "broker": brokerReady})
}
