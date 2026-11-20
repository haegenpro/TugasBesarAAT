// Package api implements the Aggregator's internal read API, called by the
// future Client-API — never by end clients directly.
package api

import (
	"aggregator/internal/platform"
	"context"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"aggregator/internal/domain"
	"aggregator/internal/poller"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
)

// Repository is the read model boundary; a later implementation may fan out across shards.
type Repository interface {
	QueryWithMetadata(context.Context, store.Filters) ([]domain.HazardEvent, map[string]domain.RecordMetadata, error)
	QueryPage(context.Context, store.Filters, store.PageRequest) (store.Page, error)
	GetByIDWithMetadata(context.Context, string) (domain.HazardEvent, domain.RecordMetadata, error)
	PollStatuses(context.Context, []string) (map[string]domain.EndpointStatus, error)
	Notices(context.Context, string) ([]domain.Notice, error)
	Notice(context.Context, string) (domain.Notice, []domain.LifecycleEvent, error)
}
type API struct {
	Store          Repository
	Status         *poller.PVMBGStatus
	Deps           *Dependencies
	BMKGFreshness  time.Duration
	PVMBGFreshness time.Duration
}

func New(s Repository, status *poller.PVMBGStatus, deps *Dependencies, bmkgFreshness, pvmbgFreshness time.Duration) *API {
	return &API{Store: s, Status: status, Deps: deps, BMKGFreshness: bmkgFreshness, PVMBGFreshness: pvmbgFreshness}
}

type listResponse struct {
	NextCursor       *string                          `json:"next_cursor"`
	Partial          bool                             `json:"partial"`
	UnavailableUnits []string                         `json:"unavailable_units"`
	Data             []domain.HazardEvent             `json:"data"`
	Count            int                              `json:"count"`
	AsOf             timeutil.Time                    `json:"as_of"`
	SourceStatus     map[string]sourceStatus          `json:"source_status"`
	Metadata         map[string]domain.RecordMetadata `json:"metadata"`
}

type sourceStatus struct {
	State       string     `json:"state"`
	LastSuccess *time.Time `json:"last_success,omitempty"`
	LastAttempt *time.Time `json:"last_attempt,omitempty"`
	StaleSince  string     `json:"stale_since,omitempty"`
}

func (a *API) ListHazards(c *gin.Context) {
	if !a.Deps.StoreReady() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "store unavailable")})
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
		} else {
			c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "invalid since")})
			return
		}
	}
	if !domain.ValidHazardType(f.HazardType) || !domain.ValidSeverity(f.Severity) {
		c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "invalid hazard_type or severity")})
		return
	}
	limit := 100
	if raw := c.Query("limit"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 1000 {
			c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "limit must be between 1 and 1000")})
			return
		}
		limit = n
	}
	page, err := a.Store.QueryPage(c.Request.Context(), f, store.PageRequest{Limit: limit, Cursor: c.Query("cursor")})
	events, metadata := page.Events, page.Metadata
	if errors.Is(err, store.ErrCursor) {
		c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "invalid cursor")})
		return
	}
	if err != nil {
		a.Deps.SetStoreReady(false)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "store unavailable")})
		return
	}
	status, err := a.statuses(c.Request.Context())
	if err != nil {
		c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "store unavailable")})
		return
	}
	metadata = a.recordMetadata(events, status, metadata)

	c.JSON(http.StatusOK, listResponse{
		Data:       events,
		NextCursor: page.NextCursor, Partial: page.Partial, UnavailableUnits: page.UnavailableUnits,
		Count:        len(events),
		AsOf:         timeutil.New(domain.Now()),
		SourceStatus: status,
		Metadata:     metadata,
	})
}

func (a *API) GetHazard(c *gin.Context) {
	if !a.Deps.StoreReady() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "store unavailable")})
		return
	}
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(400, gin.H{"error": platform.ProblemStatus(400, "invalid hazard ID")})
		return
	}
	event, storedMetadata, err := a.Store.GetByIDWithMetadata(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": platform.ProblemStatus(http.StatusNotFound, "hazard not found")})
			return
		}
		a.Deps.SetStoreReady(false)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": platform.ProblemStatus(http.StatusServiceUnavailable, "store unavailable")})
		return
	}
	status, err := a.statuses(c.Request.Context())
	if err != nil {
		c.JSON(503, gin.H{"error": platform.ProblemStatus(503, "store unavailable")})
		return
	}
	metadata := a.recordMetadata([]domain.HazardEvent{event}, status, map[string]domain.RecordMetadata{event.HazardID: storedMetadata})
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
