package api

import (
	"aggregator/internal/domain"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http"
	"time"
)

func (a *API) statuses(ctx context.Context) (map[string]sourceStatus, error) {
	out := map[string]sourceStatus{}
	for _, key := range []string{"BMKG", "BMKG_TSUNAMI", "PVMBG"} {
		p, err := a.Store.PollStatus(ctx, key)
		if err != nil {
			return nil, err
		}
		ttl := a.BMKGFreshness
		if key == "PVMBG" {
			ttl = a.PVMBGFreshness
		}
		ss := statusAt(p, domain.Now(), ttl)
		out[key] = ss
	}
	return out, nil
}
func (a *API) recordMetadata(ctx context.Context, events []domain.HazardEvent, status map[string]sourceStatus) (map[string]domain.RecordMetadata, error) {
	out, err := a.Store.Metadata(ctx, events)
	if err != nil {
		return nil, err
	}
	for _, e := range events {
		m := out[e.HazardID]
		ttl := a.BMKGFreshness
		if e.Source == domain.SourcePVMBG {
			ttl = a.PVMBGFreshness
		}
		m.State = status[string(e.Source)].State
		m.AssessmentState = "complete"
		if e.Source == domain.SourceBMKG && e.Attributes["potential_tsunami"] == true {
			m.AssessmentState = status["BMKG_TSUNAMI"].State
			if m.AssessmentState == "fresh" {
				m.AssessmentState = "complete"
			}
		}
		m.Historical = time.Since(e.OccurredAt.Time) > ttl
		if m.LastSeenAt == nil || time.Since(*m.LastSeenAt) > ttl {
			m.State = "unavailable"
		}
		out[e.HazardID] = m
	}
	return out, nil
}
func (a *API) ListNotices(c *gin.Context) {
	if !a.Deps.StoreReady() {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}
	state := c.Query("state")
	switch state {
	case "", "open", "all", "pending", "assessment_unavailable", "resolved":
	default:
		c.JSON(400, gin.H{"error": "invalid notice state"})
		return
	}
	notices, err := a.Store.Notices(c.Request.Context(), state)
	if err != nil {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}
	status, err := a.statuses(c.Request.Context())
	if err != nil {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}
	c.JSON(200, gin.H{"data": notices, "count": len(notices), "as_of": timeutil.New(domain.Now()), "source_status": status})
}
func (a *API) GetNotice(c *gin.Context) {
	if !a.Deps.StoreReady() {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(400, gin.H{"error": "invalid notice ID"})
		return
	}
	notice, history, err := a.Store.Notice(c.Request.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		c.JSON(404, gin.H{"error": "notice not found"})
		return
	}
	if err != nil {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}
	status, err := a.statuses(c.Request.Context())
	if err != nil {
		c.JSON(503, gin.H{"error": "store unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": notice, "history": history, "as_of": timeutil.New(domain.Now()), "source_status": status})
}

func statusAt(p domain.EndpointStatus, now time.Time, ttl time.Duration) sourceStatus {
	state := p.State
	if p.LastSuccess == nil || now.Sub(*p.LastSuccess) > ttl {
		state = "unavailable"
	} else if p.State == "fresh" && p.NextAttempt != nil && now.After(p.NextAttempt.Add(5*time.Second)) {
		state = "stale"
	}
	ss := sourceStatus{State: state, LastSuccess: p.LastSuccess, LastAttempt: p.LastAttempt}
	if p.StaleSince != nil {
		ss.StaleSince = timeutil.FormatISOMilli(*p.StaleSince)
	}
	if state == "stale" && ss.StaleSince == "" && p.LastSuccess != nil {
		ss.StaleSince = timeutil.FormatISOMilli(*p.LastSuccess)
	}

	return ss
}
