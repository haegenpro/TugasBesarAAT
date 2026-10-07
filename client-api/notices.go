package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http"
	"net/url"
)

type noticeSummary struct {
	NoticeID          string `json:"notice_id"`
	Source            string `json:"source"`
	ObservationType   string `json:"observation_type"`
	AreaName          string `json:"area_name"`
	OccurredAt        string `json:"occurred_at"`
	ReportedAt        string `json:"reported_at"`
	LastCheckedAt     string `json:"last_checked_at,omitempty"`
	Revision          int64  `json:"revision"`
	State             string `json:"state"`
	PendingDependency string `json:"pending_dependency"`
	HazardID          string `json:"hazard_id,omitempty"`
}
type notice struct {
	noticeSummary
	SourceRefID string         `json:"source_ref_id"`
	Observation map[string]any `json:"observation,omitempty"`
}

func (n *notice) UnmarshalJSON(b []byte) error {
	if err := requireJSONFields(b, "notice_id", "source", "observation_type", "area_name", "occurred_at", "reported_at", "revision", "state", "source_ref_id"); err != nil {
		return err
	}
	type plain notice
	var v plain
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch v.State {
	case "pending", "assessment_unavailable", "resolved":
	default:
		return fmt.Errorf("invalid notice state")
	}
	if v.NoticeID == "" || v.Revision < 1 {
		return fmt.Errorf("invalid notice identity")
	}
	*n = notice(v)
	return nil
}

type noticeTransition struct {
	EventID     string  `json:"event_id"`
	EventType   string  `json:"event_type"`
	Revision    int64   `json:"revision"`
	PublishedAt string  `json:"published_at"`
	Notice      *notice `json:"notice,omitempty"`
	Hazard      *hazard `json:"hazard,omitempty"`
}

func (a *api) listNotices(c *gin.Context) {
	view, ok := a.view(c)
	if !ok {
		return
	}
	state := c.Query("state")
	switch state {
	case "", "open", "all", "pending", "assessment_unavailable", "resolved":
	default:
		c.JSON(400, gin.H{"error": "invalid notice state"})
		return
	}
	endpoint := joinURL(a.cfg.AggregatorURL, "/internal/notices")
	if state != "" {
		endpoint += "?state=" + url.QueryEscape(state)
	}
	var list struct {
		Data         []notice                `json:"data"`
		Count        int                     `json:"count"`
		AsOf         string                  `json:"as_of"`
		SourceStatus map[string]sourceStatus `json:"source_status"`
	}
	status, err := a.call(c, "aggregator", http.MethodGet, endpoint, nil, false, &list)
	if err != nil || status != 200 || list.Data == nil || list.AsOf == "" || list.Count != len(list.Data) {
		c.JSON(503, gin.H{"error": "aggregator unavailable"})
		return
	}
	var data any = list.Data
	if view == "summary" {
		out := []noticeSummary{}
		for _, n := range list.Data {
			out = append(out, n.noticeSummary)
		}
		data = out
	}
	c.JSON(200, gin.H{"data": data, "count": list.Count, "as_of": list.AsOf, "source_status": list.SourceStatus})
}
func (a *api) getNotice(c *gin.Context) {
	view, ok := a.view(c)
	if !ok {
		return
	}
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(400, gin.H{"error": "invalid notice ID"})
		return
	}
	var item struct {
		Data         notice                  `json:"data"`
		History      []noticeTransition      `json:"history"`
		AsOf         string                  `json:"as_of"`
		SourceStatus map[string]sourceStatus `json:"source_status"`
	}
	status, err := a.call(c, "aggregator", http.MethodGet, joinURL(a.cfg.AggregatorURL, "/internal/notices/"+id), nil, false, &item)
	if status == 404 {
		c.JSON(404, gin.H{"error": "notice not found"})
		return
	}
	if err != nil || status != 200 || item.Data.NoticeID == "" || item.History == nil || item.AsOf == "" {
		c.JSON(503, gin.H{"error": "aggregator unavailable"})
		return
	}
	var data any = item.Data
	history := []any{}
	for _, e := range item.History {
		transition := gin.H{"event_id": e.EventID, "event_type": e.EventType, "revision": e.Revision, "published_at": e.PublishedAt}
		if e.Notice != nil {
			if view == "summary" {
				transition["notice"] = e.Notice.noticeSummary
			} else {
				transition["notice"] = e.Notice
			}
		}
		if e.Hazard != nil {
			if view == "summary" {
				transition["hazard"] = e.Hazard.summary()
			} else {
				transition["hazard"] = e.Hazard
			}
		}
		history = append(history, transition)
	}
	if view == "summary" {
		data = item.Data.noticeSummary
	}
	c.JSON(200, gin.H{"data": data, "history": history, "source_status": item.SourceStatus, "as_of": item.AsOf})
}

type recordMetadata struct {
	Revision        int64  `json:"revision"`
	LastSeenAt      string `json:"last_seen_at,omitempty"`
	ObservationTime string `json:"observation_time"`
	Historical      bool   `json:"historical"`
	State           string `json:"state"`
	AssessmentState string `json:"assessment_state"`
}
type aggregatorDetail struct {
	Data         hazard                  `json:"data"`
	SourceStatus map[string]sourceStatus `json:"source_status"`
	Metadata     recordMetadata          `json:"metadata"`
	AsOf         string                  `json:"as_of"`
}

// Accept the earlier internal bare-object response during a rolling upgrade.
func (d *aggregatorDetail) UnmarshalJSON(b []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return err
	}
	if _, ok := fields["data"]; !ok {
		if err := json.Unmarshal(b, &d.Data); err != nil {
			return err
		}
		d.SourceStatus = map[string]sourceStatus{d.Data.Source: detailStatus(d.Data)}
		return nil
	}
	if err := requireJSONFields(b, "data", "source_status", "metadata", "as_of"); err != nil {
		return err
	}
	type plain aggregatorDetail
	var v plain
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*d = aggregatorDetail(v)
	return nil
}
