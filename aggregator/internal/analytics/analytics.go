// Package analytics defines exact, bounded canonical aggregates, not projections.
package analytics

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/rawinput"
	"context"
	"crypto/subtle"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http"
	"net/url"
	"time"
)

const MaxResponse = 2 << 20

type Query struct {
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Interval string    `json:"interval"`
	Source   string    `json:"source,omitempty"`
	Snapshot string    `json:"snapshot_id,omitempty"`
}
type Bucket struct {
	At      time.Time        `json:"at"`
	Counts  map[string]int64 `json:"counts"`
	Tsunami int64            `json:"tsunami_warning_overlay"`
}
type Area struct {
	Name  string `json:"area_name"`
	Count int64  `json:"count"`
}
type Result struct {
	Query        Query            `json:"filters"`
	Total        int64            `json:"total"`
	Types        map[string]int64 `json:"by_type"`
	Severity     map[string]int64 `json:"by_severity"`
	Areas        []Area           `json:"by_area"`
	Series       []Bucket         `json:"series"`
	Tsunami      int64            `json:"tsunami_warning_overlay"`
	AsOf         time.Time        `json:"as_of"`
	Mode         string           `json:"mode"`
	Generation   int64            `json:"generation"`
	DataComplete bool             `json:"data_complete"`
	Partial      bool             `json:"partial"`
	Unavailable  []string         `json:"unavailable_units"`
	Freshness    any              `json:"source_freshness"`
}

func Parse(v url.Values, now time.Time) (Query, error) {
	q := Query{To: now.UTC(), Interval: "day"}
	bad := canonical.Fail(400, "bad_request", "invalid analytics filters")
	for k, values := range v {
		if len(values) != 1 {
			return q, bad
		}
		switch k {
		case "from", "to", "interval", "source", "snapshot_id":
		default:
			return q, bad
		}
	}
	parse := func(k string, dst *time.Time) error {
		if x, ok := v[k]; ok {
			t, e := time.Parse(time.RFC3339Nano, x[0])
			_, off := t.Zone()
			if e != nil || off != 0 || t.IsZero() {
				return bad
			}
			*dst = t.UTC()
		}
		return nil
	}
	if e := parse("to", &q.To); e != nil {
		return q, e
	}
	q.From = q.To.Add(-7 * 24 * time.Hour)
	if e := parse("from", &q.From); e != nil {
		return q, e
	}
	if x, ok := v["interval"]; ok {
		q.Interval = x[0]
	}
	step := 24 * time.Hour
	if q.Interval == "hour" {
		step = time.Hour
	} else if q.Interval != "day" {
		return q, bad
	}
	if !q.From.Before(q.To) || q.To.Sub(q.From) > 366*24*time.Hour || int((q.To.Sub(q.From.Truncate(step))-1)/step)+1 > 1000 {
		return q, bad
	}
	if x, ok := v["source"]; ok {
		q.Source = x[0]
		if !domain.ValidSource(q.Source) {
			return q, bad
		}
	}
	if x, ok := v["snapshot_id"]; ok {
		q.Snapshot = x[0]
		id, e := uuid.Parse(q.Snapshot)
		if e != nil || id.String() != q.Snapshot {
			return q, bad
		}
	}
	return q, nil
}

type Repository interface {
	AnalyticsSummary(context.Context, Query) (Result, error)
}
type HTTP struct {
	Repository Repository
	Token      string
}

func (h HTTP) Register(r gin.IRoutes) {
	r.GET("/internal/analytics/summary", func(c *gin.Context) {
		if h.Token == "" || subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+h.Token)) != 1 {
			rawinput.Error(c, canonical.Fail(401, "unauthorized", "analytics service credential required"))
			return
		}
		q, e := Parse(c.Request.URL.Query(), time.Now())
		if e != nil {
			rawinput.Error(c, e)
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), canonical.Deadline)
		defer cancel()
		result, e := h.Repository.AnalyticsSummary(ctx, q)
		if e != nil {
			rawinput.Error(c, e)
			return
		}
		c.JSON(http.StatusOK, result)
	})
}
