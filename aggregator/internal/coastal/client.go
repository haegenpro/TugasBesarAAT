package coastal

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrUnauthorized = errors.New("sensor rejected the configured credential")

type Station struct {
	ID        string  `json:"station_id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Page struct {
	GeneratedAt string            `json:"generated_at"`
	Items       []json.RawMessage `json:"items"`
	NextCursor  *string           `json:"next_cursor"`
}

type Client struct {
	BaseURL      string
	Token        string
	HTTP         *http.Client
	Timeout      time.Duration
	Limiter      *Limiter
	Stations     *Breaker
	Observations *Breaker
	MaxBody      int64
}

type SourceError struct {
	Status int
	Reason string
}

func (e *SourceError) Error() string {
	if e.Status == 0 {
		return "sensor unavailable: " + e.Reason
	}
	return fmt.Sprintf("sensor returned %d: %s", e.Status, e.Reason)
}

func (c *Client) get(ctx context.Context, b *Breaker, path string, out any) error {
	if err := b.Allow(); err != nil {
		return err
	}
	if err := c.Limiter.Wait(ctx); err != nil {
		b.Release()
		return err
	}
	call, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(call, http.MethodGet, strings.TrimRight(c.BaseURL, "/")+path, nil)
	if err != nil {
		b.Release()
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "application/json")
	if cid := middleware.CorrelationFromContext(ctx); cid != "" {
		req.Header.Set(middleware.CorrelationIDHeader, cid)
	}
	started := time.Now()
	resp, err := c.HTTP.Do(req)
	platform.Observe("coastal_"+b.Name+"_request_seconds", time.Since(started).Seconds())
	if err != nil {
		if _, waiting := acquisition.WaitDelay(err); waiting {
			b.Release()
			return err
		}
		if ctx.Err() != nil {
			b.Release()
			return ctx.Err()
		}
		b.Failure()
		return &SourceError{Reason: err.Error()}
	}
	defer resp.Body.Close()
	limit := c.MaxBody
	if limit <= 0 {
		limit = 4 << 20
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		b.Release()
		return ErrUnauthorized
	case resp.StatusCode != http.StatusOK:
		b.Failure()
		return &SourceError{Status: resp.StatusCode, Reason: http.StatusText(resp.StatusCode)}
	case err != nil:
		b.Failure()
		return &SourceError{Status: resp.StatusCode, Reason: "truncated body: " + err.Error()}
	case int64(len(body)) > limit:
		b.Failure()
		return &SourceError{Status: resp.StatusCode, Reason: "oversized body"}
	}
	if err = json.Unmarshal(body, out); err != nil {
		b.Failure()
		return &SourceError{Status: resp.StatusCode, Reason: "malformed body: " + err.Error()}
	}
	b.Success()
	return nil
}

func (c *Client) FetchStations(ctx context.Context) ([]Station, error) {
	var body struct {
		Items []Station `json:"items"`
	}
	if err := c.get(ctx, c.Stations, "/v1/stations", &body); err != nil {
		return nil, err
	}
	if body.Items == nil {
		c.Stations.Failure()
		return nil, &SourceError{Status: 200, Reason: "stations missing items"}
	}
	return body.Items, nil
}

func (c *Client) FetchObservations(ctx context.Context, cursor *string, limit int) (Page, error) {
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if cursor != nil && *cursor != "" {
		q.Set("cursor", *cursor)
	}
	var page Page
	if err := c.get(ctx, c.Observations, "/v1/observations?"+q.Encode(), &page); err != nil {
		return page, err
	}
	if page.Items == nil {
		c.Observations.Failure()
		return page, &SourceError{Status: 200, Reason: "observations missing items"}
	}
	return page, nil
}
