package coastal

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"bytes"
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

func (c *Client) get(ctx context.Context, b *Breaker, path string, out any, validate func() error) error {
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
		return rejectResponse(ctx, resp, io.MultiReader(bytes.NewReader(body), failedResponseRead{err}), "incomplete response body")
	case int64(len(body)) > limit:
		b.Failure()
		return rejectResponse(ctx, resp, io.MultiReader(bytes.NewReader(body), resp.Body), "oversized body")
	case acquisition.ResponseOverflow(resp):
		b.Failure()
		return rejectResponse(ctx, resp, bytes.NewReader(body), "response exceeds dispatch cache bound")
	}
	if err = platform.ValidateJSON(body); err != nil {
		b.Failure()
		return rejectResponse(ctx, resp, bytes.NewReader(body), "malformed body: "+err.Error())
	}
	if err = json.Unmarshal(body, out); err != nil {
		b.Failure()
		return rejectResponse(ctx, resp, bytes.NewReader(body), "malformed body: "+err.Error())
	}
	if validate != nil {
		if err := validate(); err != nil {
			b.Failure()
			return rejectResponse(ctx, resp, bytes.NewReader(body), err.Error())
		}
	}
	b.Success()
	return nil
}

func (c *Client) FetchStations(ctx context.Context) ([]Station, error) {
	var body struct {
		Items []Station `json:"items"`
	}
	if err := c.get(ctx, c.Stations, "/v1/stations", &body, func() error {
		if body.Items == nil {
			return errors.New("stations missing items")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return body.Items, nil
}

func (c *Client) FetchObservations(ctx context.Context, cursor *string, limit int) (Page, error) {
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if cursor != nil && *cursor != "" {
		q.Set("cursor", *cursor)
	}
	var page Page
	if err := c.get(ctx, c.Observations, "/v1/observations?"+q.Encode(), &page, func() error {
		if page.Items == nil {
			return errors.New("observations missing items")
		}
		return nil
	}); err != nil {
		return page, err
	}
	return page, nil
}

type failedResponseRead struct{ err error }

func (r failedResponseRead) Read([]byte) (int, error) { return 0, r.err }
