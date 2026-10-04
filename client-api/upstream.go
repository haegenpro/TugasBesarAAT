package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type api struct {
	cfg    config
	log    logger
	client *http.Client
}

type logger interface {
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any)
}

var errUpstream = errors.New("upstream unavailable")

func (a *api) call(c *gin.Context, service, method, endpoint string, payload any, internal bool, output any) (int, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(c.Request.Context(), method, endpoint, body)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	if internal {
		req.Header.Set("X-Internal-Key", a.cfg.AuthInternalKey)
	}
	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil {
		a.log.Warn("outbound request", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", service, "target_endpoint", req.URL.Path, "latency_ms", time.Since(start).Milliseconds(), "success", false, "error", err.Error())
		return 0, errUpstream
	}
	defer resp.Body.Close()
	fields := []any{"service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", service, "target_endpoint", req.URL.Path, "status_code", resp.StatusCode, "latency_ms", time.Since(start).Milliseconds(), "success", resp.StatusCode < 500}
	if resp.StatusCode >= 500 {
		a.log.Warn("outbound request", fields...)
	} else {
		a.log.Info("outbound request", fields...)
	}
	if output != nil && resp.StatusCode == http.StatusOK {
		decoder := json.NewDecoder(io.LimitReader(resp.Body, 4<<20))
		if err := decoder.Decode(output); err != nil {
			a.log.Error("upstream contract violation", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", service, "target_endpoint", req.URL.Path, "error", err.Error())
			return resp.StatusCode, fmt.Errorf("decode %s response: %w", service, err)
		}
	}
	return resp.StatusCode, nil
}

func joinURL(base, path string) string {
	return strings.TrimRight(base, "/") + path
}

func filterQuery(c *gin.Context) (url.Values, error) {
	query := url.Values{}
	for _, key := range []string{"since", "hazard_type", "severity", "area_name"} {
		if value := c.Query(key); value != "" {
			query.Set(key, value)
		}
	}
	if since := query.Get("since"); since != "" {
		if _, err := time.Parse(time.RFC3339Nano, since); err != nil {
			return nil, errors.New("since must be an ISO 8601 timestamp")
		}
	}
	if value := query.Get("hazard_type"); value != "" && value != "SEISMIC" && value != "VOLCANIC" {
		return nil, errors.New("invalid hazard_type")
	}
	if value := query.Get("severity"); value != "" && value != "NORMAL" && value != "WASPADA" && value != "SIAGA" && value != "AWAS" {
		return nil, errors.New("invalid severity")
	}
	return query, nil
}
