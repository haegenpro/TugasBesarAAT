package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

type api struct {
	cfg            config
	log            logger
	client         *http.Client
	chatClient     *http.Client
	authSlots      chan struct{}
	fieldAuthSlots chan struct{}
	fieldClient    *http.Client
}

type logger interface {
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any)
}

var errUpstream = errors.New("upstream unavailable")

const retryDelay = 25 * time.Millisecond

// retryableTransportError reports whether an idempotent request without a body failed at
// the connection level, so repeating it once cannot apply an effect twice. Timeouts and
// cancellations are never retried: under overload a second attempt only adds load.
func retryableTransportError(req *http.Request, err error) bool {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return false
	}
	if req.Body != nil && req.Body != http.NoBody {
		return false
	}
	if req.Context().Err() != nil {
		return false
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return false
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

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
	if service == "aggregator" && a.cfg.FieldReadToken != "" {
		if value, ok := c.Get("principal"); ok {
			if p, valid := value.(principal); valid && p.Role == "field" {
				for _, prefix := range []string{"/internal/hazards", "/internal/notices"} {
					if req.URL.Path == prefix || strings.HasPrefix(req.URL.Path, prefix+"/") {
						req.URL.Path = "/internal/field" + strings.TrimPrefix(req.URL.Path, "/internal")
						req.Header.Set("X-Field-Read-Key", a.cfg.FieldReadToken)
						break
					}
				}
			}
		}
	}
	req.Header.Set("X-Correlation-ID", c.GetString("correlation_id"))
	if internal {
		req.Header.Set("X-Internal-Key", a.cfg.AuthInternalKey)
	}
	start := time.Now()
	client := a.client
	if c.GetBool("field_auth_lane") && a.fieldClient != nil {
		client = a.fieldClient
	}
	resp, err := client.Do(req)
	if err != nil && retryableTransportError(req, err) {
		// The chosen pod stopped listening while Service endpoints were still being
		// updated. Another attempt is routed to a live endpoint, so the caller does not
		// see a 503 for a request that was never served.
		a.log.Warn("outbound request retry", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", service, "target_endpoint", req.URL.Path, "error", err.Error())
		select {
		case <-time.After(retryDelay):
		case <-c.Request.Context().Done():
		}
		resp, err = client.Do(req)
	}
	if err != nil {
		countUpstreamFailure(service, failureClass(err))
		a.log.Warn("outbound request", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", service, "target_endpoint", req.URL.Path, "latency_ms", time.Since(start).Milliseconds(), "success", false, "error", err.Error())
		return 0, errUpstream
	}
	defer resp.Body.Close()
	headersLatency := time.Since(start)
	var decodeLatency time.Duration
	var decodeErr error
	defer func() {
		// Small unread response bodies (notably Auth 429s) must be consumed so
		// the transport can reuse its bounded connections under overload.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32<<10))
		fields := []any{"service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", service, "target_endpoint", req.URL.Path, "status_code", resp.StatusCode,
			"latency_ms", time.Since(start).Milliseconds(), "headers_latency_ms", headersLatency.Milliseconds(),
			"decode_latency_ms", decodeLatency.Milliseconds(), "success", resp.StatusCode < 500 && decodeErr == nil}
		if resp.StatusCode >= 500 {
			countUpstreamFailure(service, "status_5xx")
		}
		if resp.StatusCode >= 500 || decodeErr != nil {
			a.log.Warn("outbound request", fields...)
		} else {
			a.log.Info("outbound request", fields...)
		}
	}()
	if output != nil && resp.StatusCode == http.StatusOK {
		decodeStart := time.Now()
		decoder := json.NewDecoder(io.LimitReader(resp.Body, 4<<20))
		decodeErr = decoder.Decode(output)
		decodeLatency = time.Since(decodeStart)
		if decodeErr != nil {
			a.log.Error("upstream contract violation", "service", "client-api", "correlation_id", c.GetString("correlation_id"), "target_service", service, "target_endpoint", req.URL.Path, "error", decodeErr.Error())
			return resp.StatusCode, fmt.Errorf("decode %s response: %w", service, decodeErr)
		}
	}
	return resp.StatusCode, nil
}

func joinURL(base, path string) string {
	return strings.TrimRight(base, "/") + path
}

func filterQuery(c *gin.Context) (url.Values, error) {
	query := url.Values{}
	for _, key := range []string{"since", "hazard_type", "severity", "area_name", "limit", "cursor"} {
		if value := c.Query(key); value != "" {
			query.Set(key, value)
		}
	}
	if raw := query.Get("limit"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 1000 {
			return nil, errors.New("limit must be between 1 and 1000")
		}
	}
	if len(query.Get("cursor")) > 2048 {
		return nil, errors.New("invalid cursor")
	}
	if since := query.Get("since"); since != "" {
		if _, err := time.Parse(time.RFC3339Nano, since); err != nil {
			return nil, errors.New("since must be an ISO 8601 timestamp")
		}
	}
	if value := query.Get("hazard_type"); value != "" && !validHazardType(value) {
		return nil, errors.New("invalid hazard_type")
	}
	if value := query.Get("severity"); value != "" && !validSeverity(value) {
		return nil, errors.New("invalid severity")
	}
	return query, nil
}
