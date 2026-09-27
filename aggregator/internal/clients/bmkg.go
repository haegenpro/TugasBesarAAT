// Package clients holds the Aggregator's outbound HTTP clients for BMKG and
// PVMBG, each with a bounded context timeout and structured outbound-call
// logging.
package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"aggregator/internal/middleware"
)

type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKM          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}

type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}

type BMKGClient struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
	Logger  *slog.Logger
}

func NewBMKGClient(baseURL, apiKey string, logger *slog.Logger) *BMKGClient {
	return &BMKGClient{
		BaseURL: baseURL,
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 2 * time.Second},
		Logger:  logger,
	}
}

func (c *BMKGClient) get(ctx context.Context, correlationID, path string, out interface{}) error {
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-BMKG-Key", c.APIKey)
	req.Header.Set(middleware.CorrelationIDHeader, correlationID)

	start := time.Now()
	resp, err := c.HTTP.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		middleware.LogOutbound(c.Logger, "aggregator", correlationID, "bmkg", path, 0, false, latency, err.Error())
		return err
	}
	defer resp.Body.Close()

	success := resp.StatusCode < 400
	middleware.LogOutbound(c.Logger, "aggregator", correlationID, "bmkg", path, resp.StatusCode, success, latency, "")
	if !success {
		return fmt.Errorf("bmkg %s returned status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *BMKGClient) FetchSeismicEvents(ctx context.Context, correlationID string, since time.Time) ([]SeismicEvent, error) {
	path := "/seismic-events"
	if !since.IsZero() {
		path += "?since=" + since.UTC().Format(time.RFC3339Nano)
	}
	var events []SeismicEvent
	if err := c.get(ctx, correlationID, path, &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (c *BMKGClient) FetchTsunamiWarnings(ctx context.Context, correlationID string, since time.Time) ([]TsunamiWarning, error) {
	path := "/tsunami-warnings"
	if !since.IsZero() {
		path += "?since=" + since.UTC().Format(time.RFC3339Nano)
	}
	var warnings []TsunamiWarning
	if err := c.get(ctx, correlationID, path, &warnings); err != nil {
		return nil, err
	}
	return warnings, nil
}
