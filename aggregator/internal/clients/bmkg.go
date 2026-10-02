// Package clients holds the Aggregator's outbound HTTP clients for BMKG and
// PVMBG, each with a bounded context timeout and structured outbound-call
// logging.
package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"aggregator/internal/avrowire"
	"aggregator/internal/middleware"
)

type SeismicEvent struct {
	EventID          string         `json:"event_id"`
	Magnitude        float64        `json:"magnitude"`
	DepthKM          float64        `json:"depth_km"`
	EpicenterLat     float64        `json:"epicenter_lat"`
	EpicenterLon     float64        `json:"epicenter_lon"`
	RegionName       string         `json:"region_name"`
	OccurredAt       time.Time      `json:"occurred_at"`
	PotentialTsunami bool           `json:"potential_tsunami"`
	Attributes       map[string]any `json:"-"`
}

type TsunamiWarning struct {
	WarningID        string         `json:"warning_id"`
	RelatedEventID   string         `json:"related_event_id"`
	ThreatLevel      string         `json:"threat_level"`
	AffectedZones    []string       `json:"affected_zones"`
	EstimatedArrival time.Time      `json:"estimated_arrival"`
	Attributes       map[string]any `json:"-"`
}

type BMKGClient struct {
	BaseURL  string
	APIKey   string
	HTTP     *http.Client
	Logger   *slog.Logger
	Registry *avrowire.Client
}

func NewBMKGClient(baseURL, apiKey string, logger *slog.Logger) *BMKGClient {
	return &BMKGClient{
		BaseURL: baseURL,
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 2 * time.Second},
		Logger:  logger,
	}
}

func (c *BMKGClient) get(ctx context.Context, correlationID, path string, required []string, out interface{}) error {
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-BMKG-Key", c.APIKey)
	if c.Registry != nil {
		req.Header.Set("Accept", avrowire.MediaType)
	}
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
	var records []map[string]any
	if c.Registry == nil {
		if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
			return fmt.Errorf("%w: bmkg %s JSON: %v", ErrContractViolation, path, err)
		}
	} else {
		if resp.Header.Get("Content-Type") != avrowire.MediaType {
			return fmt.Errorf("%w: bmkg %s did not return Avro", ErrContractViolation, path)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		if err != nil {
			return err
		}
		native, err := c.Registry.Decode(avrowire.WithCorrelationID(callCtx, correlationID), body)
		if err != nil {
			return err
		}
		records, err = avrowire.Records(native)
		if err != nil {
			return fmt.Errorf("%w: bmkg %s: %v", ErrContractViolation, path, err)
		}
	}
	if records == nil {
		return fmt.Errorf("%w: bmkg %s returned null instead of an array", ErrContractViolation, path)
	}
	for i, record := range records {
		if err := requireFields(record, required...); err != nil {
			return fmt.Errorf("bmkg %s record %d: %w", path, i, err)
		}
	}
	encoded, err := json.Marshal(records)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(encoded, out); err != nil {
		return fmt.Errorf("%w: bmkg %s: %v", ErrContractViolation, path, err)
	}
	switch typed := out.(type) {
	case *[]SeismicEvent:
		for i, event := range *typed {
			if event.EventID == "" {
				return fmt.Errorf("%w: bmkg %s record %d: empty event_id", ErrContractViolation, path, i)
			}
			(*typed)[i].Attributes = unmappedFields(records[i], "event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami")
		}
	case *[]TsunamiWarning:
		for i, warning := range *typed {
			if warning.WarningID == "" || warning.RelatedEventID == "" {
				return fmt.Errorf("%w: bmkg %s record %d: empty warning identifier", ErrContractViolation, path, i)
			}
			switch warning.ThreatLevel {
			case "Waspada", "Siaga", "Awas":
			default:
				return fmt.Errorf("%w: bmkg %s record %d: invalid threat_level %q", ErrContractViolation, path, i, warning.ThreatLevel)
			}
			(*typed)[i].Attributes = unmappedFields(records[i], "warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival")
		}
	}
	return nil
}

func (c *BMKGClient) FetchSeismicEvents(ctx context.Context, correlationID string, since time.Time) ([]SeismicEvent, error) {
	path := "/seismic-events"
	if !since.IsZero() {
		path += "?since=" + since.UTC().Format(time.RFC3339Nano)
	}
	var events []SeismicEvent
	if err := c.get(ctx, correlationID, path, []string{"event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami"}, &events); err != nil {
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
	if err := c.get(ctx, correlationID, path, []string{"warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival"}, &warnings); err != nil {
		return nil, err
	}
	return warnings, nil
}
