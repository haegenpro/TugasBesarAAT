package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"aggregator/internal/avrowire"
	"aggregator/internal/middleware"
)

// VolcanicReport contains mapped PVMBG fields and source-specific attributes.
type VolcanicReport struct {
	SourceSequence   int64
	ReportID         string
	VolcanoID        string
	AlertLevel       string
	EruptionCount24h int
	AshColumnHeightM float64
	ReportedAt       time.Time
	Attributes       map[string]any
}

func decodeVolcanicReport(raw map[string]interface{}) (VolcanicReport, error) {
	var r VolcanicReport
	var err error
	if r.ReportID, err = requiredString(raw, "report_id"); err != nil {
		return r, err
	}
	if r.VolcanoID, err = requiredString(raw, "volcano_id"); err != nil {
		return r, err
	}
	if r.AlertLevel, err = requiredString(raw, "alert_level"); err != nil {
		return r, err
	}
	switch r.AlertLevel {
	case "Normal", "Waspada", "Siaga", "Awas":
	default:
		return r, fmt.Errorf("%w: invalid alert_level %q", ErrContractViolation, r.AlertLevel)
	}
	if r.EruptionCount24h, err = requiredInt(raw, "eruption_count_24h"); err != nil {
		return r, err
	}
	if r.AshColumnHeightM, err = requiredNumber(raw, "ash_column_height_m"); err != nil {
		return r, err
	}
	reportedAt, err := requiredString(raw, "reported_at")
	if err != nil {
		return r, err
	}
	r.ReportedAt, err = time.Parse(time.RFC3339Nano, reportedAt)
	if err != nil {
		return r, fmt.Errorf("%w: invalid required field %q: %v", ErrContractViolation, "reported_at", err)
	}
	r.Attributes = unmappedFields(raw, "report_id", "volcano_id", "alert_level", "reported_at", "eruption_count_24h", "ash_column_height_m")
	return r, nil
}

type PVMBGClient struct {
	BaseURL  string
	Token    string
	HTTP     *http.Client
	Logger   *slog.Logger
	Registry *avrowire.Client
}

func NewPVMBGClient(baseURL, token string, logger *slog.Logger) *PVMBGClient {
	return &PVMBGClient{
		BaseURL: baseURL,
		Token:   token,
		HTTP:    &http.Client{Timeout: 5 * time.Second},
		Logger:  logger,
	}
}

func (c *PVMBGClient) FetchVolcanicReports(ctx context.Context, correlationID string, since time.Time) ([]VolcanicReport, error) {
	path := "/volcanic-reports"
	if !since.IsZero() {
		path += "?since=" + since.UTC().Format(time.RFC3339Nano)
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if c.Registry != nil {
		req.Header.Set("Accept", avrowire.MediaType)
	}
	req.Header.Set(middleware.CorrelationIDHeader, correlationID)

	start := time.Now()
	resp, err := c.HTTP.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		middleware.LogOutbound(c.Logger, "aggregator", correlationID, "pvmbg", path, 0, false, latency, err.Error())
		return nil, err
	}
	defer resp.Body.Close()

	success := resp.StatusCode < 400
	middleware.LogOutbound(c.Logger, "aggregator", correlationID, "pvmbg", path, resp.StatusCode, success, latency, "")
	if !success {
		return nil, fmt.Errorf("pvmbg %s returned status %d", path, resp.StatusCode)
	}

	var raws []map[string]interface{}
	if c.Registry != nil {
		if resp.Header.Get("Content-Type") != avrowire.MediaType {
			return nil, fmt.Errorf("%w: pvmbg %s did not return Avro", ErrContractViolation, path)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		if err != nil {
			return nil, err
		}
		native, err := c.Registry.Decode(avrowire.WithCorrelationID(callCtx, correlationID), body)
		if err != nil {
			return nil, fmt.Errorf("pvmbg %s: %w", path, err)
		}
		raws, err = avrowire.Records(native)
		if err != nil {
			return nil, fmt.Errorf("%w: pvmbg %s: %v", ErrContractViolation, path, err)
		}
	} else if err := json.NewDecoder(resp.Body).Decode(&raws); err != nil {
		return nil, fmt.Errorf("%w: pvmbg %s JSON: %v", ErrContractViolation, path, err)
	}
	if raws == nil {
		return nil, fmt.Errorf("%w: pvmbg %s returned null instead of an array", ErrContractViolation, path)
	}
	sequence := int64(0)
	if header := resp.Header.Get("X-Source-Sequence"); header != "" {
		var err error
		sequence, err = strconv.ParseInt(header, 10, 64)
		if err != nil || sequence < 0 {
			return nil, fmt.Errorf("%w: invalid source sequence", ErrContractViolation)
		}
	}
	reports := make([]VolcanicReport, 0, len(raws))
	for _, raw := range raws {
		r, err := decodeVolcanicReport(raw)
		if err != nil {
			return nil, fmt.Errorf("pvmbg %s record %d: %w", path, len(reports), err)
		}
		r.SourceSequence = sequence
		reports = append(reports, r)
	}
	return reports, nil
}
