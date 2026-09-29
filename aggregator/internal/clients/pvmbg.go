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

// VolcanicReport decodes generically so any field PVMBG adds beyond the
// known set (schema-evolution requirement) survives into Extra rather than
// being silently discarded by a rigid struct decoder.
type VolcanicReport struct {
	ReportID         string
	VolcanoID        string
	AlertLevel       string
	EruptionCount24h float64
	AshColumnHeightM float64
	ReportedAt       time.Time
	Extra            map[string]interface{}
}

var knownVolcanicFields = map[string]bool{
	"report_id": true, "volcano_id": true, "alert_level": true,
	"eruption_count_24h": true, "ash_column_height_m": true, "reported_at": true,
}

func decodeVolcanicReport(raw map[string]interface{}) (VolcanicReport, error) {
	r := VolcanicReport{Extra: map[string]interface{}{}}
	if v, ok := raw["report_id"].(string); ok {
		r.ReportID = v
	}
	if v, ok := raw["volcano_id"].(string); ok {
		r.VolcanoID = v
	}
	if v, ok := raw["alert_level"].(string); ok {
		r.AlertLevel = v
	}
	if v, ok := raw["eruption_count_24h"].(float64); ok {
		r.EruptionCount24h = v
	}
	if v, ok := raw["ash_column_height_m"].(float64); ok {
		r.AshColumnHeightM = v
	}
	if v, ok := raw["reported_at"].(string); ok {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return r, fmt.Errorf("invalid reported_at: %w", err)
		}
		r.ReportedAt = t
	}
	for k, v := range raw {
		if !knownVolcanicFields[k] {
			r.Extra[k] = v
		}
	}
	return r, nil
}

type PVMBGClient struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
	Logger  *slog.Logger
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
	if err := json.NewDecoder(resp.Body).Decode(&raws); err != nil {
		return nil, err
	}
	reports := make([]VolcanicReport, 0, len(raws))
	for _, raw := range raws {
		r, err := decodeVolcanicReport(raw)
		if err != nil {
			return nil, err
		}
		reports = append(reports, r)
	}
	return reports, nil
}
