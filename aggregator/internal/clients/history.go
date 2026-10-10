package clients

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/middleware"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type HistoryItem struct {
	Cursor  int64           `json:"cursor"`
	Payload json.RawMessage `json:"payload"`
}
type HistoryPage struct {
	Data       []HistoryItem `json:"data"`
	NextCursor int64         `json:"next_cursor"`
	HasMore    bool          `json:"has_more"`
}

func fetchHistory(ctx context.Context, httpClient *http.Client, base, header, key, source, entity, cid string, after int64, logger *slog.Logger, registry *avrowire.Client) (HistoryPage, error) {
	p := HistoryPage{}
	path := "/history?entity=" + url.QueryEscape(entity) + "&after=" + strconv.FormatInt(after, 10) + "&limit=100"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return p, err
	}
	req.Header.Set(header, key)
	if registry != nil {
		req.Header.Set("Accept", avrowire.MediaType)
	}
	req.Header.Set(middleware.CorrelationIDHeader, cid)
	started := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		middleware.LogOutbound(logger, "aggregator", cid, source, path, 0, false, time.Since(started).Milliseconds(), err.Error())
		return p, err
	}
	defer resp.Body.Close()
	middleware.LogOutbound(logger, "aggregator", cid, source, path, resp.StatusCode, resp.StatusCode == 200, time.Since(started).Milliseconds(), "")
	if resp.StatusCode != 200 {
		return p, fmt.Errorf("history returned status %d", resp.StatusCode)
	}
	if registry != nil {
		if resp.Header.Get("Content-Type") != avrowire.MediaType {
			return p, fmt.Errorf("%w: %s history requires Avro", ErrContractViolation, source)
		}
		body, e := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
		if e != nil {
			return p, e
		}
		if len(body) > 4<<20 {
			return p, fmt.Errorf("history body exceeds 4 MiB")
		}
		native, e := registry.Decode(avrowire.WithCorrelationID(ctx, cid), body)
		if e != nil {
			return p, fmt.Errorf("%s history: %w", source, e)
		}
		body, e = json.Marshal(native)
		if e != nil {
			return p, e
		}
		if e = json.Unmarshal(body, &p); e != nil {
			return p, e
		}
	} else if err = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&p); err != nil {
		return p, err
	}
	if p.Data == nil {
		return p, fmt.Errorf("%w: missing history data", ErrContractViolation)
	}
	last := after
	for _, i := range p.Data {
		if i.Cursor <= last || len(i.Payload) == 0 {
			return p, fmt.Errorf("%w: invalid history cursor", ErrContractViolation)
		}
		last = i.Cursor
	}
	if last != p.NextCursor || (p.HasMore && len(p.Data) == 0) {
		return p, fmt.Errorf("%w: inconsistent history page", ErrContractViolation)
	}
	return p, nil
}
func (c *BMKGClient) FetchHistory(ctx context.Context, cid, entity string, after int64) (HistoryPage, error) {
	return fetchHistory(ctx, c.HTTP, c.BaseURL, "X-BMKG-Key", c.APIKey, "bmkg", entity, cid, after, c.Logger, c.Registry)
}
func (c *PVMBGClient) FetchHistory(ctx context.Context, cid, entity string, after int64) (HistoryPage, error) {
	return fetchHistory(ctx, c.HTTP, c.BaseURL, "Authorization", "Bearer "+c.Token, "pvmbg", entity, cid, after, c.Logger, c.Registry)
}
func DecodeSeismic(raw json.RawMessage) (SeismicEvent, error) {
	var e SeismicEvent
	var fields map[string]any
	if err := decodeHistoryFields(raw, &fields); err != nil {
		return e, err
	}
	if err := requireFields(fields, "event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami"); err != nil {
		return e, err
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, err
	}
	if e.EventID == "" || e.OccurredAt.IsZero() {
		return e, ErrContractViolation
	}
	e.Attributes = unmappedFields(fields, "event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami")
	return e, nil
}
func DecodeWarning(raw json.RawMessage) (TsunamiWarning, error) {
	var w TsunamiWarning
	var fields map[string]any
	if err := decodeHistoryFields(raw, &fields); err != nil {
		return w, err
	}
	if err := requireFields(fields, "warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival"); err != nil {
		return w, err
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return w, err
	}
	if w.WarningID == "" || w.RelatedEventID == "" || w.EstimatedArrival.IsZero() {
		return w, ErrContractViolation
	}
	switch w.ThreatLevel {
	case "Waspada", "Siaga", "Awas":
	default:
		return w, ErrContractViolation
	}
	w.Attributes = unmappedFields(fields, "warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival")
	return w, nil
}
func DecodeVolcanic(raw json.RawMessage) (VolcanicReport, error) {
	var fields map[string]any
	if err := decodeHistoryFields(raw, &fields); err != nil {
		return VolcanicReport{}, err
	}
	return decodeVolcanicReport(fields)
}

// Preserve writer long values in extension attributes while mapping core fields.
func decodeHistoryFields(raw []byte, out *map[string]any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(out)
}
