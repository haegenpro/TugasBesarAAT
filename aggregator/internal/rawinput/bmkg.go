package rawinput

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/clients"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"unicode/utf8"
)

type Record struct {
	Cursor  int64           `json:"cursor"`
	Payload json.RawMessage `json:"payload"`
}

func DecodeBMKG(ctx context.Context, c *avrowire.Client, r *avrowire.SourceReader, meta CaptureCursor, payload []byte) ([]Record, CaptureCursor, error) {
	if r == nil {
		return nil, meta, fmt.Errorf("explicit BMKG reader required")
	}
	reader := r.Live
	if meta.History {
		reader = r.History
	}
	v, err := c.DecodeWithReader(ctx, payload, reader)
	if err != nil {
		return nil, meta, err
	}
	if err = sourceValue(v, 0); err != nil {
		return nil, meta, err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, meta, err
	}
	var records []Record
	if meta.History {
		var p struct {
			Data []Record `json:"data"`
			Next int64    `json:"next_cursor"`
			More bool     `json:"has_more"`
		}
		if err = json.Unmarshal(b, &p); err != nil {
			return nil, meta, err
		}
		if p.Data == nil {
			return nil, meta, fmt.Errorf("history data required")
		}
		last := meta.After
		for _, item := range p.Data {
			if item.Cursor <= last || len(item.Payload) == 0 {
				return nil, meta, fmt.Errorf("invalid archive ordering")
			}
			last = item.Cursor
		}
		if p.Next != last || p.More && len(p.Data) == 0 {
			return nil, meta, fmt.Errorf("invalid history continuation")
		}
		meta.Next, meta.HasMore = p.Next, p.More
		records = p.Data
	} else {
		var a []json.RawMessage
		if err = json.Unmarshal(b, &a); err != nil || a == nil {
			return nil, meta, fmt.Errorf("live array required")
		}
		records = make([]Record, len(a))
		for i, p := range a {
			records[i] = Record{Cursor: meta.SourceSequence, Payload: p}
		}
	}
	for _, record := range records {
		if meta.Entity == "seismic-events" {
			event, err := clients.DecodeSeismic(record.Payload)
			if err != nil || event.RegionName == "" || math.IsNaN(event.EpicenterLat) || math.IsNaN(event.EpicenterLon) || event.EpicenterLat < -90 || event.EpicenterLat > 90 || event.EpicenterLon < -180 || event.EpicenterLon > 180 {
				return nil, meta, canonical.Fail(400, "invalid_request", "BMKG seismic adapter validation failed")
			}
		}
		if meta.Entity == "tsunami-warnings" {
			if _, err := clients.DecodeWarning(record.Payload); err != nil {
				return nil, meta, canonical.Fail(400, "invalid_request", "BMKG warning adapter validation failed")
			}
		}
	}
	return records, meta, nil
}
func sourceValue(v any, depth int) error {
	if depth > 64 {
		return canonical.Fail(400, "invalid_request", "BMKG value exceeds depth 64")
	}
	switch value := v.(type) {
	case string:
		if !utf8.ValidString(value) {
			return canonical.Fail(400, "invalid_request", "invalid UTF-8 source string")
		}
	case map[string]any:
		for key, x := range value {
			if !utf8.ValidString(key) {
				return canonical.Fail(400, "invalid_request", "invalid UTF-8 source key")
			}
			if err := sourceValue(x, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, x := range value {
			if err := sourceValue(x, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
