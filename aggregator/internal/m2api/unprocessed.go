package m2api

import (
	"aggregator/internal/platform"
	"aggregator/internal/store"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type unprocessedCursor struct{ Owner, Position, Source string }
type unprocessedResult struct {
	Items       []json.RawMessage `json:"items"`
	Next        *string           `json:"next_cursor"`
	Partial     bool              `json:"partial,omitempty"`
	Unavailable []string          `json:"unavailable_units,omitempty"`
}

func composite(owner, position, source string) *string {
	raw, _ := json.Marshal(unprocessedCursor{owner, position, source})
	value := base64.RawURLEncoding.EncodeToString(raw)
	return &value
}
func localFailures(ctx context.Context, s *store.Store, source string, limit int, cursor string) (unprocessedResult, error) {
	page, e := s.Unprocessed(ctx, source, limit, cursor)
	if e != nil {
		return unprocessedResult{}, e
	}
	raw, e := json.Marshal(page)
	if e != nil {
		return unprocessedResult{}, e
	}
	var out unprocessedResult
	e = json.Unmarshal(raw, &out)
	return out, e
}
func remoteFailures(ctx context.Context, cfg Config, source string, limit int, cursor string) (unprocessedResult, error) {
	if cfg.IngestionURL == "" || cfg.IngestionToken == "" {
		return unprocessedResult{Items: []json.RawMessage{}}, nil
	}
	query := url.Values{"limit": {strconv.Itoa(limit)}}
	if source != "" {
		query.Set("source", source)
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	req, e := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(cfg.IngestionURL, "/")+"/internal/ops/failures?"+query.Encode(), nil)
	if e != nil {
		return unprocessedResult{}, e
	}
	req.Header.Set("Authorization", "Bearer "+cfg.IngestionToken)
	client := platform.Client(2 * time.Second)
	defer client.CloseIdleConnections()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, e := client.Do(req)
	if e != nil {
		return unprocessedResult{}, e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return unprocessedResult{}, errors.New("ingestion metadata unavailable")
	}
	raw, e := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if e != nil || len(raw) > 4<<20 {
		return unprocessedResult{}, errors.New("ingestion metadata exceeds response allowance")
	}
	var out unprocessedResult
	e = json.Unmarshal(raw, &out)
	if e == nil && out.Items == nil {
		e = errors.New("ingestion returned no metadata list")
	}
	return out, e
}
func unprocessed(ctx context.Context, s *store.Store, cfg Config, source string, limit int, cursor string) (unprocessedResult, error) {
	position := unprocessedCursor{Owner: "canonical", Source: source}
	if source == "SATELIT" || source == "SENSOR_PANTAI" {
		position.Owner = "ingestion"
	}
	if cursor != "" {
		position = unprocessedCursor{}
		raw, e := base64.RawURLEncoding.DecodeString(cursor)
		if len(cursor) > 4096 || e != nil || json.Unmarshal(raw, &position) != nil || position.Source != source || (position.Owner != "canonical" && position.Owner != "ingestion") {
			return unprocessedResult{}, store.ErrCursor
		}
	}
	out := unprocessedResult{Items: []json.RawMessage{}}
	if position.Owner == "canonical" {
		local, e := localFailures(ctx, s, source, limit, position.Position)
		if e != nil {
			return out, e
		}
		out.Items = local.Items
		if local.Next != nil {
			out.Next = composite("canonical", *local.Next, source)
			return out, nil
		}
		position.Position = ""
	}
	remaining := limit - len(out.Items)
	if remaining == 0 {
		probe, e := remoteFailures(ctx, cfg, source, 1, "")
		if e != nil {
			out.Partial = true
			out.Unavailable = []string{"ingestion"}
			out.Next = composite("ingestion", "", source)
			return out, nil
		}
		if len(probe.Items) > 0 {
			out.Next = composite("ingestion", "", source)
		}
		return out, nil
	}
	remote, e := remoteFailures(ctx, cfg, source, remaining, position.Position)
	if e != nil {
		out.Partial = true
		out.Unavailable = []string{"ingestion"}
		out.Next = composite("ingestion", position.Position, source)
		return out, nil
	}
	out.Items = append(out.Items, remote.Items...)
	if remote.Next != nil {
		out.Next = composite("ingestion", *remote.Next, source)
	}
	return out, nil
}
