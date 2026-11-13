package store

import (
	"aggregator/internal/domain"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"time"
)

var ErrCursor = errors.New("invalid cursor or cursor filters changed")

type PageRequest struct {
	Limit  int
	Cursor string
}
type Page struct {
	Events           []domain.HazardEvent
	Metadata         map[string]domain.RecordMetadata
	NextCursor       *string
	Partial          bool
	UnavailableUnits []string
}
type cursor struct {
	Time  time.Time `json:"time"`
	ID    string    `json:"id"`
	Scope string    `json:"scope"`
}

func filterScope(f Filters) string {
	b, _ := json.Marshal(struct {
		Since                time.Time
		Type, Severity, Area string
	}{f.Since, f.HazardType, f.Severity, f.AreaName})
	h := sha256.Sum256(b)
	return base64.RawURLEncoding.EncodeToString(h[:])
}
func (s *Store) QueryPage(ctx context.Context, f Filters, p PageRequest) (Page, error) {
	if s.Reads.Inventory != nil {
		return s.distributedPage(ctx, f, p)
	}
	out := Page{UnavailableUnits: []string{}}
	if p.Limit < 1 || p.Limit > 1000 {
		return out, ErrCursor
	}
	scope := filterScope(f)
	if p.Cursor != "" {
		if len(p.Cursor) > 2048 {
			return out, ErrCursor
		}
		b, e := base64.RawURLEncoding.DecodeString(p.Cursor)
		var c cursor
		if e != nil || json.Unmarshal(b, &c) != nil || c.Time.IsZero() || c.Scope != scope {
			return out, ErrCursor
		}
		if parsed, err := uuid.Parse(c.ID); err != nil || parsed.String() != c.ID {
			return out, ErrCursor
		}
		f.BeforeTime = &c.Time
		f.BeforeID = c.ID
	}
	f.Limit = p.Limit + 1
	events, metadata, err := s.QueryWithMetadata(ctx, f)
	if err != nil {
		return out, err
	}
	if len(events) > p.Limit {
		events = events[:p.Limit]
		last := events[len(events)-1]
		b, _ := json.Marshal(cursor{last.OccurredAt.Time, last.HazardID, scope})
		next := base64.RawURLEncoding.EncodeToString(b)
		out.NextCursor = &next
	}
	out.Events = events
	out.Metadata = make(map[string]domain.RecordMetadata, len(events))
	for _, e := range events {
		out.Metadata[e.HazardID] = metadata[e.HazardID]
	}
	return out, nil
}
