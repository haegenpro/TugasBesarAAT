// Package audit defines the prescribed live Audit API. A live page is never a
// retained snapshot; listing and checksum reconcile at a stable processing point.
package audit

import (
	"aggregator/internal/domain"
	"aggregator/internal/pagecursor"
	"context"
	"sort"
	"time"
)

type Query struct {
	Snapshot string
	Source   string
	Since    time.Time
	Limit    int
	After    pagecursor.Position
	Scope    string
}

func (q Query) Filters() string { return q.Source + "|" + q.Since.UTC().Format(time.RFC3339Nano) }

type Page struct {
	Items       []domain.HazardEvent `json:"items"`
	NextCursor  *string              `json:"next_cursor"`
	Partial     bool                 `json:"partial"`
	Unavailable []string             `json:"unavailable_units"`
}
type Checksum struct {
	Source      *string   `json:"source"`
	Count       int64     `json:"count"`
	Checksum    string    `json:"checksum"`
	Algorithm   string    `json:"algorithm"`
	AsOf        time.Time `json:"as_of"`
	Partial     bool      `json:"partial"`
	Unavailable []string  `json:"unavailable_units"`
}
type Location struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}
type Locations struct {
	ID          string     `json:"hazard_id"`
	Locations   []Location `json:"locations"`
	Partial     bool       `json:"partial"`
	Unavailable []string   `json:"unavailable_units,omitempty"`
}
type Repository interface {
	AuditEvents(context.Context, Query) (Page, error)
	AuditChecksum(context.Context, string) (Checksum, error)
	AuditLocation(context.Context, string) (Locations, error)
}
type Unit struct {
	Name, Node, Host              string
	Group                         int
	Available, Primary, RoleKnown bool
}
type Inventory struct {
	Units    []Unit
	Topology string
}

func (i Inventory) Unavailable() []string {
	out := []string{}
	for _, u := range i.Units {
		if !u.Available {
			out = append(out, u.Name)
		}
	}
	sort.Strings(out)
	return out
}
func (i Inventory) Primary(group int) (Unit, bool) {
	for _, u := range i.Units {
		if u.Group == group && u.Available && u.RoleKnown && u.Primary {
			return u, true
		}
	}
	return Unit{}, false
}
