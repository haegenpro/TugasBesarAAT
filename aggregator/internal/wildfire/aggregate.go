package wildfire

import (
	"aggregator/internal/domain"
	"fmt"
	"github.com/pymaxion/geographiclib-go/v2/geodesic"
	"math"
	"time"
)

// SumState preserves both IEEE-754 terms across durable page boundaries.
type SumState struct{ Sum, Compensation float64 }

func (s *SumState) Add(v float64) {
	y := v - s.Compensation
	t := s.Sum + y
	s.Compensation = (t - s.Sum) - y
	s.Sum = t
}

type Aggregate struct {
	Count        int64
	X, Y, Z, FRP SumState
	EarliestKey  string
	EarliestAt   time.Time
	Confidence   string
}

func (a *Aggregate) Add(key string, at time.Time, latitude, longitude, frp float64, confidence string) {
	lat, lon := latitude*math.Pi/180, longitude*math.Pi/180
	a.X.Add(math.Cos(lat) * math.Cos(lon))
	a.Y.Add(math.Cos(lat) * math.Sin(lon))
	a.Z.Add(math.Sin(lat))
	a.FRP.Add(frp)
	if a.Count == 0 || at.Before(a.EarliestAt) || at.Equal(a.EarliestAt) && key < a.EarliestKey {
		a.EarliestKey, a.EarliestAt = key, at
	}
	if a.Confidence == "" {
		a.Confidence = "n"
	}
	if confidence == "h" {
		a.Confidence = "h"
	}
	a.Count++
}
func (a Aggregate) Event(keys, satellites []string, provinces []Province) (*Event, error) {
	if a.Count < 2 || int64(len(keys)) != a.Count || len(provinces) == 0 {
		return nil, fmt.Errorf("aggregate membership integrity")
	}
	norm := math.Hypot(math.Hypot(a.X.Sum, a.Y.Sum), a.Z.Sum)
	if norm/float64(a.Count) <= 1e-12 {
		return nil, fmt.Errorf("undefined_center")
	}
	if !finite(a.FRP.Sum) {
		return nil, fmt.Errorf("numeric_overflow")
	}
	lat := math.Atan2(a.Z.Sum, math.Hypot(a.X.Sum, a.Y.Sum)) * 180 / math.Pi
	lon := math.Atan2(a.Y.Sum, a.X.Sum) * 180 / math.Pi
	if lat == 0 {
		lat = 0
	}
	if lon == 0 {
		lon = 0
	}
	nearest := provinces[0]
	best := geodesic.WGS84.Inverse(lat, lon, nearest.Latitude, nearest.Longitude).S12
	for _, p := range provinces[1:] {
		d := geodesic.WGS84.Inverse(lat, lon, p.Latitude, p.Longitude).S12
		if d < best || d == best && (p.Name < nearest.Name || p.Name == nearest.Name && p.ID < nearest.ID) {
			best, nearest = d, p
		}
	}
	return &Event{HazardType: domain.HazardWildfire, Severity: severity(int(a.Count), a.FRP.Sum), AreaName: nearest.Name, Latitude: lat, Longitude: lon, OccurredAt: a.EarliestAt, Attributes: domain.WildfireAttributes{DetectionCount: a.Count, Satellites: satellites, MaxConfidence: a.Confidence, TotalFRPMW: a.FRP.Sum, DetectionKeys: keys}}, nil
}
func LogicalIntent(namespace string, offset int64, f Fire) Intent {
	kind := "upsert"
	event := f.Event
	if !f.Active {
		kind, event = "retract", nil
	}
	id := digest("fire-op-v1:", "["+quote(BatchIdentity(namespace, offset))+","+quote(f.ID)+","+quote(kind)+"]")
	return Intent{OperationID: id, Source: domain.SourceSatelit, SourceRefID: f.ID, Operation: kind, SourceOrder: SourceOrder{namespace + ":wildfire-output-v1", offset}, LogicalRevision: f.LogicalRevision, Event: event}
}
