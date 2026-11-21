package wildfire

import (
	"aggregator/internal/domain"
	"fmt"
	"github.com/pymaxion/geographiclib-go/v2/geodesic"
	"math"
	"sort"
)

type kahan struct{ sum, correction float64 }

func (s *kahan) add(v float64) {
	y := v - s.correction
	t := s.sum + y
	s.correction = (t - s.sum) - y
	s.sum = t
}
func center(keys []string, obs map[string]Observation) (float64, float64, error) {
	var x, y, z kahan
	for _, k := range keys {
		o := obs[k]
		lat, lon := o.Latitude*math.Pi/180, o.Longitude*math.Pi/180
		x.add(math.Cos(lat) * math.Cos(lon))
		y.add(math.Cos(lat) * math.Sin(lon))
		z.add(math.Sin(lat))
	}
	norm := math.Hypot(math.Hypot(x.sum, y.sum), z.sum)
	if norm/float64(len(keys)) <= 1e-12 {
		return 0, 0, fmt.Errorf("undefined_center")
	}
	lat := math.Atan2(z.sum, math.Hypot(x.sum, y.sum)) * 180 / math.Pi
	lon := math.Atan2(y.sum, x.sum) * 180 / math.Pi
	if lat == 0 {
		lat = 0
	}
	if lon == 0 {
		lon = 0
	}
	return lat, lon, nil
}
func severity(count int, frp float64) domain.Severity {
	if count >= 20 || frp >= 100 {
		return domain.SeverityAwas
	}
	if count >= 6 || frp >= 30 {
		return domain.SeveritySiaga
	}
	return domain.SeverityWaspada
}
func mapEvent(keys []string, obs map[string]Observation, provinces []Province) (*Event, error) {
	lat, lon, err := center(keys, obs)
	if err != nil {
		return nil, err
	}
	var sum kahan
	satellites := map[string]bool{}
	confidence := "n"
	for _, key := range keys {
		o := obs[key]
		sum.add(o.Detection.FRPMW)
		satellites[o.Detection.Satellite] = true
		if o.Detection.Confidence == "h" {
			confidence = "h"
		}
	}
	if !finite(sum.sum) {
		return nil, fmt.Errorf("numeric_overflow")
	}
	names := []string{}
	for s := range satellites {
		names = append(names, s)
	}
	sort.Strings(names)
	nearest := provinces[0]
	best := geodesic.WGS84.Inverse(lat, lon, nearest.Latitude, nearest.Longitude).S12
	for _, p := range provinces[1:] {
		d := geodesic.WGS84.Inverse(lat, lon, p.Latitude, p.Longitude).S12
		if d < best || (d == best && (p.Name < nearest.Name || (p.Name == nearest.Name && p.ID < nearest.ID))) {
			best = d
			nearest = p
		}
	}
	return &Event{HazardType: domain.HazardWildfire, Severity: severity(len(keys), sum.sum), AreaName: nearest.Name, Latitude: lat, Longitude: lon, OccurredAt: *obs[earliest(keys, obs)].EventTime, Attributes: domain.WildfireAttributes{DetectionCount: int64(len(keys)), Satellites: names, MaxConfidence: confidence, TotalFRPMW: sum.sum, DetectionKeys: append([]string(nil), keys...)}}, nil
}
