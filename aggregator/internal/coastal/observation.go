package coastal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrUnknownStation = errors.New("observation references an unknown station")

type Observation struct {
	ID         string
	StationID  string
	ObservedAt time.Time
	SeaLevel   float64
	WaveHeight float64
	Quality    string
	Critical   bool
}

type Violation struct{ Reason string }

func (v *Violation) Error() string { return "contract_violation: " + v.Reason }

func violation(format string, args ...any) error {
	return &Violation{Reason: fmt.Sprintf(format, args...)}
}

func Identify(raw []byte) (string, bool) {
	var probe struct {
		ID any `json:"observation_id"`
	}
	if json.Unmarshal(raw, &probe) != nil {
		return "", false
	}
	id, ok := probe.ID.(string)
	return id, ok && validText(id)
}

func Parse(raw []byte, virtualNow *time.Time, skew time.Duration) (Observation, error) {
	var o Observation
	if !utf8.Valid(raw) {
		return o, violation("payload is not UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var fields map[string]any
	if err := d.Decode(&fields); err != nil || d.More() {
		return o, violation("payload is not a single JSON object")
	}
	text := func(name string) (string, error) {
		v, ok := fields[name].(string)
		if !ok || !validText(v) {
			return "", violation("%s must be a non-empty string", name)
		}
		return v, nil
	}
	number := func(name string) (float64, error) {
		n, ok := fields[name].(json.Number)
		if !ok {
			return 0, violation("%s must be a number", name)
		}
		v, err := n.Float64()
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, violation("%s must be finite", name)
		}
		return v, nil
	}
	var err error
	if o.ID, err = text("observation_id"); err != nil {
		return o, err
	}
	if o.StationID, err = text("station_id"); err != nil {
		return o, err
	}
	if o.Quality, err = text("quality"); err != nil {
		return o, err
	}
	at, err := text("observed_at")
	if err != nil {
		return o, err
	}
	o.ObservedAt, err = time.Parse(time.RFC3339Nano, at)
	if _, offset := o.ObservedAt.Zone(); err != nil || offset != 0 {
		return o, violation("observed_at must be an RFC 3339 UTC timestamp")
	}
	if virtualNow != nil && o.ObservedAt.After(virtualNow.Add(skew)) {
		return o, violation("observed_at %s is later than the sensor clock %s", at, virtualNow.Format(time.RFC3339))
	}
	if o.SeaLevel, err = number("sea_level_anomaly_m"); err != nil {
		return o, err
	}
	if o.WaveHeight, err = number("wave_height_m"); err != nil {
		return o, err
	}
	if o.WaveHeight < 0 {
		return o, violation("wave_height_m cannot be negative")
	}
	critical, ok := fields["critical"].(bool)
	if !ok {
		return o, violation("critical must be a boolean")
	}
	o.Critical = critical
	return o, nil
}

func validText(s string) bool {
	return s != "" && len(s) <= 256 && strings.TrimSpace(s) == s && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
