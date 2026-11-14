// Package wildfire is a deterministic, single-caller, fixture-sized M5 oracle.
// It performs no I/O, assigns no canonical UUIDs and establishes no durability.
package wildfire

import (
	"aggregator/internal/domain"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const Version = 1
const Algorithm = "wildfire-reference-v1"

type Province struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}
type Config struct {
	Version                int             `json:"version"`
	AlgorithmVersion       int             `json:"algorithm_version,omitempty"`
	Namespace              string          `json:"namespace"`
	InitialOffset          *int64          `json:"initial_offset"`
	AllowedLatenessSeconds *int64          `json:"allowed_lateness_seconds"`
	ProvinceSHA256         string          `json:"province_sha256"`
	ProvinceJSON           json.RawMessage `json:"-"`
}
type Detection struct {
	Satellite  string  `json:"satellite"`
	AcqDate    string  `json:"acq_date"`
	AcqTime    string  `json:"acq_time"`
	Latitude   string  `json:"latitude"`
	Longitude  string  `json:"longitude"`
	Confidence string  `json:"confidence"`
	FRPMW      float64 `json:"frp_mw"`
}

// Lexical key components may be strings or JSON numbers. Never parse a number
// through float64 before deriving its identity. Payload preserves original bytes.
func (d *Detection) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	keys := []string{"satellite", "acq_date", "acq_time", "latitude", "longitude", "confidence", "frp_mw"}
	if bytes.Equal(fields["frp_mw"], []byte("null")) {
		return fmt.Errorf("frp_mw is required")
	}
	if len(fields) != len(keys) {
		return fmt.Errorf("detection requires exactly its seven fields")
	}
	text := func(key string) (string, error) {
		r, ok := fields[key]
		if !ok || bytes.Equal(r, []byte("null")) {
			return "", fmt.Errorf("missing %s", key)
		}
		var s string
		if len(r) > 0 && r[0] == '"' {
			if err := json.Unmarshal(r, &s); err != nil {
				return "", err
			}
			return s, nil
		}
		decoder := json.NewDecoder(bytes.NewReader(r))
		decoder.UseNumber()
		var v any
		if err := decoder.Decode(&v); err != nil {
			return "", err
		}
		n, ok := v.(json.Number)
		if !ok {
			return "", fmt.Errorf("%s must be string or number", key)
		}
		return string(n), nil
	}
	dest := []*string{&d.Satellite, &d.AcqDate, &d.AcqTime, &d.Latitude, &d.Longitude}
	for i, key := range keys[:5] {
		s, err := text(key)
		if err != nil {
			return err
		}
		*dest[i] = s
	}
	if err := json.Unmarshal(fields["confidence"], &d.Confidence); err != nil {
		return err
	}
	return json.Unmarshal(fields["frp_mw"], &d.FRPMW)
}

type Operation struct {
	Version    int        `json:"version"`
	Namespace  string     `json:"namespace"`
	Offset     int64      `json:"offset"`
	Kind       string     `json:"kind"`
	TargetKey  string     `json:"target_key,omitempty"`
	Detection  *Detection `json:"detection,omitempty"`
	VirtualNow *time.Time `json:"virtual_now,omitempty"`
	Payload    []byte     `json:"payload"`
}
type Observation struct {
	Key        string     `json:"key"`
	Tuple      [5]string  `json:"tuple"`
	Detection  *Detection `json:"detection"`
	Latitude   float64    `json:"latitude"`
	Longitude  float64    `json:"longitude"`
	EventTime  *time.Time `json:"event_time"`
	Active     bool       `json:"active"`
	LastOffset int64      `json:"last_offset"`
	Payload    []byte     `json:"payload"`
}
type Derivation struct {
	Namespace   string `json:"namespace"`
	EarliestKey string `json:"earliest_key"`
	BirthOffset int64  `json:"birth_offset"`
}
type Event struct {
	HazardType domain.HazardType         `json:"hazard_type"`
	Severity   domain.Severity           `json:"severity"`
	AreaName   string                    `json:"area_name"`
	Latitude   float64                   `json:"latitude"`
	Longitude  float64                   `json:"longitude"`
	OccurredAt time.Time                 `json:"occurred_at"`
	Attributes domain.WildfireAttributes `json:"attributes"`
}
type Fire struct {
	Source          domain.Source `json:"source"`
	ID              string        `json:"source_ref_id"`
	Derivation      Derivation    `json:"derivation"`
	Members         []string      `json:"members"`
	Active          bool          `json:"active"`
	LogicalRevision int64         `json:"logical_revision"`
	DeactivatedAt   *int64        `json:"deactivated_at"`
	Related         []string      `json:"related"`
	Event           *Event        `json:"event"`
}
type Diagnostic struct {
	Code         string   `json:"code"`
	Keys         []string `json:"keys"`
	Predecessors []string `json:"predecessors"`
}
type SourceOrder struct {
	Namespace string `json:"namespace"`
	Position  int64  `json:"position"`
}
type Intent struct {
	OperationID     string        `json:"operation_id"`
	Source          domain.Source `json:"source"`
	SourceRefID     string        `json:"source_ref_id"`
	Operation       string        `json:"operation"`
	SourceOrder     SourceOrder   `json:"source_order"`
	LogicalRevision int64         `json:"logical_revision"`
	Event           *Event        `json:"event,omitempty"`
}
type Batch struct {
	ID      string   `json:"batch_id"`
	Intents []Intent `json:"intents"`
}
type Snapshot struct {
	Version        int           `json:"version"`
	Algorithm      string        `json:"algorithm"`
	Namespace      string        `json:"namespace"`
	ProvinceSHA256 string        `json:"province_sha256"`
	Frontier       *int64        `json:"frontier"`
	Watermark      *time.Time    `json:"watermark"`
	VirtualNow     *time.Time    `json:"virtual_now"`
	LateMutations  uint64        `json:"late_mutations"`
	Singletons     int           `json:"singletons"`
	Excluded       int           `json:"excluded"`
	Observations   []Observation `json:"observations"`
	Fires          []Fire        `json:"fires"`
	Diagnostics    []Diagnostic  `json:"diagnostics"`
	History        []Operation   `json:"history"`
}
type Result struct {
	Version           int        `json:"version"`
	Offset            int64      `json:"offset"`
	Outcome           string     `json:"outcome"`
	Late              bool       `json:"late"`
	PreviousWatermark *time.Time `json:"previous_watermark"`
	Batch             *Batch     `json:"batch"`
	State             Snapshot   `json:"state"`
}
type Error struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	ExpectedOffset int64  `json:"expected_offset"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }
func clone[T any](v T) T {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out T
	if err = json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return out
}

// Decode is used only for fixture/config JSON, never to infer a live feed format.
func Decode(raw []byte, v any) error {
	if err := validJSON(raw); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

func (o *Operation) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, key := range []string{"version", "namespace", "offset", "kind", "payload"} {
		v, ok := fields[key]
		if !ok || (key != "payload" && bytes.Equal(v, []byte("null"))) {
			return fmt.Errorf("required operation field %s", key)
		}
	}
	type plain Operation
	var p plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return err
	}
	*o = Operation(p)
	return nil
}

func (p *Province) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, key := range []string{"id", "name", "latitude", "longitude"} {
		v, ok := fields[key]
		if !ok || bytes.Equal(v, []byte("null")) {
			return fmt.Errorf("required province field %s", key)
		}
	}
	type plain Province
	var out plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&out); err != nil {
		return err
	}
	*p = Province(out)
	return nil
}
