package satellite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

var detectionFields = []string{"satellite", "acq_date", "acq_time", "latitude", "longitude", "confidence", "frp_mw"}

var aliases = map[string][]string{"frp_mw": {"frp_mw", "frp"}}

type Normalized struct {
	Offset     int64
	Kind       string
	VirtualNow *time.Time
	Operation  []byte
}

type Unreadable struct{ Reason string }

func (u *Unreadable) Error() string { return "unreadable satellite event: " + u.Reason }

func unreadable(format string, args ...any) error {
	return &Unreadable{Reason: fmt.Sprintf(format, args...)}
}

func fields(raw []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var m map[string]json.RawMessage
	if err := d.Decode(&m); err != nil || m == nil {
		return nil, unreadable("event data is not a JSON object")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, unreadable("event data is not a single JSON object")
	}
	return m, nil
}

func lexical(raw json.RawMessage) (string, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", false
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return "", false
		}
		return s, s != ""
	}
	var n json.Number
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&n) != nil {
		return "", false
	}
	return string(n), true
}

func pick(m map[string]json.RawMessage, name string) (json.RawMessage, bool) {
	names := aliases[name]
	if names == nil {
		names = []string{name}
	}
	for _, n := range names {
		if v, ok := m[n]; ok && !bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return v, true
		}
	}
	return nil, false
}

func detection(m map[string]json.RawMessage) (json.RawMessage, error) {
	if nested, ok := m["detection"]; ok {
		inner, err := fields(nested)
		if err != nil {
			return nil, unreadable("detection is not an object")
		}
		m = inner
	}
	out := map[string]json.RawMessage{}
	for _, name := range detectionFields {
		v, ok := pick(m, name)
		if !ok {
			return nil, unreadable("detection is missing %s", name)
		}
		if name != "frp_mw" && name != "confidence" {
			if _, ok := lexical(v); !ok {
				return nil, unreadable("%s must be a string or number", name)
			}
		}
		out[name] = v
	}
	var conf string
	if json.Unmarshal(out["confidence"], &conf) != nil {
		if text, ok := lexical(out["confidence"]); ok {
			out["confidence"], _ = json.Marshal(text)
		} else {
			return nil, unreadable("confidence must be text")
		}
	}
	var frp float64
	if json.Unmarshal(out["frp_mw"], &frp) != nil {
		text, ok := lexical(out["frp_mw"])
		value, err := strconv.ParseFloat(text, 64)
		if !ok || err != nil {
			return nil, unreadable("frp must be numeric")
		}
		out["frp_mw"], _ = json.Marshal(value)
	}
	return json.Marshal(out)
}

func key(m map[string]json.RawMessage) (string, bool) {
	for _, name := range []string{"target_key", "detection_key", "key"} {
		if v, ok := m[name]; ok {
			if s, ok := lexical(v); ok {
				return s, true
			}
		}
	}
	source := m
	if nested, ok := m["detection"]; ok {
		if inner, err := fields(nested); err == nil {
			source = inner
		}
	}
	parts := make([]string, 0, 5)
	for _, name := range detectionFields[:5] {
		v, ok := source[name]
		if !ok {
			return "", false
		}
		s, ok := lexical(v)
		if !ok {
			return "", false
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, "|"), true
}

func offsetOf(e Event, m map[string]json.RawMessage) (int64, error) {
	text := e.ID
	if v, ok := m["offset"]; ok {
		if s, ok := lexical(v); ok {
			if text != "" && text != s {
				return 0, unreadable("event id %q disagrees with offset %q", e.ID, s)
			}
			text = s
		}
	}
	offset, err := strconv.ParseInt(text, 10, 64)
	if err != nil || offset < 0 {
		return 0, unreadable("event has no valid offset")
	}
	return offset, nil
}

func clock(m map[string]json.RawMessage) *time.Time {
	v, ok := m["virtual_now"]
	if !ok {
		return nil
	}
	var s string
	if json.Unmarshal(v, &s) != nil {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	t = t.UTC()
	return &t
}

func Normalize(namespace string, e Event) (Normalized, error) {
	// The SSE id remains usable for quarantine even if the JSON is truncated.
	n := Normalized{Offset: -1, Kind: e.Type}
	if offset, err := offsetOf(e, nil); err == nil {
		n.Offset = offset
	}
	m, err := fields(e.Data)
	if err != nil {
		if e.Type == "heartbeat" && len(bytes.TrimSpace(e.Data)) == 0 {
			m = map[string]json.RawMessage{}
		} else {
			return n, err
		}
	}
	n.VirtualNow = clock(m)
	offset, err := offsetOf(e, m)
	if err != nil {
		return n, err
	}
	n.Offset = offset
	op := map[string]any{"version": 1, "namespace": namespace, "offset": n.Offset, "kind": n.Kind, "payload": e.Data}
	if n.VirtualNow != nil {
		op["virtual_now"] = n.VirtualNow.Format(time.RFC3339Nano)
	}
	switch n.Kind {
	case "detection", "update":
		d, err := detection(m)
		if err != nil {
			return n, err
		}
		op["detection"] = json.RawMessage(d)
		if n.Kind == "update" {
			k, ok := key(m)
			if !ok {
				return n, unreadable("update has no target key")
			}
			op["target_key"] = k
		}
	case "retract":
		k, ok := key(m)
		if !ok {
			return n, unreadable("retract has no target key")
		}
		op["target_key"] = k
	case "heartbeat":
	default:
		return n, unreadable("unknown event type %q", n.Kind)
	}
	n.Operation, err = json.Marshal(op)
	return n, err
}
