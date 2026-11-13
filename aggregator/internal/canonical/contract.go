// Package canonical defines the private Canonical Adapter v1 contract.
package canonical

import (
	"aggregator/internal/domain"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const Deadline = 2 * time.Second
const InlineLimit int64 = 1 << 20

type Ownership struct {
	Scope  string `json:"scope"`
	Epoch  int64  `json:"epoch"`
	Holder string `json:"holder"`
}
type SourceOrder struct {
	Namespace string `json:"namespace"`
	Position  int64  `json:"position"`
}
type MutableEvent struct {
	HazardType domain.HazardType `json:"hazard_type"`
	Severity   domain.Severity   `json:"severity"`
	AreaName   string            `json:"area_name"`
	Latitude   float64           `json:"latitude"`
	Longitude  float64           `json:"longitude"`
	OccurredAt time.Time         `json:"occurred_at"`
	Attributes map[string]any    `json:"attributes"`
}
type Mutation struct {
	OperationID      string        `json:"operation_id"`
	Source           domain.Source `json:"source"`
	SourceRefID      string        `json:"source_ref_id"`
	Operation        string        `json:"operation"`
	SourceOrder      SourceOrder   `json:"source_order"`
	ExpectedRevision int64         `json:"expected_revision"`
	Event            *MutableEvent `json:"event,omitempty"`
}
type PreparedReference struct {
	ID     string `json:"id"`
	Count  int64  `json:"count"`
	SHA256 string `json:"sha256"`
}
type Batch struct {
	ContractVersion int                `json:"contract_version"`
	BatchID         string             `json:"batch_id"`
	Ownership       Ownership          `json:"ownership"`
	Mutations       []Mutation         `json:"mutations,omitempty"`
	MutationsRef    *PreparedReference `json:"mutations_ref,omitempty"`
}
type Item struct {
	Source      domain.Source `json:"source"`
	SourceRefID string        `json:"source_ref_id"`
	HazardID    string        `json:"hazard_id"`
	Revision    int64         `json:"revision"`
	Outcome     string        `json:"outcome"`
}
type Result struct {
	BatchID string `json:"batch_id"`
	Outcome string `json:"outcome"`
	Items   []Item `json:"items"`
}
type Entity struct {
	Source      domain.Source       `json:"source"`
	SourceRefID string              `json:"source_ref_id"`
	HazardID    string              `json:"hazard_id"`
	Revision    int64               `json:"revision"`
	Active      bool                `json:"active"`
	SourceOrder *SourceOrder        `json:"source_order"`
	Event       *domain.HazardEvent `json:"event"`
}
type Error struct {
	Status  int
	Code    string
	Message string
	Details any
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }
func Fail(status int, code, message string) *Error {
	return &Error{Status: status, Code: code, Message: message}
}
func Unavailable() *Error {
	return Fail(503, "temporarily_unavailable", "canonical storage, authority, durability or admission unavailable")
}
func validText(s string) bool { return s != "" && utf8.ValidString(s) && !strings.ContainsRune(s, 0) }
func Validate(b Batch) error {
	if b.ContractVersion != 1 || !validText(b.BatchID) || !validText(b.Ownership.Scope) || b.Ownership.Epoch <= 0 || !validText(b.Ownership.Holder) {
		return Fail(400, "invalid_request", "invalid version, batch identity or ownership")
	}
	if b.MutationsRef != nil || len(b.Mutations) == 0 {
		return Fail(400, "invalid_request", "exactly one nonempty resolved mutation array required")
	}
	seen := map[string]bool{}
	for _, m := range b.Mutations {
		key, err := Route(string(m.Source), m.SourceRefID)
		if err != nil || !domain.ValidSource(string(m.Source)) || !validText(m.SourceRefID) || !validText(m.OperationID) || !validText(m.SourceOrder.Namespace) || m.SourceOrder.Position < 0 || m.ExpectedRevision < 0 || seen[key] {
			return Fail(400, "invalid_request", "invalid mutation identity, ordering, revision or duplicate entity")
		}
		seen[key] = true
		if m.Operation == "retract" {
			if m.Event != nil {
				return Fail(400, "invalid_request", "retract must omit event")
			}
			continue
		}
		e := m.Event
		if m.Operation != "upsert" || e == nil || e.AreaName == "" || e.OccurredAt.IsZero() || !domain.ValidHazardType(string(e.HazardType)) || e.HazardType == "" || !domain.ValidSeverity(string(e.Severity)) || e.Severity == "" || e.Attributes == nil || math.IsNaN(e.Latitude) || math.IsNaN(e.Longitude) || math.IsInf(e.Latitude, 0) || math.IsInf(e.Longitude, 0) || e.Latitude < -90 || e.Latitude > 90 || e.Longitude < -180 || e.Longitude > 180 {
			return Fail(400, "invalid_request", "invalid upsert event")
		}
		if e.HazardType == domain.HazardWildfire {
			if err := domain.ValidateWildfire(e.Attributes); err != nil {
				return Fail(400, "invalid_request", err.Error())
			}
		}
		if _, err := json.Marshal(e); err != nil {
			return Fail(400, "invalid_request", "attributes must be JSON values")
		}
	}
	return nil
}

// Route deliberately avoids Go's HTML/U+2028 escaping; this is UTF-8 compact
// JSON with ordinary JSON escapes, matching PostgreSQL to_json(text).
func Route(source, ref string) (string, error) {
	if !validText(source) || !validText(ref) {
		return "", fmt.Errorf("invalid UTF-8 identity")
	}
	return "[" + quote(source) + "," + quote(ref) + "]", nil
}
func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 32 {
				b.WriteString(fmt.Sprintf(`\u%04x`, r))
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

// Fingerprints exclude delivery ownership/trace metadata. time.Time preserves
// database timestamp precision; maps sort deterministically without float coercion.
func Fingerprint(v any) (string, error) {
	normalizeTime := func(m Mutation) Mutation {
		if m.Event != nil {
			e := *m.Event
			e.OccurredAt = e.OccurredAt.UTC()
			m.Event = &e
		}
		return m
	}
	switch value := v.(type) {
	case Mutation:
		v = normalizeTime(value)
	case []Mutation:
		copy := make([]Mutation, len(value))
		for i, m := range value {
			copy[i] = normalizeTime(m)
		}
		v = copy
	}

	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var value any
	if err = d.Decode(&value); err != nil {
		return "", err
	}
	value, err = normalizeNumbers(value)
	if err != nil {
		return "", err
	}
	b, err = json.Marshal(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(b)), nil
}

// Decode rejects duplicate JSON keys as well as unknown typed fields/trailing input.
func Decode(r io.Reader, v any) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if !utf8.Valid(b) {
		return fmt.Errorf("invalid UTF-8")
	}
	if err = uniqueValue(d); err != nil {
		return err
	}
	if _, err = d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	d.DisallowUnknownFields()
	return d.Decode(v)
}
func uniqueValue(d *json.Decoder) error {
	return uniqueAt(d, 0)
}
func uniqueAt(d *json.Decoder, depth int) error {
	if depth > 64 {
		return fmt.Errorf("JSON depth exceeds 64")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			t, e := d.Token()
			if e != nil {
				return e
			}
			k, ok := t.(string)
			if !ok || seen[k] {
				return fmt.Errorf("duplicate JSON key")
			}
			seen[k] = true
			if e = uniqueAt(d, depth+1); e != nil {
				return e
			}
		}
	case '[':
		for d.More() {
			if e := uniqueAt(d, depth+1); e != nil {
				return e
			}
		}
	default:
		return fmt.Errorf("unexpected delimiter")
	}
	_, err = d.Token()
	return err
}
func PositionString(p int64) string { return strconv.FormatInt(p, 10) }

func normalizeNumbers(v any) (any, error) {
	switch n := v.(type) {
	case json.Number:
		s := strings.ToLower(string(n))
		negative := strings.HasPrefix(s, "-")
		s = strings.TrimPrefix(s, "-")
		parts := strings.Split(s, "e")
		exponent := int64(0)
		if len(parts) == 2 {
			var err error
			exponent, err = strconv.ParseInt(parts[1], 10, 32)
			if err != nil {
				return nil, err
			}
		}
		s = parts[0]
		if dot := strings.IndexByte(s, '.'); dot >= 0 {
			exponent -= int64(len(s) - dot - 1)
			s = strings.ReplaceAll(s, ".", "")
		}
		s = strings.TrimLeft(s, "0")
		if s == "" {
			return json.Number("0"), nil
		}
		before := len(s)
		s = strings.TrimRight(s, "0")
		exponent += int64(before - len(s))
		if negative {
			s = "-" + s
		}
		return json.Number(s + "e" + strconv.FormatInt(exponent, 10)), nil
	case map[string]any:
		for k, value := range n {
			x, err := normalizeNumbers(value)
			if err != nil {
				return nil, err
			}
			n[k] = x
		}
		return n, nil
	case []any:
		for i, value := range n {
			x, err := normalizeNumbers(value)
			if err != nil {
				return nil, err
			}
			n[i] = x
		}
		return n, nil
	}
	return v, nil
}
func requiredObject(raw []byte, keys ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, key := range keys {
		v, ok := fields[key]
		if !ok || string(v) == "null" {
			return fmt.Errorf("required field %s", key)
		}
	}
	return nil
}
func (m *Mutation) UnmarshalJSON(raw []byte) error {
	if err := requiredObject(raw, "operation_id", "source", "source_ref_id", "operation", "source_order", "expected_revision"); err != nil {
		return err
	}
	type plain Mutation
	var p plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return err
	}
	if p.Event != nil {
		e := *p.Event
		e.OccurredAt = e.OccurredAt.UTC()
		p.Event = &e
	}
	*m = Mutation(p)
	return nil
}
func (o *SourceOrder) UnmarshalJSON(raw []byte) error {
	if err := requiredObject(raw, "namespace", "position"); err != nil {
		return err
	}
	type plain SourceOrder
	var p plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return err
	}
	*o = SourceOrder(p)
	return nil
}
func (e *MutableEvent) UnmarshalJSON(raw []byte) error {
	if err := requiredObject(raw, "hazard_type", "severity", "area_name", "latitude", "longitude", "occurred_at", "attributes"); err != nil {
		return err
	}
	type plain MutableEvent
	var p plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return err
	}
	*e = MutableEvent(p)
	return nil
}
func (b *Batch) UnmarshalJSON(raw []byte) error {
	if err := requiredObject(raw, "contract_version", "batch_id", "ownership"); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	_, inline := fields["mutations"]
	_, reference := fields["mutations_ref"]
	if inline == reference {
		return fmt.Errorf("exactly one mutation form required")
	}
	type plain Batch
	var p plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return err
	}
	*b = Batch(p)
	return nil
}
