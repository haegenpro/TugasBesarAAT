// Package rawinput implements the registered RawInput v1 transport, not a live
// satellite payload parser. Exact acquired bytes remain distinct from values.
package rawinput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
)

const Subject = "bnpb.raw-input-value"
const MaxMessage = 1 << 20

type Envelope struct {
	Version         int     `json:"envelope_version"`
	Source          string  `json:"source"`
	MessageID       string  `json:"message_id"`
	Namespace       string  `json:"source_namespace"`
	Ref             *string `json:"source_ref_id"`
	Operation       string  `json:"operation"`
	Position        *int64  `json:"source_position"`
	Cursor          *string `json:"source_cursor"`
	EventTime       *string `json:"event_time"`
	ReceivedAt      string  `json:"received_at"`
	Payload         []byte  `json:"payload"`
	PayloadRef      *string `json:"payload_ref"`
	Size            int64   `json:"payload_size"`
	Digest          string  `json:"payload_sha256"`
	ContentType     string  `json:"content_type"`
	SchemaID        *string `json:"source_schema_id"`
	VirtualNow      *string `json:"virtual_now"`
	CorrelationID   string  `json:"correlation_id"`
	Traceparent     *string `json:"traceparent"`
	OwnershipScope  string  `json:"ownership_scope"`
	OwnershipEpoch  int64   `json:"ownership_epoch"`
	OwnershipHolder string  `json:"ownership_holder"`
}

// Compact uses the UTF-8 agreement encoding, including literal non-ASCII and
// HTML characters (encoding/json's default HTML escaping is not this contract).
func Compact(values ...any) []byte {
	var b bytes.Buffer
	b.WriteByte('[')
	for i, v := range values {
		if i > 0 {
			b.WriteByte(',')
		}
		if s, ok := v.(string); ok {
			q, err := canonical.Route("x", s)
			if err != nil {
				return nil
			}
			b.WriteString(q[5 : len(q)-1])
		} else {
			var x bytes.Buffer
			enc := json.NewEncoder(&x)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(v); err != nil {
				return nil
			}
			b.Write(bytes.TrimSpace(x.Bytes()))
		}
	}
	b.WriteByte(']')
	return b.Bytes()
}
func ID(e Envelope) string {
	var p any
	if e.Position != nil {
		p = *e.Position
	}
	var ref any
	if e.Ref != nil {
		ref = *e.Ref
	}
	return Identity("raw-v1", e.Source, e.Namespace, p, ref, e.Operation)
}
func Identity(prefix string, values ...any) string {
	b := Compact(values...)
	if b == nil {
		return ""
	}
	return prefix + ":" + fmt.Sprintf("%x", sha256.Sum256(b))
}
func Hash(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }
func text(s string) bool   { return s != "" && utf8.ValidString(s) && !strings.ContainsRune(s, 0) }
func utc(s string) bool {
	t, e := time.Parse(time.RFC3339Nano, s)
	_, offset := t.Zone()
	return e == nil && offset == 0 && !t.IsZero()
}
func Validate(e Envelope) error {
	bad := func() error {
		return canonical.Fail(400, "invalid_request", "invalid RawInput v1 identity, context or byte integrity")
	}
	if e.Version != 1 || !text(e.Namespace) || e.Position == nil || *e.Position < 0 || !text(e.CorrelationID) || !utc(e.ReceivedAt) || !text(e.ContentType) || !text(e.OwnershipScope) || !text(e.OwnershipHolder) || e.OwnershipEpoch < 1 {
		return bad()
	}
	if e.Ref != nil && !text(*e.Ref) {
		return bad()
	}
	if e.EventTime != nil && !utc(*e.EventTime) || e.VirtualNow != nil && !utc(*e.VirtualNow) {
		return bad()
	}
	allowed := false
	switch e.Source {
	case "SATELIT":
		allowed = e.Operation == "detection" || e.Operation == "update" || e.Operation == "retract" || e.Operation == "heartbeat"
	case "BMKG", "PVMBG", "SENSOR_PANTAI":
		allowed = e.Operation == "create" || e.Operation == "update" || e.Operation == "retract"
	}
	if !allowed || e.MessageID != ID(e) || e.Size < 0 {
		return bad()
	}
	d, err := hex.DecodeString(e.Digest)
	if err != nil || len(d) != 32 || strings.ToLower(e.Digest) != e.Digest {
		return bad()
	}
	if e.PayloadRef != nil {
		if !Opaque(*e.PayloadRef) || len(e.Payload) != 0 {
			return bad()
		}
	} else if int64(len(e.Payload)) != e.Size || Hash(e.Payload) != e.Digest {
		return bad()
	}
	return nil
}
func Opaque(s string) bool {
	if len(s) < 1 || len(s) > 256 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune(":_-", r)) {
			return false
		}
	}
	return true
}

// Fingerprint includes original acquisition provenance, but delivery authority is
// supplied separately by the current owner and is not part of this envelope.
func Fingerprint(e Envelope) string { b, _ := json.Marshal(e); return Hash(b) }
func Route(e Envelope) string       { return string(Compact(e.Source, e.Namespace, *e.Position)) }

func Encode(schema *avrowire.Schema, e Envelope) ([]byte, error) {
	if err := Validate(e); err != nil {
		return nil, err
	}
	b, _ := json.Marshal(e)
	var m map[string]any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	_ = d.Decode(&m)
	m["envelope_version"] = int32(e.Version)
	m["ownership_epoch"] = e.OwnershipEpoch
	m["payload_size"] = e.Size
	m["payload"] = e.Payload
	for _, key := range []string{"source_ref_id", "source_cursor", "event_time", "payload_ref", "source_schema_id", "virtual_now", "traceparent"} {
		if m[key] != nil {
			m[key] = map[string]any{"string": m[key]}
		}
	}
	if e.Position != nil {
		m["source_position"] = map[string]any{"long": *e.Position}
	}
	return avrowire.Encode(schema, m)
}
func Decode(ctx context.Context, c *avrowire.Client, reader *avrowire.Reader, b []byte) (Envelope, error) {
	var e Envelope
	if len(b) > MaxMessage {
		return e, canonical.Fail(413, "payload_too_large", "raw frame exceeds 1 MiB")
	}
	v, err := c.DecodeWithReader(ctx, b, reader)
	if err != nil {
		return e, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return e, fmt.Errorf("RawInput record required")
	}
	payload, ok := m["payload"].([]byte)
	if !ok {
		return e, fmt.Errorf("Avro bytes required")
	}
	copy := make(map[string]any, len(m))
	for k, v := range m {
		copy[k] = v
	}
	copy["payload"] = payload
	j, err := json.Marshal(copy)
	if err != nil {
		return e, err
	}
	err = json.Unmarshal(j, &e)
	if err != nil {
		return e, err
	}
	return e, Validate(e)
}

type Resolver interface {
	Open(context.Context, string) (io.ReadCloser, error)
}

func Resolve(ctx context.Context, e Envelope, resolver Resolver, quota int64) ([]byte, error) {
	if err := Validate(e); err != nil {
		return nil, err
	}
	if e.PayloadRef == nil {
		return append([]byte(nil), e.Payload...), nil
	}
	if resolver == nil || quota <= 0 || e.Size > quota {
		return nil, canonical.Unavailable()
	}
	r, err := resolver.Open(ctx, *e.PayloadRef)
	if err != nil {
		return nil, canonical.Unavailable()
	}
	defer r.Close()
	stop := context.AfterFunc(ctx, func() { _ = r.Close() })
	defer stop()
	// A single input is bounded by the configured quota; blob storage itself uses
	// 64 KiB chunks. No caller path or URL is interpreted here.
	b, err := io.ReadAll(io.LimitReader(r, e.Size+1))
	if err != nil || int64(len(b)) != e.Size || Hash(b) != e.Digest {
		return nil, canonical.Unavailable()
	}
	return b, nil
}

// CaptureCursor is the versioned BMKG whole-response adapter context. Original
// archive positions are retained, distinct from the acquisition position.
type CaptureCursor struct {
	Version        int       `json:"version"`
	Entity         string    `json:"entity"`
	History        bool      `json:"history"`
	After          int64     `json:"after"`
	Next           int64     `json:"next"`
	HasMore        bool      `json:"has_more"`
	SourceSequence int64     `json:"source_sequence"`
	RequestStarted time.Time `json:"request_started"`
	Full           bool      `json:"full"`
}

func (c CaptureCursor) Encode() string { b, _ := json.Marshal(c); return string(b) }
func ParseCursor(s *string) (CaptureCursor, error) {
	var c CaptureCursor
	if s == nil {
		return c, fmt.Errorf("capture cursor required")
	}
	err := canonical.Decode(strings.NewReader(*s), &c)
	if err != nil || c.Version != 1 || (c.Entity != "seismic-events" && c.Entity != "tsunami-warnings") || c.After < 0 || c.Next < c.After || c.SourceSequence < 0 || c.RequestStarted.IsZero() {
		return c, fmt.Errorf("invalid BMKG capture cursor")
	}
	return c, nil
}
func SchemaNumber(b []byte) *string {
	if len(b) < 5 || b[0] != 0 {
		return nil
	}
	v := int64(b[1])<<24 | int64(b[2])<<16 | int64(b[3])<<8 | int64(b[4])
	s := strconv.FormatInt(v, 10)
	return &s
}
