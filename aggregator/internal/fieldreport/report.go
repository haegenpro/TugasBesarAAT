package fieldreport

import (
	"aggregator/internal/canonical"
	"bytes"
	"encoding/json"
	"math"
	"mime"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	Source      = "FIELD_REPORT"
	MaxBody     = 1<<20 - 16<<10
	ContentType = "application/json"
)

type Report struct {
	Type        string          `json:"report_type"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Latitude    float64         `json:"latitude"`
	Longitude   float64         `json:"longitude"`
	Description string          `json:"description"`
	Attributes  json.RawMessage `json:"attributes"`
}

func Parse(body []byte) (Report, error) {
	var r Report
	if len(body) > MaxBody {
		return r, canonical.Fail(413, "payload_too_large", "field report exceeds the accepted size")
	}
	if !utf8.Valid(body) {
		return r, invalid("field report must be UTF-8 JSON")
	}
	var raw struct {
		Type        *string          `json:"report_type"`
		OccurredAt  *string          `json:"occurred_at"`
		Latitude    *float64         `json:"latitude"`
		Longitude   *float64         `json:"longitude"`
		Description *string          `json:"description"`
		Attributes  *json.RawMessage `json:"attributes"`
	}
	if err := canonical.Decode(bytes.NewReader(body), &raw); err != nil {
		return r, invalid("field report must be a JSON object with the documented fields")
	}
	if raw.Type == nil || strings.TrimSpace(*raw.Type) == "" || raw.Description == nil || strings.TrimSpace(*raw.Description) == "" {
		return r, invalid("report_type and description are required")
	}
	if raw.OccurredAt == nil {
		return r, invalid("occurred_at is required")
	}
	at, err := time.Parse(time.RFC3339Nano, *raw.OccurredAt)
	if _, offset := at.Zone(); err != nil || offset != 0 {
		return r, invalid("occurred_at must be an RFC 3339 UTC timestamp")
	}
	if raw.Latitude == nil || raw.Longitude == nil || !finite(*raw.Latitude) || !finite(*raw.Longitude) || math.Abs(*raw.Latitude) > 90 || math.Abs(*raw.Longitude) > 180 {
		return r, invalid("latitude and longitude must be finite coordinates")
	}
	attributes := json.RawMessage(`{}`)
	if raw.Attributes != nil {
		trimmed := bytes.TrimSpace(*raw.Attributes)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return r, invalid("attributes must be an object")
		}
		attributes = trimmed
	}
	return Report{Type: *raw.Type, OccurredAt: at.UTC(), Latitude: *raw.Latitude, Longitude: *raw.Longitude, Description: *raw.Description, Attributes: attributes}, nil
}

func ValidKey(key string) bool {
	if len(key) < 1 || len(key) > 128 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return false
		}
	}
	return true
}

func MediaType(header string) (string, bool) {
	media, params, err := mime.ParseMediaType(header)
	if err != nil || media != ContentType {
		return "", false
	}
	if charset, ok := params["charset"]; ok && !strings.EqualFold(charset, "utf-8") || len(params) > 1 || len(params) == 1 && params["charset"] == "" {
		return "", false
	}
	return media, true
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func invalid(message string) error { return canonical.Fail(400, "invalid_request", message) }
