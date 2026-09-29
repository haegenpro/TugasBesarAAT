// Package timeutil centralizes the wire-format timestamp rules shared across
// this service: ISO 8601, UTC, millisecond precision, "Z" suffix.
package timeutil

import "time"

const ISOMilli = "2006-01-02T15:04:05.000Z07:00"

// FormatISOMilli renders t as UTC, millisecond-precision ISO 8601 with a Z suffix.
func FormatISOMilli(t time.Time) string {
	return t.UTC().Format(ISOMilli)
}

// ParseISO parses a timestamp in RFC3339 (with or without fractional seconds).
func ParseISO(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	return time.Parse(time.RFC3339, s)
}

// Time wraps time.Time to marshal per the wire format rules.
type Time struct {
	time.Time
}

func New(t time.Time) Time { return Time{t.UTC()} }

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(`"` + FormatISOMilli(t.Time) + `"`), nil
}

func (t *Time) UnmarshalJSON(data []byte) error {
	s := string(data)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	parsed, err := ParseISO(s)
	if err != nil {
		return err
	}
	t.Time = parsed
	return nil
}
