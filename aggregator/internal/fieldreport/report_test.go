package fieldreport

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const validReport = `{"report_type":"field_observation","occurred_at":"2026-11-16T03:00:00Z","latitude":-6.9,"longitude":107.6,"description":"Banjir setinggi lutut","attributes":{"photos":2}}`

func TestParseAcceptsTheDocumentedShape(t *testing.T) {
	r, err := Parse([]byte(validReport))
	require.NoError(t, err)
	require.Equal(t, "field_observation", r.Type)
	require.Equal(t, -6.9, r.Latitude)
	require.JSONEq(t, `{"photos":2}`, string(r.Attributes))
	r, err = Parse([]byte(`{"report_type":"x","occurred_at":"2026-11-16T03:00:00.5Z","latitude":0,"longitude":0,"description":"d"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(r.Attributes))
}

func TestParseRejectsInvalidReports(t *testing.T) {
	cases := map[string]string{
		"not json":          `{"report_type":`,
		"unknown field":     `{"report_type":"x","occurred_at":"2026-11-16T03:00:00Z","latitude":0,"longitude":0,"description":"d","priority":1}`,
		"missing type":      `{"occurred_at":"2026-11-16T03:00:00Z","latitude":0,"longitude":0,"description":"d"}`,
		"blank description": `{"report_type":"x","occurred_at":"2026-11-16T03:00:00Z","latitude":0,"longitude":0,"description":"  "}`,
		"local time":        `{"report_type":"x","occurred_at":"2026-11-16T10:00:00+07:00","latitude":0,"longitude":0,"description":"d"}`,
		"latitude range":    `{"report_type":"x","occurred_at":"2026-11-16T03:00:00Z","latitude":91,"longitude":0,"description":"d"}`,
		"missing longitude": `{"report_type":"x","occurred_at":"2026-11-16T03:00:00Z","latitude":0,"description":"d"}`,
		"array attributes":  `{"report_type":"x","occurred_at":"2026-11-16T03:00:00Z","latitude":0,"longitude":0,"description":"d","attributes":[1]}`,
		"deep nesting":      `{"report_type":"x","occurred_at":"2026-11-16T03:00:00Z","latitude":0,"longitude":0,"description":"d","attributes":` + strings.Repeat(`{"a":`, 80) + `1` + strings.Repeat(`}`, 80) + `}`,
		"invalid utf8":      "{\"report_type\":\"\xff\",\"occurred_at\":\"2026-11-16T03:00:00Z\",\"latitude\":0,\"longitude\":0,\"description\":\"d\"}",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(body))
			require.ErrorContains(t, err, "invalid_request")
		})
	}
	_, err := Parse([]byte(`{"description":"` + strings.Repeat("x", MaxBody) + `"}`))
	require.ErrorContains(t, err, "payload_too_large")
}

func TestKeyAndMediaTypeRules(t *testing.T) {
	require.True(t, ValidKey("report-fixture-1"))
	require.False(t, ValidKey(""))
	require.False(t, ValidKey("has space"))
	require.False(t, ValidKey(strings.Repeat("k", 129)))
	for _, header := range []string{"application/json", "application/json; charset=utf-8", "Application/JSON"} {
		media, ok := MediaType(header)
		require.True(t, ok, header)
		require.Equal(t, ContentType, media)
	}
	for _, header := range []string{"", "text/plain", "application/json; charset=latin1", "application/json; boundary=x"} {
		_, ok := MediaType(header)
		require.False(t, ok, header)
	}
}
