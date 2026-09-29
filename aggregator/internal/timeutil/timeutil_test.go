package timeutil_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"aggregator/internal/timeutil"
)

var isoMilliPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)

func TestFormatISOMilli_MatchesRequiredShape(t *testing.T) {
	tm := time.Date(2026, 9, 20, 14, 32, 7, 123000000, time.UTC)
	got := timeutil.FormatISOMilli(tm)
	assert.Equal(t, "2026-09-20T14:32:07.123Z", got)
	assert.Regexp(t, isoMilliPattern, got)
}

func TestFormatISOMilli_ConvertsToUTC(t *testing.T) {
	loc := time.FixedZone("WIB", 7*3600)
	tm := time.Date(2026, 9, 20, 21, 32, 7, 0, loc) // 14:32:07 UTC
	got := timeutil.FormatISOMilli(tm)
	assert.Equal(t, "2026-09-20T14:32:07.000Z", got)
}
