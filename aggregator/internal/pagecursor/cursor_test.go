package pagecursor

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestSignedPrecisionScopeAndTampering(t *testing.T) {
	s := Signer{Key: []byte(strings.Repeat("s", 32))}
	p := Position{Endpoint: "audit/events", Scope: "audit", Filters: "SATELIT|since", Time: time.Date(2026, 11, 13, 0, 0, 0, 123456000, time.UTC), ID: "00000000-0000-4000-8000-000000000001", Units: []string{"g1-a", "g1-b"}}
	token, err := s.Encode(p)
	require.NoError(t, err)
	got, err := s.Decode(token, p.Endpoint, p.Scope, p.Filters)
	require.NoError(t, err)
	require.Equal(t, p.Time, got.Time)
	for _, bad := range []string{"", "e30", token + "x", "x" + token, strings.Repeat("a", 17000)} {
		_, err = s.Decode(bad, p.Endpoint, p.Scope, p.Filters)
		require.Error(t, err)
	}
	_, err = s.Decode(token, "client/hazards", p.Scope, p.Filters)
	require.Error(t, err)
	_, err = s.Decode(token, p.Endpoint, p.Scope, "changed")
	require.Error(t, err)
	wrong := Signer{Key: []byte(strings.Repeat("x", 32))}
	_, err = wrong.Decode(token, p.Endpoint, p.Scope, p.Filters)
	require.Error(t, err)
}
