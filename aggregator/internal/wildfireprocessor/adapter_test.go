package wildfireprocessor

import (
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixtureConfig(t *testing.T) Config {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "wildfire-m5", "provinces.json"))
	require.NoError(t, err)
	start, late := int64(1), int64(1800)
	return Config{Version: 1, Namespace: "fixture-satellite-session", InitialOffset: &start, AlgorithmVersion: 2, AllowedLatenessSeconds: &late, ProvinceJSON: b, ProvinceSHA256: rawinput.Hash(b), PageRows: 8, CacheBytes: 16 << 20, StagingBytes: 64 << 20, PreparedBytes: 2 << 20}
}
func TestM6NormalizedAdapterExactBytesAndDisagreement(t *testing.T) {
	raw := []byte(`{"version":1,"namespace":"fixture-satellite-session","offset":1,"kind":"heartbeat","virtual_now":"2026-11-13T00:00:00Z","payload":"AP8="}`)
	env, err := FixtureEnvelope(raw, "2026-11-13T00:00:00Z", "acquisition:SATELIT", 1, "fixture")
	require.NoError(t, err)
	op, err := DecodeOperation(env, raw, 1<<20)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 255}, op.Payload)
	require.Equal(t, raw, env.Payload)
	env.Operation = "retract"
	env.MessageID = rawinput.ID(env)
	_, err = DecodeOperation(env, raw, 1<<20)
	require.Error(t, err)
	env.Operation = "heartbeat"
	env.MessageID = rawinput.ID(env)
	env.VirtualNow = nil
	_, err = DecodeOperation(env, raw, 1<<20)
	require.Error(t, err)
	require.NotEqual(t, rawinput.Hash(raw), rawinput.Hash(append(raw, byte(' '))))
}
func TestM6ConfigurationPinsSemanticsAndBounds(t *testing.T) {
	c := fixtureConfig(t)
	_, fingerprint, err := c.validate()
	require.NoError(t, err)
	c.PageRows = 2
	_, same, err := c.validate()
	require.NoError(t, err)
	require.Equal(t, fingerprint, same)
	late := int64(3)
	c.AllowedLatenessSeconds = &late
	_, different, err := c.validate()
	require.NoError(t, err)
	require.NotEqual(t, fingerprint, different)
	c.PreparedBytes = c.CacheBytes
	_, _, err = c.validate()
	require.Error(t, err)
	c = fixtureConfig(t)
	c.AlgorithmVersion = 1
	_, _, err = c.validate()
	require.Error(t, err)
}

func TestM6NormalizedAdapterRejectsZeroClock(t *testing.T) {
	zero := time.Time{}
	b, err := json.Marshal(wildfire.Operation{Version: 1, Namespace: "fixture-satellite-session", Offset: 1, Kind: "heartbeat", VirtualNow: &zero})
	require.NoError(t, err)
	_, err = FixtureEnvelope(b, "2026-11-13T00:00:00Z", "acquisition:SATELIT", 1, "fixture")
	// RawInput may reject the invalid clock before the normalized decoder.
	require.Error(t, err)
}
func fixtureOperations(t *testing.T, name string) []wildfire.Operation {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "wildfire-m5", name+".jsonl"))
	require.NoError(t, err)
	out := []wildfire.Operation{}
	for _, line := range bytesLines(b) {
		var op wildfire.Operation
		require.NoError(t, wildfire.Decode(line, &op))
		out = append(out, op)
	}
	return out
}
func bytesLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			if i > start {
				line := b[start:i]
				if len(line) > 0 && line[len(line)-1] == '\r' {
					line = line[:len(line)-1]
				}
				out = append(out, line)
			}
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}
func envelope(t *testing.T, op wildfire.Operation) rawinput.Envelope {
	t.Helper()
	b, err := json.Marshal(op)
	require.NoError(t, err)
	e, err := FixtureEnvelope(b, "2026-11-13T00:00:00Z", "acquisition:SATELIT", 1, "original-fixture-producer")
	require.NoError(t, err)
	return e
}
