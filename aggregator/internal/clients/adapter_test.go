package clients

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"aggregator/internal/avrowire"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

const (
	renamedWriter     = `{"type":"array","items":{"type":"record","name":"VolcanicReport","namespace":"id.go.pvmbg","fields":[{"name":"report_id","type":"string"},{"name":"volcano_id","type":"string"},{"name":"level","type":"string"},{"name":"eruption_count_24h","type":"int"},{"name":"ash_column_height_m","type":"double"},{"name":"reported_at","type":"string"}]}}`
	retypedWriter     = `{"type":"array","items":{"type":"record","name":"VolcanicReport","namespace":"id.go.pvmbg","fields":[{"name":"report_id","type":"string"},{"name":"volcano_id","type":"string"},{"name":"alert_level","type":"string"},{"name":"eruption_count_24h","type":"string"},{"name":"ash_column_height_m","type":"double"},{"name":"reported_at","type":"string"}]}}`
	missingAshWriter  = `{"type":"array","items":{"type":"record","name":"VolcanicReport","namespace":"id.go.pvmbg","fields":[{"name":"report_id","type":"string"},{"name":"volcano_id","type":"string"},{"name":"alert_level","type":"string"},{"name":"eruption_count_24h","type":"int"},{"name":"reported_at","type":"string"}]}}`
	adapterSchemaPath = "../../schemas/volcanic-adapter-v2.avsc"
)

func withAdapter(t *testing.T, version int) {
	t.Helper()
	previous := VolcanicAdapter()
	require.NoError(t, SetVolcanicAdapter(version))
	t.Cleanup(func() { _ = SetVolcanicAdapter(previous) })
}

func incompatibleModes() map[string]struct {
	writer string
	record map[string]any
} {
	base := func() map[string]any {
		return map[string]any{"report_id": "rep-1", "volcano_id": "merapi", "reported_at": "2026-10-10T00:00:00Z"}
	}
	renamed := base()
	renamed["level"], renamed["eruption_count_24h"], renamed["ash_column_height_m"] = "Siaga", int32(2), 800.0
	retyped := base()
	retyped["alert_level"], retyped["eruption_count_24h"], retyped["ash_column_height_m"] = "Siaga", "2", 800.0
	missing := base()
	missing["alert_level"], missing["eruption_count_24h"] = "Siaga", int32(2)
	return map[string]struct {
		writer string
		record map[string]any
	}{
		"rename":           {renamedWriter, renamed},
		"type_change":      {retypedWriter, retyped},
		"missing_required": {missingAshWriter, missing},
	}
}

func fetchWith(t *testing.T, writer string, record map[string]any, readerText string, failures *[]Failure) ([]VolcanicReport, error) {
	t.Helper()
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": writer})
	}))
	t.Cleanup(registry.Close)
	codec, err := goavro.NewCodec(writer)
	require.NoError(t, err)
	body, err := codec.BinaryFromNative(nil, []any{record})
	require.NoError(t, err)
	frame := make([]byte, 5, 5+len(body))
	binary.BigEndian.PutUint32(frame[1:], 7)
	frame = append(frame, body...)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", avrowire.MediaType)
		_, _ = w.Write(frame)
	}))
	t.Cleanup(source.Close)
	client := NewPVMBGClient(source.URL, "token", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.Registry = avrowire.NewClient(registry.URL)
	client.Reader, err = avrowire.NewSourceReader(readerText, "VolcanicHistoryPage")
	require.NoError(t, err)
	client.CaptureFailure = func(_ context.Context, f Failure) error {
		*failures = append(*failures, f)
		return nil
	}
	return client.FetchVolcanicReports(context.Background(), "test", time.Time{})
}

func TestIncompatibleWriterIsParkedWithBaselineReader(t *testing.T) {
	withAdapter(t, VolcanicAdapterV1)
	baseline, err := os.ReadFile("../../schemas/volcanic-v1.avsc")
	require.NoError(t, err)
	for name, mode := range incompatibleModes() {
		t.Run(name, func(t *testing.T) {
			var failures []Failure
			_, err := fetchWith(t, mode.writer, mode.record, string(baseline), &failures)
			require.Error(t, err)
			require.True(t, errors.Is(err, avrowire.ErrIncompatible))
			require.Len(t, failures, 1)
			require.False(t, failures[0].Retryable)
			require.NotEmpty(t, failures[0].Payload)
		})
	}
}

func TestAdapterV2ReadsEveryIncompatibleMode(t *testing.T) {
	withAdapter(t, VolcanicAdapterV2)
	adapter, err := os.ReadFile(adapterSchemaPath)
	require.NoError(t, err)
	for name, mode := range incompatibleModes() {
		t.Run(name, func(t *testing.T) {
			var failures []Failure
			reports, err := fetchWith(t, mode.writer, mode.record, string(adapter), &failures)
			require.NoError(t, err)
			require.Empty(t, failures)
			require.Len(t, reports, 1)
			require.Equal(t, "Siaga", reports[0].AlertLevel)
			require.Equal(t, 2, reports[0].EruptionCount24h)
			require.NotContains(t, reports[0].Attributes, "level")
			if name == "missing_required" {
				require.Nil(t, reports[0].AshColumnHeightM)
			} else {
				require.Equal(t, 800.0, *reports[0].AshColumnHeightM)
			}
		})
	}
}

func TestAdapterV2KeepsBaselineWriterReadable(t *testing.T) {
	withAdapter(t, VolcanicAdapterV2)
	adapter, err := os.ReadFile(adapterSchemaPath)
	require.NoError(t, err)
	baseline, err := os.ReadFile("../../schemas/volcanic-v1.avsc")
	require.NoError(t, err)
	record := map[string]any{"report_id": "rep-2", "volcano_id": "merapi", "alert_level": "Normal", "eruption_count_24h": int32(0), "ash_column_height_m": 0.0, "reported_at": "2026-10-10T00:00:00Z"}
	var failures []Failure
	reports, err := fetchWith(t, string(baseline), record, string(adapter), &failures)
	require.NoError(t, err)
	require.Len(t, reports, 1)
	require.Equal(t, 0.0, *reports[0].AshColumnHeightM)
}

func TestAdapterOnlyRelaxesDeclaredChanges(t *testing.T) {
	cases := map[string]map[string]any{
		"renamed level": {"report_id": "r", "volcano_id": "merapi", "level": "Awas", "eruption_count_24h": float64(1), "ash_column_height_m": float64(1), "reported_at": "2026-09-01T00:00:00Z"},
		"text count":    {"report_id": "r", "volcano_id": "merapi", "alert_level": "Awas", "eruption_count_24h": "4", "ash_column_height_m": float64(1), "reported_at": "2026-09-01T00:00:00Z"},
		"missing ash":   {"report_id": "r", "volcano_id": "merapi", "alert_level": "Awas", "eruption_count_24h": float64(1), "reported_at": "2026-09-01T00:00:00Z"},
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			withAdapter(t, VolcanicAdapterV1)
			_, err := decodeVolcanicReport(raw)
			require.ErrorIs(t, err, ErrContractViolation)
			withAdapter(t, VolcanicAdapterV2)
			_, err = decodeVolcanicReport(raw)
			require.NoError(t, err)
		})
	}
	withAdapter(t, VolcanicAdapterV2)
	for _, count := range []string{"-1", "1.5", " 2", "two", "99999999999"} {
		_, err := decodeVolcanicReport(map[string]any{"report_id": "r", "volcano_id": "merapi", "alert_level": "Awas", "eruption_count_24h": count, "reported_at": "2026-09-01T00:00:00Z"})
		require.ErrorIs(t, err, ErrContractViolation, count)
	}
	require.Error(t, SetVolcanicAdapter(3))
}
