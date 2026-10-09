package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"aggregator/internal/middleware"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func brokerLogs(t *testing.T, output string) []map[string]any {
	t.Helper()
	var result []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		require.Equal(t, "nats", entry["target_service"])
		require.NotEmpty(t, entry["correlation_id"])
		require.GreaterOrEqual(t, entry["latency_ms"].(float64), float64(0))
		result = append(result, entry)
	}
	return result
}

func TestBrokerCallLogsFailureWithoutConnectionCredentials(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	ctx := middleware.WithCorrelationID(context.Background(), "setup-cid")
	cause := errors.New("nats://private-user:private-password@host")
	_, err := brokerCall(ctx, logger, "broker_connect", func() (int, error) { return 0, cause })
	require.ErrorIs(t, err, cause)
	logs := brokerLogs(t, output.String())
	require.Equal(t, "setup-cid", logs[0]["correlation_id"])
	require.Equal(t, false, logs[0]["success"])
	require.NotContains(t, output.String(), "private-")
}

func TestPublisherSetupTracesConnectLookupAndInfo(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("set TEST_NATS_URL for publisher setup integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	stream, subject := "TRACE_"+suffix, "trace."+suffix
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: stream, Subjects: []string{subject}})
	require.NoError(t, err)
	defer js.DeleteStream(context.Background(), stream)
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	publisher, err := New(ctx, url, stream, subject, nil, logger)
	require.NoError(t, err)
	publisher.Close()
	logs := brokerLogs(t, output.String())
	require.Len(t, logs, 3)
	for i, operation := range []string{"broker_connect", "stream_lookup", "stream_info"} {
		require.Equal(t, operation, logs[i]["target_endpoint"])
		require.Equal(t, true, logs[i]["success"])
		require.Equal(t, logs[0]["correlation_id"], logs[i]["correlation_id"])
	}
	output.Reset()
	_, err = New(middleware.WithCorrelationID(ctx, "failed-lookup-cid"), url, "MISSING_"+suffix, subject, nil, logger)
	require.Error(t, err)
	logs = brokerLogs(t, output.String())
	require.Len(t, logs, 2)
	require.Equal(t, "stream_lookup", logs[1]["target_endpoint"])
	require.Equal(t, false, logs[1]["success"])
	require.Equal(t, "failed-lookup-cid", logs[1]["correlation_id"])
}
