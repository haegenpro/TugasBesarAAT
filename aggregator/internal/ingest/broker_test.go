package ingest

import (
	"aggregator/internal/avrowire"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func TestRawAvroHandoffIsBoundedAndPreservesOriginalOffset(t *testing.T) {
	i := Input{ID: "original-message", Source: "SATELIT", Namespace: "session", Position: 1<<54 + 17, CorrelationID: "correlation", ReceivedAt: time.Now().UTC(), Payload: bytes.Repeat([]byte("original bytes"), 100000)}
	codec, e := goavro.NewCodec(RawSchema)
	require.NoError(t, e)
	frame, e := avrowire.Encode(&avrowire.Schema{ID: 17, Codec: codec}, referenceValue(i))
	require.NoError(t, e)
	require.Less(t, len(frame), 512)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"schema": RawSchema})
	}))
	defer server.Close()
	decoded, e := decodeInput(context.Background(), avrowire.NewClient(server.URL), frame)
	require.NoError(t, e)
	require.Equal(t, i.ID, decoded.ID)
	require.Equal(t, i.Position, decoded.Position)
	require.Equal(t, i.CorrelationID, decoded.CorrelationID)
	require.Empty(t, decoded.Payload)
}
