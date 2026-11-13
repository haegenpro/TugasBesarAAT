package rawinput

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgreementVectorsAndBinaryRoundTrip(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "m2-contract-examples.json"))
	require.NoError(t, err)
	var fixtures struct {
		Raw []Envelope `json:"raw_input_examples"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixtures))
	schemaBytes, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "raw-input-v1.avsc"))
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": string(schemaBytes)})
	}))
	defer server.Close()
	registry := avrowire.NewClient(server.URL)
	reader, err := avrowire.NewReader(string(schemaBytes))
	require.NoError(t, err)
	schema, err := registry.Lookup(context.Background(), 41)
	require.NoError(t, err)
	for _, original := range fixtures.Raw {
		require.NoError(t, Validate(original))
		require.Equal(t, original.MessageID, ID(original))
		require.Equal(t, original.Digest, Hash(original.Payload))
		frame, err := Encode(schema, original)
		require.NoError(t, err)
		require.Equal(t, []byte{0, 0, 0, 0, 41}, frame[:5])
		decoded, err := Decode(context.Background(), registry, reader, frame)
		require.NoError(t, err)
		require.Equal(t, original, decoded)
	}
	original := fixtures.Raw[0]
	original.Payload = []byte{0xff, 0, 0xfe}
	original.Size = 3
	original.Digest = Hash(original.Payload)
	original.MessageID = ID(original)
	frame, err := Encode(schema, original)
	require.NoError(t, err)
	got, err := Decode(context.Background(), registry, reader, frame)
	require.NoError(t, err)
	require.Equal(t, original.Payload, got.Payload)
}
func TestExactUTF8IdentityAndReferences(t *testing.T) {
	require.Empty(t, Identity("raw-v1", string([]byte{0xff})))
	require.Equal(t, "[\"SATELIT\",\"ns|é<&\u2028\",1,null,\"detection\"]", string(Compact("SATELIT", "ns|é<&\u2028", int64(1), nil, "detection")))
	require.NotEqual(t, Identity("x", "a|b", "c"), Identity("x", "a", "b|c"))
	for _, bad := range []string{"../secret", "https://other", "C:\\data", "x/y", ""} {
		require.False(t, Opaque(bad))
	}
}
func TestInvalidBytesAreNotTruncatedOrSilentlyAccepted(t *testing.T) {
	pos := int64(7)
	e := Envelope{Version: 1, Source: "SATELIT", Namespace: "fixture", Operation: "detection", Position: &pos, ReceivedAt: "2026-11-13T00:00:00Z", Payload: []byte("hello"), Size: 5, Digest: Hash([]byte("hello")), ContentType: "application/json", CorrelationID: "c", OwnershipScope: "acquisition:SATELIT", OwnershipEpoch: 1, OwnershipHolder: "producer"}
	e.MessageID = ID(e)
	require.NoError(t, Validate(e))
	e.ReceivedAt = "2026-11-13T00:00:00+00:00"
	require.NoError(t, Validate(e))
	e.Payload = e.Payload[:4]
	require.Error(t, Validate(e))
	e.Payload = nil
	ref := "payload-v1:1"
	e.PayloadRef = &ref
	require.NoError(t, Validate(e))
	_, err := Resolve(context.Background(), e, nil, 100)
	require.Error(t, err)
}
func TestMalformedFramesRetainExplicitErrors(t *testing.T) {
	c := avrowire.NewClient("http://127.0.0.1:1")
	for _, b := range [][]byte{nil, {1, 0, 0, 0, 1}, {0, 0, 0, 0, 0}} {
		_, err := Decode(context.Background(), c, nil, b)
		require.Error(t, err)
	}
}
func TestM3PartnerEnvelopeReceipt(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "role1-m3-m4-v1.json"))
	require.NoError(t, err)
	var f struct {
		Request AppendRequest `json:"satellite_append"`
		Receipt Receipt       `json:"receipt"`
	}
	require.NoError(t, json.Unmarshal(b, &f))
	require.Len(t, f.Request.Messages, 1)
	require.NoError(t, Validate(f.Request.Messages[0]))
	require.Equal(t, f.Request.Messages[0].MessageID, f.Receipt.Items[0].MessageID)
	require.Equal(t, int64(7), f.Receipt.Through)
}
func TestDeepJSONRejectedBeforeHistoryMutation(t *testing.T) {
	var v any
	err := canonical.Decode(strings.NewReader(strings.Repeat("[", 80)+"0"+strings.Repeat("]", 80)), &v)
	require.Error(t, err)
}

func TestOriginalSourceStringsRemainValidUTF8(t *testing.T) {
	require.Error(t, sourceValue(map[string]any{"region": string([]byte{0xff})}, 0))
	require.Error(t, sourceValue(map[string]any{string([]byte{0xff}): "value"}, 0))
	require.NoError(t, sourceValue(map[string]any{"region": "雪|é"}, 0))
}
