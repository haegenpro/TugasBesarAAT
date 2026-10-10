package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"aggregator/internal/avrowire"
	"github.com/stretchr/testify/require"
)

func TestHistoryRequiresAvroAndValidatesWriterRules(t *testing.T) {
	text := `{"type":"record","name":"Page","fields":[{"name":"data","type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"cursor","type":"long"},{"name":"payload","type":{"type":"record","name":"Report","fields":[{"name":"score","type":"double","x-validation":{"minimum":0,"maximum":1}}]}}]}}},{"name":"next_cursor","type":"long"},{"name":"has_more","type":"boolean"}]}`
	registryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PUT":
			fmt.Fprint(w, `{}`)
		case "POST":
			fmt.Fprint(w, `{"id":7}`)
		default:
			json.NewEncoder(w).Encode(map[string]any{"schema": text})
		}
	}))
	defer registryServer.Close()
	registry := avrowire.NewClient(registryServer.URL)
	schema, e := registry.Register(context.Background(), "test-history", text)
	require.NoError(t, e)
	score := 0.5
	jsonResponse := false
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, avrowire.MediaType, r.Header.Get("Accept"))
		if jsonResponse {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"data":[],"next_cursor":0,"has_more":false}`)
			return
		}
		b, e := avrowire.Encode(schema, map[string]any{"data": []any{map[string]any{"cursor": int64(9007199254740993), "payload": map[string]any{"score": score}}}, "next_cursor": int64(9007199254740993), "has_more": false})
		require.NoError(t, e)
		w.Header().Set("Content-Type", avrowire.MediaType)
		w.Write(b)
	}))
	defer source.Close()
	c := NewPVMBGClient(source.URL, "token", slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Registry = avrowire.NewClient(registryServer.URL)
	reader, e := avrowire.NewReader(text)
	require.NoError(t, e)
	c.Reader = &avrowire.SourceReader{History: reader}
	page, e := c.FetchHistory(context.Background(), "test", "volcanic-reports", 0)
	require.NoError(t, e)
	require.Equal(t, int64(9007199254740993), page.NextCursor)
	registryServer.Close() // previously fetched writer schema remains usable
	score = 1.5
	_, e = c.FetchHistory(context.Background(), "test", "volcanic-reports", 0)
	require.ErrorContains(t, e, "pvmbg history")
	require.ErrorContains(t, e, "$.data[0].payload.score: maximum")
	jsonResponse = true
	_, e = c.FetchHistory(context.Background(), "test", "volcanic-reports", 0)
	require.ErrorIs(t, e, ErrContractViolation)
}
