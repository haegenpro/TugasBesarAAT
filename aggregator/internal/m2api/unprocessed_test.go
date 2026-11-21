package m2api

import (
	"aggregator/internal/store"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIngestionAuditPaginationUsesOwnerCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer private-owner-token", r.Header.Get("Authorization"))
		require.Equal(t, "SATELIT", r.URL.Query().Get("source"))
		require.Equal(t, "3", r.URL.Query().Get("limit"))
		if r.URL.Query().Get("cursor") == "remote-next" {
			fmt.Fprint(w, `{"items":[],"next_cursor":null}`)
		} else {
			fmt.Fprint(w, `{"items":[{"message_id":"original-input"}],"next_cursor":"remote-next"}`)
		}
	}))
	defer server.Close()
	cfg := Config{IngestionURL: server.URL, IngestionToken: "private-owner-token"}
	page, e := unprocessed(context.Background(), nil, cfg, "SATELIT", 3, "")
	require.NoError(t, e)
	require.Len(t, page.Items, 1)
	require.NotNil(t, page.Next)
	next, e := unprocessed(context.Background(), nil, cfg, "SATELIT", 3, *page.Next)
	require.NoError(t, e)
	require.Empty(t, next.Items)
	require.Nil(t, next.Next)
	_, e = unprocessed(context.Background(), nil, cfg, "SENSOR_PANTAI", 3, *page.Next)
	require.ErrorIs(t, e, store.ErrCursor)
}
func TestUnavailableIngestionNeverPretendsAuditIsComplete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	page, e := unprocessed(context.Background(), nil, Config{IngestionURL: server.URL, IngestionToken: "token"}, "SATELIT", 10, "")
	require.NoError(t, e)
	require.True(t, page.Partial)
	require.Contains(t, page.Unavailable, "ingestion")
	require.NotNil(t, page.Next)
	_, e = unprocessed(context.Background(), nil, Config{}, "SATELIT", 10, base64.RawURLEncoding.EncodeToString([]byte(`{}`)))
	require.ErrorIs(t, e, store.ErrCursor)
}
