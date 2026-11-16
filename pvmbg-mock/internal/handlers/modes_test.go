package handlers

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"

	"pvmbg-mock/internal/avrowire"
	"pvmbg-mock/internal/state"
)

type fakeRegistry struct {
	mu      sync.Mutex
	config  map[string]string
	schemas map[int]string
	fail    bool
}

func newFakeRegistry(t *testing.T) (*fakeRegistry, *avrowire.Client) {
	t.Helper()
	f := &fakeRegistry{config: map[string]string{}, schemas: map[int]string{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.fail {
			http.Error(w, "unavailable", 503)
			return
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		switch {
		case strings.HasPrefix(r.URL.Path, "/config/"):
			f.config[strings.TrimPrefix(r.URL.Path, "/config/")] = body["compatibility"]
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/versions"):
			id := len(f.schemas) + 1
			f.schemas[id] = body["schema"]
			_ = json.NewEncoder(w).Encode(map[string]int{"id": id})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return f, avrowire.NewClient(server.URL)
}

func post(t *testing.T, h *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/schema-version", strings.NewReader(body))
	h.AdminSchemaVersion(c)
	return w
}

func liveJSON(t *testing.T, h *Handler) []map[string]any {
	t.Helper()
	w := invoke(t, h.VolcanicReports, http.MethodGet, "/volcanic-reports")
	require.Equal(t, 200, w.Code, w.Body.String())
	var rows []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rows))
	require.NotEmpty(t, rows)
	return rows
}

func TestIncompatibleModesChangeTheWireShape(t *testing.T) {
	cases := map[string]func(t *testing.T, row map[string]any){
		"rename": func(t *testing.T, row map[string]any) {
			require.NotContains(t, row, "alert_level")
			require.Equal(t, "Waspada", row["level"])
		},
		"type_change": func(t *testing.T, row map[string]any) {
			require.IsType(t, "", row["eruption_count_24h"])
		},
		"missing_required": func(t *testing.T, row map[string]any) {
			require.NotContains(t, row, "ash_column_height_m")
		},
	}
	for mode, check := range cases {
		t.Run(mode, func(t *testing.T) {
			h, a := testArchivedSource(t)
			head, err := a.Head()
			require.NoError(t, err)
			w := post(t, h, `{"mode":"`+mode+`"}`)
			require.Equal(t, 200, w.Code, w.Body.String())
			require.JSONEq(t, `{"schema_version":"`+mode+`","mode":"`+mode+`","confidence_level_visible":false}`, w.Body.String())
			check(t, liveJSON(t, h)[0])
			after, err := a.Head()
			require.NoError(t, err)
			require.Equal(t, head, after)
			w = invoke(t, h.History, http.MethodGet, "/history?entity=volcanic-reports")
			require.Equal(t, 200, w.Code, w.Body.String())
			var page struct {
				Data []struct {
					Payload map[string]any `json:"payload"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &page))
			require.NotEmpty(t, page.Data)
			check(t, page.Data[0].Payload)
		})
	}
}

func TestIncompatibleModesUseSeparateRegistrySubject(t *testing.T) {
	registry, client := newFakeRegistry(t)
	h, _ := testArchivedSource(t)
	h.Registry = client
	require.Equal(t, 200, post(t, h, `{"mode":"type_change"}`).Code)
	require.Equal(t, "NONE", registry.config[incompatibleSubject])
	require.Equal(t, "NONE", registry.config[incompatibleHistorySubject])
	require.NotContains(t, registry.config, volcanicSubject)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/volcanic-reports", nil)
	c.Request.Header.Set("Accept", avrowire.MediaType)
	h.VolcanicReports(c)
	require.Equal(t, 200, w.Code, w.Body.String())
	frame, _ := io.ReadAll(w.Body)
	id := int(binary.BigEndian.Uint32(frame[1:5]))
	codec, err := goavro.NewCodec(registry.schemas[id])
	require.NoError(t, err)
	native, _, err := codec.NativeFromBinary(frame[5:])
	require.NoError(t, err)
	row := native.([]any)[0].(map[string]any)
	require.IsType(t, "", row["eruption_count_24h"])
	require.Equal(t, 200, post(t, h, `{"mode":"v1"}`).Code)
	require.Equal(t, "FULL_TRANSITIVE", registry.config[volcanicSubject])
}

func TestSchemaModeRejectsUnknownAndKeepsStateOnRegistryFailure(t *testing.T) {
	registry, client := newFakeRegistry(t)
	h, _ := testArchivedSource(t)
	h.Registry = client
	require.Equal(t, 400, post(t, h, `{"mode":"drop_everything"}`).Code)
	require.Equal(t, 400, post(t, h, `{"mode":"rename","extra":true}`).Code)
	require.Equal(t, state.ModeV1, h.Toggles.Mode())
	registry.fail = true
	require.Equal(t, 503, post(t, h, `{"mode":"missing_required"}`).Code)
	require.Equal(t, state.ModeV1, h.Toggles.Mode())
	registry.fail = false
	require.Equal(t, 200, post(t, h, `{"mode":"rename"}`).Code)
	require.Equal(t, 200, post(t, h, ``).Code)
	require.Equal(t, state.ModeV2, h.Toggles.Mode())
	require.Equal(t, 200, post(t, h, ``).Code)
	require.Equal(t, state.ModeV1, h.Toggles.Mode())
}
