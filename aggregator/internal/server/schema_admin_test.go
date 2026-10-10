package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"aggregator/internal/avrowire"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestSchemaAdminAuthenticationAndToggle(t *testing.T) {
	baseline := `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"id","type":"string"}]}}`
	reader, err := avrowire.NewSourceReader(baseline, "Page")
	require.NoError(t, err)
	readers, err := avrowire.NewReaderSwitch(reader, "Page")
	require.NoError(t, err)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterSchemaAdmin(r, readers, "operator")
	request := func(auth string, status int, body string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/admin/schema-version", nil)
		req.Header.Set("Authorization", auth)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, status, w.Code)
		if body != "" {
			require.JSONEq(t, body, w.Body.String())
		}
	}
	for _, auth := range []string{"", "Bearer source-data-token", "Bearer pvmbg-admin", "operator"} {
		request(auth, http.StatusUnauthorized, "")
	}
	request("Bearer operator", http.StatusConflict, "")
	require.NoError(t, readers.Observe(&avrowire.Schema{Text: `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"id","type":"string"},{"name":"extra","type":"double","default":0}]}}`}))
	request("Bearer operator", http.StatusOK, `{"reader_version":"v2"}`)
	request("Bearer operator", http.StatusOK, `{"reader_version":"v1"}`)
	request("Bearer operator", http.StatusOK, `{"reader_version":"v2"}`)
}
