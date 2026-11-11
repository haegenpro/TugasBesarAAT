package store

import (
	"aggregator/internal/canonical"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type preparedFixture []byte

func (p preparedFixture) Open(context.Context, canonical.PreparedReference) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(string(p))), nil
}
func TestM2PreparedReferenceHTTP(t *testing.T) {
	dsn, c := m2Database(t, true)
	m2Schema(t, c, "citus", false)
	s := m2Store(t, dsn)
	b := m2Batch("prepared-a", 1, 0)
	other := m2Batch("prepared-b", 1, 0)
	b.Mutations = append(b.Mutations, other.Mutations[0])
	for i := range b.Mutations {
		b.Mutations[i].Event.Attributes["large_fixture"] = strings.Repeat("x", 700<<10)
	}
	prepared, e := json.Marshal(b.Mutations)
	require.NoError(t, e)
	require.Greater(t, len(prepared), int(canonical.InlineLimit))
	require.Less(t, len(prepared), 2<<20)
	sum := sha256.Sum256(prepared)
	b.MutationsRef = &canonical.PreparedReference{ID: "owned:prepared", Count: 2, SHA256: fmt.Sprintf("%x", sum)}
	b.Mutations = nil
	body, e := json.Marshal(b)
	require.NoError(t, e)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	canonical.HTTP{Repository: s, Token: "fixture-service", Resolver: preparedFixture(prepared), MaxPreparedBytes: 2 << 20}.Register(r)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	status := 0
	for attempt := 0; attempt < 3; attempt++ {
		req := httptest.NewRequest("POST", "/internal/canonical/batches", strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer fixture-service")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		status = w.Code
		if status == 503 {
			require.NoError(t, s.RecoverAttempts(context.Background()))
			continue
		}
		require.Equal(t, 200, status, w.Body.String())
		break
	}
	require.Equal(t, 200, status)
	runtime.ReadMemStats(&after)
	var entities, operations, intents int
	require.NoError(t, c.QueryRow(context.Background(), "SELECT (SELECT count(*) FROM canonical_entities),(SELECT count(*) FROM canonical_operations),(SELECT count(*) FROM hazard_outbox)").Scan(&entities, &operations, &intents))
	require.Equal(t, 2, entities)
	require.Equal(t, 2, operations)
	require.Equal(t, 2, intents)
	record := map[string]any{"quota_bytes": 2 << 20, "prepared_bytes": len(prepared), "sha256": fmt.Sprintf("%x", sum), "inline_reference_bytes": len(body), "elapsed_ms": time.Since(started).Milliseconds(), "heap_before": before.HeapInuse, "heap_after": after.HeapInuse, "entities": entities, "operations": operations, "publication_intents": intents, "quota_scope": "measured synthetic lab only; no production quota configured"}
	if dir := os.Getenv("M2_EVIDENCE_DIR"); dir != "" {
		raw, _ := json.MarshalIndent(record, "", "  ")
		require.NoError(t, os.WriteFile(filepath.Join(dir, "prepared-reference.json"), raw, 0600))
	}
	t.Log(record)
}
