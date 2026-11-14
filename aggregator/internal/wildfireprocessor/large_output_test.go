package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfire"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// This isolates the logical-intent/delivery boundary, not large-graph capacity.
// Real verification/activation, distributed chunks, HTTP resolver, canonical
// transaction and applied-progress paths still execute against actual storage.
func TestM6PreparedLargeCorrectionUsesOwnedReference(t *testing.T) {
	l := databaseLab(t, true)
	ctx := context.Background()
	l.append(t, wildfire.Operation{Version: 1, Namespace: l.e.Config.Namespace, Offset: 1, Kind: "heartbeat"})
	_, err := l.e.Step(ctx, l.p)
	require.NoError(t, err)
	j := job{Position: 1}
	tx, err := l.e.begin(ctx, l.p)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("synthetic-%d-", i) + strings.Repeat("S", 140000)
		keyA, keyB := name+"|2026-11-13|0000|0|0", name+"|2026-11-13|0001|0|0"
		id := rawinput.Identity("large-output-fixture-v1", i)
		intent := wildfire.Intent{OperationID: rawinput.Identity("large-operation-fixture-v1", i), Source: domain.SourceSatelit, SourceRefID: id, Operation: "upsert", SourceOrder: wildfire.SourceOrder{Namespace: l.e.Config.Namespace, Position: 1}, LogicalRevision: 1,
			Event: &wildfire.Event{HazardType: domain.HazardWildfire, Severity: domain.SeverityWaspada, AreaName: "Synthetic", OccurredAt: time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC), Attributes: domain.WildfireAttributes{DetectionCount: 2, Satellites: []string{name}, MaxConfidence: "n", TotalFRPMW: 2, DetectionKeys: []string{keyA, keyB}}}}
		require.NoError(t, l.e.put(ctx, tx, &j, "intent", id, intent, nil, true, ""))
	}
	_, err = tx.Exec(ctx, `UPDATE wildfire_jobs SET phase='verify',checkpoint='{}',record_count=$2,staged_bytes=$3,output_count=3 WHERE namespace=$1 AND position=1`, l.e.Config.Namespace, j.Records, j.Bytes)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	l.prepare(t, 1)
	var sealed bool
	for i := 0; i < 10 && !sealed; i++ {
		_, err = l.e.StepDelivery(ctx, l.p, l.d)
		require.NoError(t, err)
		require.NoError(t, l.c.QueryRow(ctx, `SELECT sealed FROM wildfire_jobs WHERE position=1`).Scan(&sealed))
	}
	require.True(t, sealed)
	var id, digest string
	var size, count int64
	var chunks int
	require.NoError(t, l.c.QueryRow(ctx, `SELECT batch_id,output_sha256,output_bytes,output_count,output_chunks FROM wildfire_jobs WHERE position=1`).Scan(&id, &digest, &size, &count, &chunks))
	require.Greater(t, size, canonical.InlineLimit)
	require.Greater(t, chunks, l.e.Config.PageRows)
	request, err := json.Marshal(canonical.Batch{ContractVersion: 1, BatchID: id, Ownership: l.d, MutationsRef: &canonical.PreparedReference{ID: id, Count: count, SHA256: digest}})
	require.NoError(t, err)
	require.Less(t, int64(len(request)), canonical.InlineLimit)
	router := gin.New()
	(canonical.HTTP{Repository: l.s, Token: "test-only-reference", Resolver: l.e, MaxPreparedBytes: l.e.Config.PreparedBytes}).Register(router)
	deadline := time.Now().Add(30 * time.Second)
	for {
		req := httptest.NewRequest(http.MethodPost, "/internal/canonical/batches", bytes.NewReader(request))
		req.Header.Set("Authorization", "Bearer test-only-reference")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		if response.Code == http.StatusOK {
			break
		}
		require.True(t, response.Code == 503 && time.Now().Before(deadline), "status=%d body=%s", response.Code, response.Body.String())
		t.Logf("retrying unchanged reference after HTTP 503")
		call, cancel := context.WithTimeout(ctx, canonical.Deadline)
		err = l.s.RecoverAttempts(call)
		cancel()
		if err != nil {
			require.True(t, retryableFixtureError(err), "%v", err)
		}
	}
	// The processor retries the same already committed batch before recording
	// delivery acknowledgement. No revised expected revisions are manufactured.
	l.deliver(t, 1)
	var events, operations, publications int
	require.NoError(t, l.c.QueryRow(ctx, `SELECT count(*) FROM hazard_events`).Scan(&events))
	require.NoError(t, l.c.QueryRow(ctx, `SELECT count(*) FROM canonical_operations`).Scan(&operations))
	require.NoError(t, l.c.QueryRow(ctx, `SELECT count(*) FROM hazard_outbox`).Scan(&publications))
	require.Equal(t, 3, events)
	require.Equal(t, 3, operations)
	require.Equal(t, 3, publications)
	t.Logf("complete reference correction: bytes=%d chunks=%d logical_count=%d sha256=%s; HTTP metadata bytes=%d", size, chunks, count, digest, len(request))
}
