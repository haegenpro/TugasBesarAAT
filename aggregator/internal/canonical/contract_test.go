package canonical

import (
	"aggregator/internal/domain"
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
	"strings"
	"testing"
	"time"
)

func fixtureBatch() Batch {
	return Batch{ContractVersion: 1, BatchID: "batch", Ownership: Ownership{Scope: "fixture", Epoch: 1, Holder: "owner"}, Mutations: []Mutation{{OperationID: "op", Source: domain.SourceBMKG, SourceRefID: "ref", Operation: "upsert", SourceOrder: SourceOrder{Namespace: "fixture", Position: 0}, ExpectedRevision: 0, Event: &MutableEvent{HazardType: domain.HazardSeismic, Severity: domain.SeverityNormal, AreaName: "Fixture", OccurredAt: time.Now().UTC(), Attributes: map[string]any{}}}}}
}
func TestRouteAndFingerprint(t *testing.T) {
	for _, ref := range []string{"a|b:c", "雪<&>\u2028\u2029", "a\t\n\"\\b"} {
		r, e := Route("BMKG", ref)
		require.NoError(t, e)
		var pair []string
		require.NoError(t, json.Unmarshal([]byte(r), &pair))
		require.Equal(t, []string{"BMKG", ref}, pair)
	}
	_, e := Route("BMKG", string([]byte{0xff}))
	require.Error(t, e)
	_, e = Route("BMKG", "a\x00b")
	require.Error(t, e)
	a, e := Fingerprint(map[string]any{"n": json.Number("1.0")})
	require.NoError(t, e)
	b, e := Fingerprint(map[string]any{"n": json.Number("1e0")})
	require.NoError(t, e)
	require.Equal(t, a, b)
	a, _ = Fingerprint(json.Number("9007199254740993"))
	b, _ = Fingerprint(json.Number("9007199254740992"))
	require.NotEqual(t, a, b)
}
func TestStrictContract(t *testing.T) {
	b := fixtureBatch()
	raw, e := json.Marshal(b)
	require.NoError(t, e)
	var decoded Batch
	require.NoError(t, Decode(strings.NewReader(string(raw)), &decoded))
	require.NoError(t, Validate(decoded))
	for _, bad := range []string{strings.Replace(string(raw), `"expected_revision":0,`, "", 1), strings.Replace(string(raw), `"position":0`, `"position":null`, 1), strings.Replace(string(raw), `"latitude":0,`, "", 1), strings.Replace(string(raw), `"batch_id":"batch"`, `"batch_id":"batch","batch_id":"other"`, 1), string(raw) + " {}", strings.Replace(string(raw), `"expected_revision":0`, `"unexpected":0`, 1)} {
		require.Error(t, Decode(strings.NewReader(bad), &decoded), bad)
	}
	b.Mutations = append(b.Mutations, b.Mutations[0])
	require.Error(t, Validate(b))
}

type httpRepo struct {
	calls int
	err   error
}

func (r *httpRepo) ApplyBatch(context.Context, Batch) (Result, error) {
	r.calls++
	return Result{BatchID: "batch", Outcome: "applied", Items: []Item{}}, r.err
}
func (r *httpRepo) Reconcile(context.Context, domain.Source, string) (Entity, error) {
	r.calls++
	return Entity{Active: false}, r.err
}

type resolver string

func (r resolver) Open(context.Context, PreparedReference) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(string(r))), nil
}
func TestPrivateHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &httpRepo{}
	h := HTTP{Repository: repo, Token: "dedicated"}
	router := gin.New()
	h.Register(router)
	raw, _ := json.Marshal(fixtureBatch())
	request := func(auth, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/internal/canonical/batches", strings.NewReader(body))
		r.Header.Set("Authorization", auth)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
	require.Equal(t, 401, request("", string(raw)).Code)
	require.Equal(t, 403, request("Bearer audit-token", string(raw)).Code)
	require.Zero(t, repo.calls)
	require.Equal(t, 200, request("Bearer dedicated", string(raw)).Code)
	require.Equal(t, 1, repo.calls)
	require.Equal(t, 400, request("Bearer dedicated", `{}`).Code)
	require.Equal(t, 413, request("Bearer dedicated", strings.Repeat(" ", int(InlineLimit)+1)).Code)
	repo.err = Fail(409, "revision_conflict", "changed")
	w := request("Bearer dedicated", string(raw))
	require.Equal(t, 409, w.Code)
	require.Contains(t, w.Body.String(), "revision_conflict")
	repo.err = nil
	w = httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/internal/canonical/entities?source=BMKG&source_ref_id=a", nil)
	r.Header.Set("Authorization", "Bearer dedicated")
	router.ServeHTTP(w, r)
	require.Equal(t, 200, w.Code)
	require.Contains(t, w.Body.String(), `"event":null`)
}
func TestPreparedReferences(t *testing.T) {
	b := fixtureBatch()
	raw, _ := json.Marshal(b.Mutations)
	sum := sha256.Sum256(raw)
	ref := PreparedReference{ID: "owned:batch", Count: 1, SHA256: fmt.Sprintf("%x", sum)}
	h := HTTP{Resolver: resolver(raw), MaxPreparedBytes: int64(len(raw))}
	m, e := h.resolve(context.Background(), ref)
	require.NoError(t, e)
	require.Len(t, m, 1)
	h.MaxPreparedBytes = 0
	_, e = h.resolve(context.Background(), ref)
	require.Error(t, e)
	h.MaxPreparedBytes = int64(len(raw))
	ref.Count = 2
	_, e = h.resolve(context.Background(), ref)
	require.Error(t, e)
	ref.ID = "https://example.com"
	_, e = h.resolve(context.Background(), ref)
	require.Error(t, e)
}

func TestSharedM2ContractFixtures(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "fixtures", "m2-contract-examples.json"))
	require.NoError(t, err)
	var fixture struct {
		CanonicalBatch struct {
			Request json.RawMessage `json:"request"`
		} `json:"canonical_batch"`
		Routes []struct {
			Source   string `json:"source"`
			Ref      string `json:"source_ref_id"`
			Expected string `json:"expected"`
		} `json:"distribution_key_examples"`
		Wildfire domain.HazardEvent `json:"canonical_wildfire_example"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	var b Batch
	require.NoError(t, Decode(strings.NewReader(string(fixture.CanonicalBatch.Request)), &b))
	require.NoError(t, Validate(b))
	for _, r := range fixture.Routes {
		got, e := Route(r.Source, r.Ref)
		require.NoError(t, e)
		require.Equal(t, r.Expected, got)
	}
	require.NoError(t, domain.ValidateWildfire(fixture.Wildfire.Attributes))
}

type blockedRepository struct{ release chan struct{} }

func (r blockedRepository) ApplyBatch(context.Context, Batch) (Result, error) {
	<-r.release
	return Result{}, nil
}
func (r blockedRepository) Reconcile(context.Context, domain.Source, string) (Entity, error) {
	<-r.release
	return Entity{}, nil
}
func TestHTTPDeadlineBoundsUnknownOutcome(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	r := gin.New()
	HTTP{Repository: blockedRepository{release}, Token: "dedicated"}.Register(r)
	raw, _ := json.Marshal(fixtureBatch())
	request := httptest.NewRequest("POST", "/internal/canonical/batches", strings.NewReader(string(raw)))
	request.Header.Set("Authorization", "Bearer dedicated")
	start := time.Now()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, request)
	require.Equal(t, 503, w.Code)
	require.Less(t, time.Since(start), 2250*time.Millisecond)
}
