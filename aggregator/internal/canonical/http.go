package canonical

import (
	"aggregator/internal/domain"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"hash"
	"io"
	"net/http"
	"regexp"
	"strings"
)

type Repository interface {
	ApplyBatch(context.Context, Batch) (Result, error)
	Reconcile(context.Context, domain.Source, string) (Entity, error)
}

// PreparedResolver is injected by the owning output-intent service. The adapter
// never interprets an opaque ID as a filesystem path, URL or database DSN.
type PreparedResolver interface {
	Open(context.Context, PreparedReference) (io.ReadCloser, error)
}
type HTTP struct {
	Repository       Repository
	Token            string
	Resolver         PreparedResolver
	MaxPreparedBytes int64
}

func (h HTTP) Register(r *gin.Engine) {
	r.POST("/internal/canonical/batches", h.batch)
	r.GET("/internal/canonical/entities", h.entity)
}
func (h HTTP) authorize(c *gin.Context) bool {
	raw := c.GetHeader("Authorization")
	if !strings.HasPrefix(raw, "Bearer ") || h.Token == "" {
		writeError(c, Fail(401, "unauthorized", "service credential required"))
		return false
	}
	got, want := sha256.Sum256([]byte(strings.TrimPrefix(raw, "Bearer "))), sha256.Sum256([]byte(h.Token))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		writeError(c, Fail(403, "forbidden", "service credential denied"))
		return false
	}
	return true
}
func writeError(c *gin.Context, err error) {
	var p *Error
	if !errors.As(err, &p) {
		p = Unavailable()
	}
	body := gin.H{"code": p.Code, "message": p.Message}
	if p.Details != nil {
		body["details"] = p.Details
	}
	c.JSON(p.Status, gin.H{"error": body})
}
func (h HTTP) batch(c *gin.Context) {
	if !h.authorize(c) {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), Deadline)
	defer cancel()
	body := http.MaxBytesReader(c.Writer, c.Request.Body, InlineLimit)
	stop := context.AfterFunc(ctx, func() { _ = body.Close() })
	defer stop()
	var b Batch
	if err := Decode(body, &b); err != nil {
		if ctx.Err() != nil {
			writeError(c, Unavailable())
			return
		}
		var over *http.MaxBytesError
		if errors.As(err, &over) {
			writeError(c, Fail(413, "payload_too_large", "inline request exceeds 1 MiB"))
		} else {
			writeError(c, Fail(400, "invalid_request", "malformed, duplicate, unknown or trailing JSON fields"))
		}
		return
	}
	if b.MutationsRef != nil {
		if len(b.Mutations) != 0 {
			writeError(c, Fail(400, "invalid_request", "mutations and mutations_ref are mutually exclusive"))
			return
		}
		mutations, err := h.resolve(ctx, *b.MutationsRef)
		if err != nil {
			writeError(c, err)
			return
		}
		b.Mutations = mutations
		b.MutationsRef = nil
	}
	if err := Validate(b); err != nil {
		writeError(c, err)
		return
	}
	type response struct {
		result Result
		err    error
	}
	done := make(chan response, 1)
	go func() { result, err := h.Repository.ApplyBatch(ctx, b); done <- response{result, err} }()
	select {
	case <-ctx.Done():
		writeError(c, Unavailable())
	case reply := <-done:
		if ctx.Err() != nil {
			writeError(c, Unavailable())
			return
		}
		if reply.err != nil {
			writeError(c, reply.err)
			return
		}
		c.JSON(200, reply.result)
	}
}
func (h HTTP) entity(c *gin.Context) {
	if !h.authorize(c) {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), Deadline)
	defer cancel()
	if len(c.Request.URL.Query()) != 2 || len(c.Request.URL.Query()["source"]) != 1 || len(c.Request.URL.Query()["source_ref_id"]) != 1 {
		writeError(c, Fail(400, "invalid_request", "source and source_ref_id required exactly once"))
		return
	}
	source, ref := domain.Source(c.Query("source")), c.Query("source_ref_id")
	type response struct {
		entity Entity
		err    error
	}
	done := make(chan response, 1)
	go func() { e, err := h.Repository.Reconcile(ctx, source, ref); done <- response{e, err} }()
	select {
	case <-ctx.Done():
		writeError(c, Unavailable())
	case reply := <-done:
		if ctx.Err() != nil {
			writeError(c, Unavailable())
			return
		}
		if reply.err != nil {
			writeError(c, reply.err)
			return
		}
		c.JSON(200, reply.entity)
	}
}

var opaqueID = regexp.MustCompile(`^[A-Za-z0-9:_-]{1,256}$`)

type hashedReader struct {
	r io.Reader
	h hash.Hash
}

func (r hashedReader) Read(b []byte) (int, error) {
	n, e := r.r.Read(b)
	_, _ = r.h.Write(b[:n])
	return n, e
}
func (h HTTP) resolve(ctx context.Context, ref PreparedReference) ([]Mutation, error) {
	digest, err := hex.DecodeString(ref.SHA256)
	if !opaqueID.MatchString(ref.ID) || ref.Count < 1 || err != nil || len(digest) != sha256.Size {
		return nil, Fail(400, "invalid_request", "invalid prepared reference")
	}
	if h.Resolver == nil || h.MaxPreparedBytes < 1 {
		return nil, Unavailable()
	}
	r, err := h.Resolver.Open(ctx, ref)
	if err != nil {
		return nil, Unavailable()
	}
	defer r.Close()
	stop := context.AfterFunc(ctx, func() { _ = r.Close() })
	defer stop()
	limited := &io.LimitedReader{R: r, N: h.MaxPreparedBytes + 1}
	sum := sha256.New()
	d := json.NewDecoder(hashedReader{limited, sum})
	d.UseNumber()
	t, err := d.Token()
	if err != nil || t != json.Delim('[') {
		return nil, Fail(400, "invalid_request", "prepared content must be a mutation array")
	}
	items := []Mutation{}
	for d.More() {
		if ctx.Err() != nil {
			return nil, Unavailable()
		}
		// Each element is bounded by the total configured quota. Decode strict JSON
		// separately so duplicate keys and unknown mutation fields remain rejected.
		var raw json.RawMessage
		if err = d.Decode(&raw); err != nil {
			return nil, Fail(400, "invalid_request", "invalid prepared content")
		}
		var m Mutation
		if err = Decode(strings.NewReader(string(raw)), &m); err != nil {
			return nil, Fail(400, "invalid_request", "invalid prepared mutation")
		}
		items = append(items, m)
		if int64(len(items)) > ref.Count {
			return nil, Fail(400, "invalid_request", "prepared count mismatch")
		}
	}
	if _, err = d.Token(); err != nil {
		return nil, Fail(400, "invalid_request", "incomplete prepared array")
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, Fail(400, "invalid_request", "prepared trailing content")
	}
	if limited.N <= 0 {
		return nil, Unavailable()
	}
	if int64(len(items)) != ref.Count || subtle.ConstantTimeCompare(sum.Sum(nil), digest) != 1 {
		return nil, Fail(400, "invalid_request", "prepared count or SHA-256 mismatch")
	}
	return items, nil
}
