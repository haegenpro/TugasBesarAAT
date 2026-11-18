package acquisition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"aggregator/internal/canonical"
	"aggregator/internal/platform"
	"github.com/google/uuid"
)

// ErrDispatchPending is deliberately not a retry authorization. No TTL, lease
// change, process restart or HTTP timeout can reclaim an uncertain dispatch.
var ErrDispatchPending = errors.New("upstream dispatch pending; receipt or sender fencing required")
var ErrDispatchIdentity = errors.New("upstream request identity missing or changed")

type Dispatch struct {
	Owner                      canonical.Ownership
	Lane, Key, Fingerprint, ID string
	Streaming                  bool
}
type Receipt struct {
	ID, State string
	Status    int
	Headers   http.Header
	Body      []byte
	StartedAt time.Time
}
type DispatchRepository interface {
	ReserveDispatch(context.Context, Dispatch) (Receipt, error)
	CompleteDispatch(context.Context, Dispatch, Receipt) error
	CloseDispatchStream(context.Context, Dispatch) error
}

type requestKey struct{}
type requestLane struct{}

// WithRequestLane separates the assessment archive reader from the regular
// history reader, so one cannot replace the other's unconsumed receipt.
func WithRequestLane(ctx context.Context, lane string) context.Context {
	return context.WithValue(ctx, requestLane{}, lane)
}

type requestIdentity struct {
	key     string
	mu      sync.Mutex
	started time.Time
}

// WithRequestKey binds finite requests to committed input progress, never to an
// ephemeral lease, random correlation ID or worker wall clock. Archive cursors
// distinguish pages; the complete URL and credentials are fingerprinted too.
func WithRequestKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, requestKey{}, &requestIdentity{key: key})
}

// Client is the only network path used by coordinated acquisition. The durable
// intent is committed before entering the real transport. A paused sender keeps
// that intent; a successor reuses its receipt or waits, even with a newer lease.
// No extra gateway pods or new database formation are needed.
func Client(repository DispatchRepository, source, base string, timeout time.Duration, maxBody int64) (*http.Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !Supported(source) || repository == nil || maxBody < 1 {
		return nil, errors.New("dispatch requires repository, source, fixed HTTP base and positive response limit")
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	// net/http may retry GETs on reused HTTP/1 connections and HTTP/2 may
	// retry streams internally. Admit one fresh HTTP/1 connection per request.
	t.DisableKeepAlives = true
	t.Protocols = new(http.Protocols)
	t.Protocols.SetHTTP1(true)
	t.Proxy = nil
	t.MaxConnsPerHost = 8
	return &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &dispatchTransport{repository: repository, source: source, base: u, maxBody: maxBody, next: platform.Transport{Base: t}}}, nil
}

type dispatchTransport struct {
	repository DispatchRepository
	source     string
	base       *url.URL
	maxBody    int64
	next       http.RoundTripper
}

func digest(v string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(v))) }
func (t *dispatchTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.Body != nil || req.URL.Scheme != t.base.Scheme || req.URL.Host != t.base.Host || req.URL.User != nil || req.URL.Fragment != "" || !strings.HasPrefix(req.URL.Path, strings.TrimRight(t.base.Path, "/")+"/") {
		return nil, ErrDispatchIdentity
	}
	ctx := req.Context()
	owner, err := Token(ctx, t.source)
	if err != nil {
		return nil, err
	}
	identity, _ := ctx.Value(requestKey{}).(*requestIdentity)
	if identity == nil {
		return nil, ErrDispatchIdentity
	}
	key := identity.key
	if key == "" {
		return nil, ErrDispatchIdentity
	}
	path := strings.TrimPrefix(req.URL.Path, strings.TrimRight(t.base.Path, "/"))
	allowed := false
	switch t.source {
	case "BMKG":
		allowed = path == "/seismic-events" || path == "/tsunami-warnings" || path == "/history" && (req.URL.Query().Get("entity") == "seismic-events" || req.URL.Query().Get("entity") == "tsunami-warnings")
	case "PVMBG":
		allowed = path == "/volcanic-reports" || path == "/history" && req.URL.Query().Get("entity") == "volcanic-reports"
	case "SATELIT":
		allowed = path == "/v1/stream" || path == "/v1/session"
	case "SENSOR_PANTAI":
		allowed = path == "/v1/stations" || path == "/v1/observations"
	}
	if !allowed {
		return nil, ErrDispatchIdentity
	}
	lane := path
	if lane == "/history" {
		lane += ":" + req.URL.Query().Get("entity")
		origin, _ := ctx.Value(requestLane{}).(string)
		if origin != "" {
			if origin != "assessment" || t.source != "BMKG" || req.URL.Query().Get("entity") != "tsunami-warnings" {
				return nil, ErrDispatchIdentity
			}
			lane += ":assessment"
		}
	}
	streaming := t.source == "SATELIT" && strings.HasSuffix(req.URL.Path, "/v1/stream")
	// Credentials affect the fingerprint but are never stored as plaintext.
	headers, _ := json.Marshal([]string{req.Header.Get("Accept"), req.Header.Get("Authorization"), req.Header.Get("X-BMKG-Key"), req.Header.Get("User-Agent")})
	d := Dispatch{Owner: owner, Lane: lane, Key: digest(key + "\x00" + lane + "\x00" + req.URL.Query().Get("after")), Fingerprint: digest(req.Method + "\x00" + req.URL.String() + "\x00" + string(headers)), ID: uuid.NewString(), Streaming: streaming}
	call, cancel := context.WithTimeout(ctx, canonical.Deadline)
	r, err := t.repository.ReserveDispatch(call, d)
	cancel()
	if err != nil {
		platform.Add("acquisition_dispatch_blocked_total", 1)
		return nil, fmt.Errorf("%w: %v", ErrDispatchPending, err)
	}
	identity.mu.Lock()
	if identity.started.IsZero() || r.StartedAt.Before(identity.started) {
		identity.started = r.StartedAt
	}
	identity.mu.Unlock()
	if r.State == "complete" {
		platform.Add("acquisition_dispatch_reused_total", 1)
		return response(req, r), nil
	}
	// There is intentionally no release-on-error defer. A transport error can
	// mean the source received the request but the reply was lost.
	platform.Add("acquisition_dispatch_admitted_total", 1)
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		return nil, fmt.Errorf("%w: transport outcome unknown: %v", ErrDispatchPending, err)
	}
	if streaming && resp.StatusCode == http.StatusOK {
		resp.Body = &dispatchStream{ReadCloser: resp.Body, finish: func() error {
			c, done := context.WithTimeout(context.Background(), canonical.Deadline)
			defer done()
			return t.repository.CloseDispatchStream(c, d)
		}}
		return resp, nil
	}
	// Rejected stream opens are finite responses and still have a known receipt.
	// Do not turn them into a cached infinite stream.
	r.Status, r.Headers = resp.StatusCode, resp.Header.Clone()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, t.maxBody+1))
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil || int64(len(body)) > t.maxBody {
		return nil, fmt.Errorf("%w: incomplete or oversized upstream response", ErrDispatchPending)
	}
	r.Body = body
	if streaming {
		c, done := context.WithTimeout(context.Background(), canonical.Deadline)
		err = t.repository.CloseDispatchStream(c, d)
		done()
	} else {
		c, done := context.WithTimeout(context.Background(), canonical.Deadline)
		err = t.repository.CompleteDispatch(c, d, r)
		done()
	}
	if err != nil {
		return nil, fmt.Errorf("%w: durable receipt unavailable", ErrDispatchPending)
	}
	return response(req, r), nil
}
func response(req *http.Request, r Receipt) *http.Response {
	h := r.Headers.Clone()
	if h == nil {
		h = make(http.Header)
	}
	h.Set("X-Acquisition-Started-At", r.StartedAt.UTC().Format(time.RFC3339Nano))
	return &http.Response{StatusCode: r.Status, Status: fmt.Sprintf("%d %s", r.Status, http.StatusText(r.Status)), Header: h, Body: io.NopCloser(bytes.NewReader(r.Body)), ContentLength: int64(len(r.Body)), Request: req, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1}
}

// Close waits for the underlying transport's body to close before making a
// subsequent stream open possible. A process frozen before/during Close leaves
// its durable intent pending. A killed process cannot clear it automatically.
type dispatchStream struct {
	io.ReadCloser
	once   sync.Once
	finish func() error
	err    error
}

func (s *dispatchStream) Close() error {
	s.once.Do(func() {
		s.err = s.ReadCloser.Close()
		if s.err == nil {
			s.err = s.finish()
		}
	})
	return s.err
}

// RequestStarted preserves the original snapshot boundary when a successor
// processes a saved response. Using the successor's start would skip updates.
func RequestStarted(ctx context.Context, fallback time.Time) time.Time {
	i, _ := ctx.Value(requestKey{}).(*requestIdentity)
	if i == nil {
		return fallback
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.started.IsZero() {
		return fallback
	}
	return i.started
}
