package acquisition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"aggregator/internal/canonical"
	"aggregator/internal/platform"
	"github.com/google/uuid"
)

// ErrDispatchPending preserves input progress while acquisition retries. Only
// the authoritative current owner can supersede an interrupted/timed-out nonce.
var ErrDispatchPending = errors.New("upstream dispatch awaiting automatic recovery")

const DispatchAttemptTimeout = 10 * time.Second
const DispatchRetryInterval = 500 * time.Millisecond

// DispatchWait is admission contention, not an upstream failure. Source loops
// must not feed it into exponential backoff or circuit-breaker failure counts.
type DispatchWait struct{}

func (*DispatchWait) Error() string { return "source dispatch is still in flight" }
func (*DispatchWait) Unwrap() error { return ErrDispatchPending }
func WaitDelay(err error) (time.Duration, bool) {
	var wait *DispatchWait
	return DispatchRetryInterval, errors.As(err, &wait)
}

var ErrDispatchIdentity = errors.New("upstream request identity missing or changed")

type Dispatch struct {
	Owner                      canonical.Ownership
	Lane, Key, Fingerprint, ID string
	Streaming                  bool
}
type Receipt struct {
	ID, State     string
	Status        int
	Headers       http.Header
	Body          []byte
	StartedAt     time.Time
	Recovered     bool
	RecoveryCount int64
}
type DispatchRepository interface {
	ReserveDispatch(context.Context, Dispatch) (Receipt, error)
	CompleteDispatch(context.Context, Dispatch, Receipt) error
	CheckDispatch(context.Context, Dispatch) error
	OpenDispatchStream(context.Context, Dispatch) error
	InterruptDispatch(context.Context, Dispatch, string) error
	CloseDispatchStream(context.Context, Dispatch) error
	FinishResponse(context.Context, Dispatch, string) error
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
// intent is committed before entering the real transport. A successor reuses
// a receipt or performs a bounded read retry from persisted input progress.
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
	t.DialContext = (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	t.TLSHandshakeTimeout = 3 * time.Second
	t.ResponseHeaderTimeout = 5 * time.Second
	return &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &dispatchTransport{repository: repository, source: source, base: u, maxBody: maxBody, next: platform.Transport{Base: t}}}, nil
}

type dispatchTransport struct {
	repository DispatchRepository
	source     string
	base       *url.URL
	maxBody    int64
	next       http.RoundTripper
	waitMu     sync.Mutex
	waitEpoch  int64
	waiting    map[string]bool
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
		t.markWaiting(owner, lane, true)
		platform.Add("acquisition_dispatch_blocked_total", 1)
		return nil, fmt.Errorf("%w: %w", ErrDispatchPending, err)
	}
	t.markWaiting(owner, lane, false)
	identity.mu.Lock()
	if identity.started.IsZero() || r.StartedAt.Before(identity.started) {
		identity.started = r.StartedAt
	}
	identity.mu.Unlock()
	if r.State == "complete" {
		platform.Add("acquisition_dispatch_reused_total", 1)
		return attachReceipt(response(req, r), t.repository, d, r.ID, false), nil
	}
	if r.Recovered {
		platform.Add("acquisition_dispatch_recovered_total", 1)
	}
	// A pause after Reserve must not revive the sender that lost ownership.
	// This narrows the check/send race; the upstream does not enforce our epoch.
	if _, err = Token(ctx, t.source); err != nil {
		t.interrupt(d, "preflight")
		return nil, err
	}
	check, stop := context.WithTimeout(ctx, canonical.Deadline)
	err = t.repository.CheckDispatch(check, d)
	stop()
	if err != nil {
		t.interrupt(d, "preflight")
		return nil, fmt.Errorf("%w: %w", ErrDispatchPending, err)
	}
	if _, err = Token(ctx, t.source); err != nil {
		t.interrupt(d, "preflight")
		return nil, err
	}
	networkCtx := ctx
	var networkCancel context.CancelFunc = func() {}
	transferred := false
	defer func() {
		if !transferred {
			networkCancel()
		}
	}()
	if !streaming {
		networkCtx, networkCancel = context.WithTimeout(ctx, 8*time.Second)
	}
	request := req.Clone(networkCtx)
	request.Header = req.Header.Clone()
	request.Header.Set("X-Acquisition-Request-ID", d.Key)
	request.Header.Set("X-Acquisition-Epoch", strconv.FormatInt(owner.Epoch, 10))
	request.Header.Set("X-Acquisition-Recovery", strconv.FormatBool(r.Recovered))
	platform.Add("acquisition_dispatch_admitted_total", 1)
	resp, err := t.next.RoundTrip(request)
	if err != nil {
		t.interrupt(d, "transport")
		return nil, fmt.Errorf("%w: transport outcome unknown: %v", ErrDispatchPending, err)
	}
	resp.Request = req
	if streaming && resp.StatusCode == http.StatusOK {
		c, done := context.WithTimeout(context.Background(), canonical.Deadline)
		err = t.repository.OpenDispatchStream(c, d)
		done()
		if err != nil {
			_ = resp.Body.Close()
			t.interrupt(d, "receipt")
			return nil, fmt.Errorf("%w: stream admission unavailable", ErrDispatchPending)
		}
		resp.Body = &dispatchStream{ReadCloser: resp.Body, interrupt: func() { t.interrupt(d, "body") }, finish: func() error {
			c, done := context.WithTimeout(context.Background(), canonical.Deadline)
			defer done()
			err := t.repository.CloseDispatchStream(c, d)
			if err != nil {
				t.interrupt(d, "receipt")
			}
			return err
		}}
		return resp, nil
	}
	// Rejected stream opens are finite responses and still have a known receipt.
	// Do not turn them into a cached infinite stream.
	r.Status, r.Headers = resp.StatusCode, resp.Header.Clone()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, t.maxBody+1))
	if int64(len(body)) > t.maxBody && readErr == nil {
		// The cache bound is not permission to discard failed input. Give the
		// bounded prefix and ALL remaining bytes to the application recorder.
		refResponse := attachReceipt(resp, t.repository, d, r.ID, true)
		ref := responseReference(refResponse)
		refResponse.Body = &overflowBody{Reader: io.MultiReader(bytes.NewReader(body), resp.Body), source: resp.Body, cancel: networkCancel, ref: ref, interrupt: func() { t.interrupt(d, "body") }}
		transferred = true
		return refResponse, nil
	}
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil {
		t.interrupt(d, "body")
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
		t.interrupt(d, "receipt")
		return nil, fmt.Errorf("%w: durable receipt unavailable", ErrDispatchPending)
	}
	return attachReceipt(response(req, r), t.repository, d, r.ID, false), nil
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
// its intent pending until the new owner supersedes it after the grace period.
type dispatchStream struct {
	io.ReadCloser
	once      sync.Once
	finish    func() error
	interrupt func()
	err       error
}

func (s *dispatchStream) Close() error {
	s.once.Do(func() {
		s.err = s.ReadCloser.Close()
		if s.err == nil {
			s.err = s.finish()
		} else if s.interrupt != nil {
			s.interrupt()
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

func (t *dispatchTransport) interrupt(d Dispatch, reason string) {
	t.markWaiting(d.Owner, d.Lane, true)
	c, done := context.WithTimeout(context.Background(), canonical.Deadline)
	defer done()
	_ = t.repository.InterruptDispatch(c, d, reason)
}

// A counter of previous waits must not keep a recovery alert firing after
// acquisition succeeds. Aggregate current waits across lanes of this source.
func (t *dispatchTransport) markWaiting(owner canonical.Ownership, lane string, waiting bool) {
	t.waitMu.Lock()
	defer t.waitMu.Unlock()
	if owner.Epoch < t.waitEpoch {
		return
	}
	if owner.Epoch > t.waitEpoch || t.waiting == nil {
		t.waitEpoch = owner.Epoch
		t.waiting = map[string]bool{}
	}
	t.waiting[lane] = waiting
	value := float64(0)
	for _, pending := range t.waiting {
		if pending {
			value = 1
			break
		}
	}
	platform.Gauge("acquisition_"+strings.ToLower(t.source)+"_dispatch_waiting", value)
}

type receiptReferenceKey struct{}
type receiptReference struct {
	repository DispatchRepository
	dispatch   Dispatch
	overflow   bool
	settled    atomic.Bool
}

func attachReceipt(resp *http.Response, repository DispatchRepository, d Dispatch, id string, overflow bool) *http.Response {
	d.ID = id
	ref := &receiptReference{repository: repository, dispatch: d, overflow: overflow}
	resp.Request = resp.Request.WithContext(context.WithValue(resp.Request.Context(), receiptReferenceKey{}, ref))
	return resp
}
func responseReference(resp *http.Response) *receiptReference {
	if resp == nil || resp.Request == nil {
		return nil
	}
	ref, _ := resp.Request.Context().Value(receiptReferenceKey{}).(*receiptReference)
	return ref
}
func ResponseOverflow(resp *http.Response) bool {
	ref := responseReference(resp)
	return ref != nil && ref.overflow
}
func ResponseID(resp *http.Response) string {
	ref := responseReference(resp)
	if ref == nil {
		return ""
	}
	return ref.dispatch.ID
}

// QuarantineResponse preserves bytes first. A failed or uncertain retention
// commit must not invalidate the cached response or authorize a fresh read.
func QuarantineResponse(ctx context.Context, resp *http.Response, retain func() error) error {
	if retain == nil {
		return errors.New("durable response retention required")
	}
	if err := retain(); err != nil {
		return err
	}
	return finishResponse(ctx, resp, "quarantined")
}

// ConsumeResponse is called only after the input-owning store confirms capture.
func ConsumeResponse(ctx context.Context, resp *http.Response) error {
	return finishResponse(ctx, resp, "consumed")
}
func finishResponse(ctx context.Context, resp *http.Response, state string) error {
	ref := responseReference(resp)
	if ref == nil {
		return nil
	}
	owner, err := Token(ctx, strings.TrimPrefix(ref.dispatch.Owner.Scope, "acquisition:"))
	if err != nil {
		return err
	}
	d := ref.dispatch
	d.Owner = owner
	if err = ref.repository.FinishResponse(ctx, d, state); err != nil {
		return err
	}
	ref.settled.Store(true)
	return nil
}

type overflowBody struct {
	io.Reader
	source    io.ReadCloser
	cancel    context.CancelFunc
	ref       *receiptReference
	interrupt func()
	once      sync.Once
	err       error
}

func (b *overflowBody) Close() error {
	b.once.Do(func() {
		b.err = b.source.Close()
		b.cancel()
		if !b.ref.settled.Load() {
			b.interrupt()
		}
	})
	return b.err
}
