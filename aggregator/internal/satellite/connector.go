package satellite

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/canonical"
	"aggregator/internal/ingestion"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	Source      = "SATELIT"
	ContentType = "application/vnd.bnpb.satellite-fixture+json;version=1"
)

type GapError struct{ Expected, Got int64 }

func (e *GapError) Error() string {
	return fmt.Sprintf("feed skipped from offset %d to %d", e.Expected, e.Got)
}

type RetryAfter struct {
	Status int
	Delay  time.Duration
}

func (e *RetryAfter) Error() string {
	return fmt.Sprintf("feed returned %d; retry after %s", e.Status, e.Delay)
}

type Connector struct {
	BaseURL       string
	Token         string
	HTTP          *http.Client
	Progress      *ingestion.SourceStore
	Sender        rawinput.Sender
	Start         int64
	IdleTimeout   time.Duration
	FlushEvery    int
	FlushAfter    time.Duration
	MaxEventSize  int
	ExclusiveFrom bool
	Logger        *slog.Logger

	published atomic.Int64
	latest    atomic.Int64
}

func (c *Connector) Run(ctx context.Context) {
	go c.watchSession(ctx)
	failures := 0
	for ctx.Err() == nil {
		err := c.connect(ctx)
		if ctx.Err() != nil {
			return
		}
		delay := time.Duration(0)
		var retry *RetryAfter
		var gap *GapError
		switch {
		case errors.As(err, &retry):
			failures++
			delay = max(retry.Delay, rawinput.Backoff(failures))
		case errors.As(err, &gap):
			platform.Add("satellite_feed_gaps_total", 1)
			delay = rawinput.Backoff(1)
		case err != nil:
			failures++
			delay = rawinput.Backoff(failures)
		default:
			failures = 0
		}
		platform.Add("satellite_feed_reconnects_total", 1)
		c.Logger.Warn("satellite feed reconnecting", "error", err, "delay", delay)
		if delay > 0 && !rawinput.Sleep(ctx, delay) {
			return
		}
	}
}

func (c *Connector) request(ctx context.Context, path string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.BaseURL, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("User-Agent", "aat-m2-satellite/1.0")
	return req, nil
}

func retryDelay(resp *http.Response) time.Duration {
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s >= 0 {
		return time.Duration(s) * time.Second
	}
	return time.Second
}

func (c *Connector) connect(ctx context.Context) error {
	owner, err := acquisition.Token(ctx, Source)
	if err != nil {
		return err
	}
	progress, err := c.Progress.Progress(ctx, c.Start)
	if err != nil {
		return err
	}
	next := progress.Position
	c.published.Store(next)
	stream, cancel := context.WithCancel(ctx)
	defer cancel()
	from := next
	if c.ExclusiveFrom {
		from--
	}
	req, err := c.request(stream, "/v1/stream?from="+strconv.FormatInt(from, 10))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable:
		return &RetryAfter{Status: resp.StatusCode, Delay: retryDelay(resp)}
	case resp.StatusCode != http.StatusOK:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("feed returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	idle := time.AfterFunc(c.IdleTimeout, cancel)
	defer idle.Stop()
	events := NewStream(resp.Body, c.MaxEventSize)
	cid := uuid.NewString()
	ctx = middleware.WithCorrelationID(ctx, cid)
	flushed, pending := next, 0
	lastFlush := time.Now()
	flush := func() error {
		if next == flushed {
			return nil
		}
		call, done := context.WithTimeout(context.WithoutCancel(ctx), canonical.Deadline)
		defer done()
		if err := c.Progress.Advance(call, owner, ingestion.Progress{Position: next}); err != nil {
			return err
		}
		flushed, pending, lastFlush = next, 0, time.Now()
		return nil
	}
	defer func() { _ = flush() }()
	batch := make([]rawinput.Envelope, 0, batchSize)
	publish := func() error {
		if len(batch) == 0 {
			return nil
		}
		kept, err := c.Progress.PreserveBatch(ctx, owner, batch)
		if err != nil {
			return err
		}
		for _, env := range kept {
			if err = c.Sender.Send(ctx, rawinput.Satellite.Subject, env); err != nil {
				return err
			}
			next++
			pending++
			c.published.Store(next)
		}
		platform.Add("satellite_events_published_total", float64(len(kept)))
		c.reportLag()
		batch = batch[:0]
		if pending >= c.FlushEvery || time.Since(lastFlush) >= c.FlushAfter {
			return flush()
		}
		return nil
	}
	expected := next
	for {
		e, err := events.Next()
		if err != nil {
			if stream.Err() != nil && ctx.Err() == nil {
				err = fmt.Errorf("feed idle for %s", c.IdleTimeout)
			}
			return errors.Join(err, publish())
		}
		idle.Reset(c.IdleTimeout)
		if _, err = acquisition.Token(ctx, Source); err != nil {
			return err
		}
		n, normErr := Normalize(c.Progress.Namespace, e)
		switch {
		case n.Offset < 0 && e.Type == "heartbeat":
			platform.Add("satellite_feed_keepalives_total", 1)
		case n.Offset < 0:
			return errors.Join(normErr, publish())
		case n.Offset < expected:
			platform.Add("satellite_feed_duplicates_total", 1)
		case n.Offset > expected:
			return errors.Join(&GapError{Expected: expected, Got: n.Offset}, publish())
		default:
			env, err := c.envelope(ctx, owner, e, n, normErr)
			if err != nil {
				return errors.Join(err, publish())
			}
			batch = append(batch, env)
			expected++
		}
		if len(batch) >= batchSize || events.Buffered() == 0 {
			if err = publish(); err != nil {
				return err
			}
		}
	}
}

const batchSize = 64

func (c *Connector) envelope(ctx context.Context, owner canonical.Ownership, e Event, n Normalized, normErr error) (rawinput.Envelope, error) {
	received := time.Now().UTC().Format(time.RFC3339Nano)
	position := n.Offset
	env := rawinput.Envelope{Version: 1, Source: Source, Namespace: c.Progress.Namespace, Operation: n.Kind, Position: &position, ReceivedAt: received, ContentType: ContentType, CorrelationID: middleware.CorrelationFromContext(ctx), OwnershipScope: owner.Scope, OwnershipEpoch: owner.Epoch, OwnershipHolder: owner.Holder}
	operation := n.Operation
	if normErr != nil {
		env.Operation = "heartbeat"
		placeholder, err := json.Marshal(map[string]any{"version": 1, "namespace": env.Namespace, "offset": position, "kind": "heartbeat", "payload": e.Data})
		if err != nil {
			return env, err
		}
		operation = placeholder
		quarantined := env
		quarantined.ContentType = "text/event-stream"
		quarantined.Payload, quarantined.Size, quarantined.Digest = e.Data, int64(len(e.Data)), rawinput.Hash(e.Data)
		quarantined.MessageID = rawinput.Identity("raw-v1", Source, env.Namespace, position, "quarantine", e.Type)
		if err = c.Progress.Quarantine(ctx, quarantined, e.Data, normErr.Error()); err != nil {
			return env, err
		}
		platform.Add("satellite_events_quarantined_total", 1)
	}
	if normErr == nil && n.VirtualNow != nil {
		v := n.VirtualNow.Format(time.RFC3339Nano)
		env.VirtualNow = &v
	}
	env.Payload, env.Size, env.Digest = operation, int64(len(operation)), rawinput.Hash(operation)
	env.MessageID = rawinput.ID(env)
	return env, nil
}

func (c *Connector) watchSession(ctx context.Context) {
	for {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		if req, err := c.request(call, "/v1/session"); err == nil {
			if resp, err := c.HTTP.Do(req); err == nil {
				var s struct {
					Latest int64 `json:"latest_offset"`
				}
				if resp.StatusCode == http.StatusOK && json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&s) == nil {
					c.latest.Store(s.Latest)
					platform.Gauge("satellite_feed_latest_offset", float64(s.Latest))
					c.reportLag()
				}
				resp.Body.Close()
			}
		}
		cancel()
		if !rawinput.Sleep(ctx, 5*time.Second) {
			return
		}
	}
}

func (c *Connector) reportLag() {
	platform.Gauge("satellite_feed_lag_offsets", float64(max(c.latest.Load()-c.published.Load()+1, 0)))
}
