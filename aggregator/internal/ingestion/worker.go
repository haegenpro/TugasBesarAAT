package ingestion

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/avrowire"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Worker struct {
	Store           *Store
	Registry        *avrowire.Client
	SchemaText      string
	Readers         map[string]*avrowire.SourceReader
	JS              jetstream.JetStream
	HTTP            *http.Client
	BaseURL, APIKey string
	Interval        time.Duration
	Logger          *slog.Logger
	AfterPublish    func(context.Context) error
}

func (w *Worker) Step(ctx context.Context) error {
	owner, err := acquisition.Token(ctx, "BMKG")
	if err != nil {
		return err
	}
	pending, err := w.Store.Next(ctx)
	if err != nil {
		return err
	}
	if pending == nil {
		task, err := w.Store.Task(ctx)
		if err != nil {
			return err
		}
		entity := []string{"seismic-events", "tsunami-warnings", "seismic-events", "tsunami-warnings"}[task]
		meta := rawinput.CaptureCursor{Version: 1, Entity: entity, History: task < 2, Full: task >= 2, RequestStarted: time.Now().UTC()}
		path := "/" + entity
		if meta.History {
			meta.After, err = w.Store.Progress(ctx, entity)
			if err != nil {
				return err
			}
			path = "/history?entity=" + url.QueryEscape(entity) + "&after=" + strconv.FormatInt(meta.After, 10) + "&limit=100"
		}
		if err = acquisition.RequireRequest(ctx, "BMKG"); err != nil {
			return err
		}
		cid := uuid.NewString()
		call, span := platform.Start(middleware.WithCorrelationID(ctx, cid), "raw BMKG acquisition")
		defer span.End()
		req, err := http.NewRequestWithContext(call, http.MethodGet, strings.TrimRight(w.BaseURL, "/")+path, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", avrowire.MediaType)
		req.Header.Set("X-BMKG-Key", w.APIKey)
		req.Header.Set("X-Correlation-ID", cid)
		resp, err := w.HTTP.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("BMKG response %d; source progress unchanged", resp.StatusCode)
		}
		if v := resp.Header.Get("X-Source-Sequence"); v != "" {
			meta.SourceSequence, err = strconv.ParseInt(v, 10, 64)
			if err != nil || meta.SourceSequence < 0 {
				meta.SourceSequence = 0
			}
		}
		h := http.Header{}
		platform.Inject(call, h)
		var tp *string
		if v := h.Get("Traceparent"); v != "" {
			tp = &v
		}
		pending, err = w.Store.Capture(call, owner, CaptureContext{Cursor: meta, ReceivedAt: ReceivedNow(), CorrelationID: cid, Traceparent: tp, Owner: owner, HTTPStatus: resp.StatusCode, MediaType: resp.Header.Get("Content-Type"), SourceSequenceHeader: resp.Header.Get("X-Source-Sequence")}, resp.Body)
		if err != nil {
			return err
		}
	}
	if pending.Envelope == nil {
		if err := w.Store.Runnable(ctx, *pending); err != nil {
			return err
		}
		if pending.Context.MediaType != avrowire.MediaType {
			err := fmt.Errorf("BMKG captured response is not registered Avro")
			_ = w.Store.Failure(ctx, owner, *pending, err.Error(), false)
			return err
		}
		if !pending.Context.Cursor.History {
			n, err := strconv.ParseInt(pending.Context.SourceSequenceHeader, 10, 64)
			if err != nil || n < 0 {
				err = fmt.Errorf("BMKG live source sequence missing or invalid")
				_ = w.Store.Failure(ctx, owner, *pending, err.Error(), false)
				return err
			}
		}
		bytes, err := w.Store.Payload(ctx, *pending)
		if err != nil {
			return err
		}
		_, meta, err := rawinput.DecodeBMKG(ctx, w.Registry, w.Readers[pending.Context.Cursor.Entity], pending.Context.Cursor, bytes)
		if err != nil {
			_ = w.Store.Failure(ctx, owner, *pending, err.Error(), errors.Is(err, avrowire.ErrRegistryUnavailable) || ctx.Err() != nil)
			return err
		}
		cur := meta.Encode()
		e := rawinput.Envelope{Version: 1, Source: "BMKG", Namespace: w.Store.Namespace, Operation: "update", Position: &pending.Position, Cursor: &cur, ReceivedAt: pending.Context.ReceivedAt, Payload: bytes, Size: pending.Size, Digest: pending.Digest, ContentType: avrowire.MediaType, SchemaID: rawinput.SchemaNumber(bytes), CorrelationID: pending.Context.CorrelationID, Traceparent: pending.Context.Traceparent, OwnershipScope: pending.Context.Owner.Scope, OwnershipEpoch: pending.Context.Owner.Epoch, OwnershipHolder: pending.Context.Owner.Holder}
		// Leave room for envelope overhead and framing; oversized bytes stay pinned
		// in the ingestion-owned store for handoff/replay throughout the session.
		if len(bytes) > 768<<10 {
			e.Payload = nil
			e.PayloadRef = &pending.PayloadID
		}
		e.MessageID = rawinput.ID(e)
		if err = w.Store.Finalize(ctx, owner, *pending, e); err != nil {
			return err
		}
		pending.Envelope = &e
	}
	schema, err := w.Registry.Register(ctx, rawinput.Subject, w.SchemaText)
	if err != nil {
		_ = w.Store.Failure(ctx, owner, *pending, err.Error(), true)
		return err
	}
	b, err := rawinput.Encode(schema, *pending.Envelope)
	if err != nil {
		return err
	}
	if len(b) > rawinput.MaxMessage {
		return fmt.Errorf("raw envelope exceeds broker bound")
	}
	msg := &nats.Msg{Subject: rawinput.BMKG.Subject, Data: b, Header: nats.Header{}}
	msg.Header.Set("Nats-Msg-Id", pending.Envelope.MessageID)
	msg.Header.Set("X-Correlation-ID", pending.Envelope.CorrelationID)
	if pending.Envelope.Traceparent != nil {
		msg.Header.Set("Traceparent", *pending.Envelope.Traceparent)
	}
	if _, err = w.JS.PublishMsg(ctx, msg); err != nil {
		_ = w.Store.Failure(ctx, owner, *pending, err.Error(), true)
		return err
	}
	if w.AfterPublish != nil {
		if err = w.AfterPublish(ctx); err != nil {
			return err
		}
	}
	return w.Store.Published(ctx, owner, *pending, *pending.Envelope)
}
func (w *Worker) Run(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := w.Step(call)
		cancel()
		delay := w.Interval
		if err != nil {
			failures++
			cap := 250 * time.Millisecond
			for i := 1; i < failures && cap < 30*time.Second; i++ {
				cap *= 2
			}
			if cap > 30*time.Second {
				cap = 30 * time.Second
			}
			delay = time.Duration(rand.Int64N(int64(cap) + 1))
			if w.Logger != nil {
				w.Logger.Warn("raw ingestion pending", "error", err)
			}
		} else {
			failures = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// HTTPResolver accepts only a configured service base and opaque ID. It does
// not allow an envelope to select a host, filesystem path or credentials.
type HTTPResolver struct {
	Client         *http.Client
	BaseURL, Token string
}

func (r HTTPResolver) Open(ctx context.Context, id string) (io.ReadCloser, error) {
	if !rawinput.Opaque(id) {
		return nil, fmt.Errorf("invalid payload identity")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(r.BaseURL, "/")+"/internal/ingestion/payloads/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+r.Token)
	resp, err := r.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, fmt.Errorf("payload owner unavailable")
	}
	return resp.Body, nil
}
