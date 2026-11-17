package coastal

import (
	"aggregator/internal/acquisition"
	"aggregator/internal/ingestion"
	"aggregator/internal/middleware"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const (
	Source         = "SENSOR_PANTAI"
	MaxObservation = 64 << 10
)

type Acquirer struct {
	Client     *Client
	Progress   *ingestion.SourceStore
	Sender     rawinput.Sender
	Start      int64
	PageSize   int
	Interval   time.Duration
	Status     *Status
	Logger     *slog.Logger
	Repository *Repository
}

func (a *Acquirer) Run(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		more, err := a.Step(ctx)
		if ctx.Err() != nil {
			return
		}
		if a.Repository == nil {
			a.Status.Attempt(err)
		}
		delay := a.Interval
		switch {
		case err != nil:
			failures++
			delay = rawinput.Backoff(failures)
			if errors.Is(err, ErrOpen) {
				delay = max(delay, a.Interval)
			}
			a.Logger.Warn("coastal acquisition pending", "error", err, "failures", failures)
		case more:
			failures, delay = 0, 0
		default:
			failures = 0
		}
		if delay > 0 && !rawinput.Sleep(ctx, delay) {
			return
		}
	}
}

func (a *Acquirer) Step(ctx context.Context) (more bool, retErr error) {
	owner, err := acquisition.Token(ctx, Source)
	if err != nil {
		return false, err
	}
	if a.Repository != nil {
		defer func() {
			now, err := a.Repository.Now(ctx)
			if err == nil {
				a.Status.AttemptAt(now, retErr)
				err = a.Repository.SaveStatus(ctx, owner, a.Status.SnapshotAt(now))
			}
			if err != nil {
				retErr = errors.Join(retErr, err)
			}
		}()
	}
	progress, err := a.Progress.Progress(ctx, a.Start)
	if err != nil {
		return false, err
	}
	cid := uuid.NewString()
	ctx, span := platform.Start(middleware.WithCorrelationID(ctx, cid), "acquire SENSOR_PANTAI")
	defer span.End()
	page, err := a.Client.FetchObservations(ctx, progress.Cursor, a.PageSize)
	if err != nil {
		return false, err
	}
	received := time.Now().UTC()
	if a.Repository != nil {
		received, err = a.Repository.Now(ctx)
		if err != nil {
			return false, err
		}
	}
	trace := http.Header{}
	platform.Inject(ctx, trace)
	var traceparent *string
	if v := trace.Get("Traceparent"); v != "" {
		traceparent = &v
	}
	var clock *string
	sensorClock, clockErr := time.Parse(time.RFC3339Nano, page.GeneratedAt)
	if _, offset := sensorClock.Zone(); clockErr == nil && offset == 0 {
		clock = &page.GeneratedAt
	}
	var latest time.Time
	accepted := make([]rawinput.Envelope, 0, len(page.Items))
	for i, raw := range page.Items {
		position := progress.Position + int64(i)
		e := rawinput.Envelope{Version: 1, Source: Source, Namespace: a.Progress.Namespace, Operation: "create", Position: &position, Cursor: progress.Cursor, ReceivedAt: received.Format(time.RFC3339Nano), VirtualNow: clock, Payload: raw, Size: int64(len(raw)), Digest: rawinput.Hash(raw), ContentType: "application/json", CorrelationID: cid, Traceparent: traceparent, OwnershipScope: owner.Scope, OwnershipEpoch: owner.Epoch, OwnershipHolder: owner.Holder}
		if id, ok := Identify(raw); ok {
			e.Ref = &id
		}
		if o, err := Parse(raw, nil, 0); err == nil {
			at := o.ObservedAt.UTC().Format(time.RFC3339Nano)
			e.EventTime = &at
			if o.ObservedAt.After(latest) {
				latest = o.ObservedAt
			}
		}
		e.MessageID = rawinput.ID(e)
		if len(raw) > MaxObservation {
			if a.Repository != nil {
				if err = a.Repository.Expect(ctx, owner, e); err != nil {
					return false, err
				}
			}
			if err = a.Progress.Quarantine(ctx, e, raw, "observation exceeds the accepted size"); err != nil {
				return false, err
			}
			if a.Repository != nil {
				if err = a.Repository.Reject(ctx, e, "observation exceeds the accepted size"); err != nil {
					return false, err
				}
			}
			platform.Add("coastal_observations_quarantined_total", 1)
			continue
		}
		accepted = append(accepted, e)
	}
	accepted, err = a.Progress.PreserveBatch(ctx, owner, accepted)
	if err != nil {
		return false, err
	}
	for _, e := range accepted {
		if a.Repository != nil {
			if err = a.Repository.Expect(ctx, owner, e); err != nil {
				return false, err
			}
		}
		if err = a.Sender.Send(ctx, rawinput.Coastal.Subject, e); err != nil {
			return false, err
		}
	}
	next := progress.Cursor
	if page.NextCursor != nil && *page.NextCursor != "" {
		next = page.NextCursor
	}
	if len(page.Items) > 0 || !same(next, progress.Cursor) {
		if err = a.Progress.Advance(ctx, owner, ingestion.Progress{Position: progress.Position + int64(len(page.Items)), Cursor: next}); err != nil {
			return false, err
		}
	}
	platform.Add("coastal_observations_acquired_total", float64(len(page.Items)))
	if clockErr != nil {
		sensorClock = time.Time{}
	}
	a.Status.PageAt(received, sensorClock, latest, len(page.Items))
	return len(page.Items) >= a.PageSize, nil
}

func same(a, b *string) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
