// Package pipeline wires durable raw handoff to the storage owner's processing
// boundary. It never gives ingestion credentials for canonical tables.
package pipeline

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/canonical"
	"aggregator/internal/middleware"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"aggregator/internal/wildfireprocessor"
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type Repository interface {
	rawinput.History
	PrepareBMKG(context.Context, canonical.Ownership, string, *avrowire.Client, map[string]*avrowire.SourceReader) (bool, error)
	ApplyInputOutput(context.Context, canonical.Ownership, canonical.Ownership, string) (bool, error)
}
type Worker struct {
	Store               Repository
	Registry            *avrowire.Client
	Reader              *avrowire.Reader
	Readers             map[string]*avrowire.SourceReader
	Processor, Delivery canonical.OwnershipRepository
	JS                  jetstream.JetStream
	Logger              *slog.Logger
	BeforeACK           func(context.Context) error
	Wildfire            *wildfireprocessor.Engine
}

func (w *Worker) Run(ctx context.Context, source, namespace string) {
	ownership.Run(ctx, w.Processor, "processor:"+source, func(owned context.Context, p canonical.Ownership) {
		if source == "SATELIT" && w.Wildfire != nil {
			var workers sync.WaitGroup
			workers.Add(2)
			go func() { defer workers.Done(); w.Wildfire.RunPreparation(owned, p, w.Logger) }()
			go func() {
				defer workers.Done()
				ownership.Run(owned, w.Delivery, "canonical:SATELIT", func(work context.Context, d canonical.Ownership) { w.Wildfire.RunDelivery(work, p, d, w.Logger) })
			}()
			w.loop(owned, source, namespace, p, canonical.Ownership{})
			workers.Wait()
			return
		}
		if source == "BMKG" {
			ownership.Run(owned, w.Delivery, "canonical:BMKG", func(work context.Context, d canonical.Ownership) { w.loop(work, source, namespace, p, d) })
		} else {
			w.loop(owned, source, namespace, p, canonical.Ownership{})
		}
	})
}
func (w *Worker) loop(ctx context.Context, source, namespace string, p, d canonical.Ownership) {
	spec := rawinput.Satellite
	if source == "BMKG" {
		spec = rawinput.BMKG
	}
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, canonical.Deadline)
		consumer, err := rawinput.Ensure(call, w.JS, spec, 3)
		cancel()
		if err == nil {
			err = w.cycle(ctx, consumer, source, namespace, p, d)
		}
		if err != nil && w.Logger != nil {
			w.Logger.Warn("raw processing pending", "source", source, "error", err)
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (w *Worker) cycle(ctx context.Context, c jetstream.Consumer, source, namespace string, p, d canonical.Ownership) error {
	// A one-message pull deliberately makes source order observable and bounds
	// held deliveries. The durable server consumer still enforces its 32 cap.
	messages, err := c.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return err
	}
	for msg := range messages.Messages() {
		if err = w.Handoff(ctx, msg, source, namespace, p); err != nil {
			return err
		}
	}
	if err = messages.Error(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if source == "BMKG" {
		call, cancel := context.WithTimeout(ctx, canonical.Deadline)
		_, err = w.Store.ApplyInputOutput(call, p, d, namespace)
		cancel()
		if err != nil {
			return err
		}
		call, cancel = context.WithTimeout(ctx, canonical.Deadline)
		_, err = w.Store.PrepareBMKG(call, p, namespace, w.Registry, w.Readers)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}
func (w *Worker) Handoff(ctx context.Context, msg jetstream.Msg, source, namespace string, p canonical.Ownership) error {
	call, cancel := context.WithTimeout(ctx, canonical.Deadline)
	defer cancel()
	e, err := rawinput.Decode(call, w.Registry, w.Reader, msg.Data())
	if err != nil {
		return err
	}
	if e.Source != source || e.Namespace != namespace {
		return fmt.Errorf("unconfigured raw source/session; retained in broker")
	}
	if e.Traceparent != nil {
		call = platform.Extract(call, http.Header{"Traceparent": []string{*e.Traceparent}})
	}
	call = middleware.WithCorrelationID(call, e.CorrelationID)
	call, span := platform.Start(call, "durable raw handoff")
	defer span.End()
	_, err = w.Store.AppendHistory(call, rawinput.AppendRequest{Version: 1, Ownership: p, Messages: []rawinput.Envelope{e}})
	if err != nil {
		return err
	}
	if source == "SATELIT" {
		after := *e.Position - 1
		var a *int64
		if after >= 0 {
			a = &after
		}
		page, err := w.Store.ReadHistory(call, namespace, a, 1)
		if err != nil {
			return err
		}
		if len(page.Items) != 1 || rawinput.Fingerprint(page.Items[0]) != rawinput.Fingerprint(e) {
			return fmt.Errorf("durable replay proof unavailable; transport retained")
		}
	}
	if w.BeforeACK != nil {
		if err = w.BeforeACK(call); err != nil {
			return err
		}
	}
	// DoubleAck waits for server confirmation. A lost reply repeats the durable
	// append and returns duplicate; receipt is independent of canonical backlog.
	return msg.DoubleAck(call)
}
