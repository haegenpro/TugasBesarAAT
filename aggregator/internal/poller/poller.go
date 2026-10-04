// Package poller runs the BMKG and PVMBG polling loops. Each source has its
// own independent goroutine/ticker — a slow or failing PVMBG call never
// delays or blocks BMKG polling, and internal API reads never wait on a
// polling cycle since they read straight from the Store.
package poller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/mapping"
	"aggregator/internal/volcano"
)

// HazardSaver is the subset of the Store the poller needs, kept as an
// interface so BMKG/PVMBG polling and dedup/concurrency behavior can be
// tested with httptest servers and a fake, without a real Postgres.
type HazardSaver interface {
	SaveDedup(ctx context.Context, e domain.HazardEvent, correlationID string) (inserted bool, err error)
}

type Poller struct {
	BMKG     *clients.BMKGClient
	PVMBG    *clients.PVMBGClient
	Store    HazardSaver
	Lookup   volcano.Lookuper
	Status   *PVMBGStatus
	Ready    func() bool
	Interval time.Duration
	Logger   *slog.Logger
}

// Run starts both pollers as independent goroutines and blocks until ctx is
// cancelled.
func (p *Poller) Run(ctx context.Context) {
	go p.loop(ctx, "bmkg", p.PollBMKGOnce)
	go p.loop(ctx, "pvmbg", p.PollPVMBGOnce)
	<-ctx.Done()
}

func (p *Poller) loop(ctx context.Context, name string, fn func(context.Context)) {
	if p.Ready == nil || p.Ready() {
		fn(ctx) // poll immediately on startup, then on each tick
	}
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.Ready == nil || p.Ready() {
				fn(ctx)
			}
		}
	}
}

func (p *Poller) PollBMKGOnce(ctx context.Context) {
	cid := uuid.NewString()

	events, err := p.BMKG.FetchSeismicEvents(ctx, cid, time.Time{})
	if err != nil {
		p.logSourceFailure("bmkg", cid, err)
		return
	}
	warnings, err := p.BMKG.FetchTsunamiWarnings(ctx, cid, time.Time{})
	if err != nil {
		p.logSourceFailure("bmkg", cid, err)
		return
	}

	inserted := 0
	for _, ev := range events {
		hazard := mapping.SeismicToHazard(ev, warnings)
		p.saveAndQueue(ctx, hazard, cid, &inserted)
	}
	p.Logger.Info("polled bmkg", "service", "aggregator", "correlation_id", cid,
		"fetched", len(events), "new_hazards", inserted)
}

func (p *Poller) PollPVMBGOnce(ctx context.Context) {
	cid := uuid.NewString()

	reports, err := p.PVMBG.FetchVolcanicReports(ctx, cid, time.Time{})
	if err != nil {
		// A PVMBG outage/timeout is a handled, expected degradation: warn,
		// not error. BMKG polling above is entirely unaffected by this.
		p.Status.MarkUnhealthy(domain.Now())
		p.logSourceFailure("pvmbg", cid, err)
		return
	}
	p.Status.MarkHealthy()

	inserted := 0
	for _, r := range reports {
		hazard, err := mapping.VolcanicToHazard(ctx, p.Lookup, r)
		if err != nil {
			p.Logger.Error("volcanic mapping failed", "service", "aggregator", "correlation_id", cid, "error", err.Error())
			continue
		}
		p.saveAndQueue(ctx, hazard, cid, &inserted)
	}
	p.Logger.Info("polled pvmbg", "service", "aggregator", "correlation_id", cid,
		"fetched", len(reports), "new_hazards", inserted)
}

func (p *Poller) logSourceFailure(source, correlationID string, err error) {
	if errors.Is(err, clients.ErrContractViolation) {
		p.Logger.Error("source contract violation; poll rejected", "service", "aggregator", "source", source, "correlation_id", correlationID, "error", err.Error())
		return
	}
	p.Logger.Warn("source unavailable; poll skipped", "service", "aggregator", "source", source, "correlation_id", correlationID, "error", err.Error())
}

// saveAndQueue deduplicates on (source, source_ref_id). The Store atomically
// queues a new insert for the outbox dispatcher to publish.
func (p *Poller) saveAndQueue(ctx context.Context, hazard domain.HazardEvent, cid string, inserted *int) {
	wasInserted, err := p.Store.SaveDedup(ctx, hazard, cid)
	if err != nil {
		p.Logger.Error("store save failed", "service", "aggregator", "correlation_id", cid, "error", err.Error())
		return
	}
	if !wasInserted {
		return
	}
	*inserted++
}
