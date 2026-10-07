// Package poller runs independent live and replay loops. Reads never wait on ingestion.
package poller

import (
	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/mapping"
	"aggregator/internal/volcano"
	"context"
	"errors"
	"github.com/google/uuid"
	"log/slog"
	"time"
)

type HazardSaver interface {
	SaveDedup(context.Context, domain.HazardEvent, string) (bool, error)
}
type RecoveryStore interface {
	CommitSeismic(context.Context, []clients.SeismicEvent, string, int64) error
	CommitWarnings(context.Context, []clients.TsunamiWarning, string, int64, time.Time, bool) error
	CommitVolcanic(context.Context, []clients.VolcanicReport, []domain.HazardEvent, string, int64) error
	HistoryCursor(context.Context, string, string) (int64, error)
	PollStatus(context.Context, string) (domain.EndpointStatus, error)
	RecordAttempt(context.Context, string, string, bool, time.Duration, time.Duration) error
}
type Poller struct {
	BMKG       *clients.BMKGClient
	PVMBG      *clients.PVMBGClient
	Store      HazardSaver
	Lookup     volcano.Lookuper
	Status     *PVMBGStatus
	Ready      func() bool
	Interval   time.Duration
	MaxBackoff time.Duration
	Logger     *slog.Logger
}

func (p *Poller) Run(ctx context.Context) {
	go p.loop(ctx, "BMKG", p.pollSeismic)
	go p.loop(ctx, "BMKG_TSUNAMI", p.pollWarnings)
	go p.loop(ctx, "PVMBG", p.pollVolcanic)
	if _, ok := p.Store.(interface {
		PendingVolcanic(context.Context) ([]clients.VolcanicReport, error)
	}); ok {
		go p.loop(ctx, "mapping:volcanic", p.retryVolcanicMapping)
	}
	if _, ok := p.Store.(RecoveryStore); ok {
		for _, entity := range []string{"seismic-events", "tsunami-warnings", "volcanic-reports"} {
			entity := entity
			go p.loop(ctx, "history:"+entity, func(ctx context.Context, cid string) error { return p.replay(ctx, cid, entity) })
		}
	}
	<-ctx.Done()
}
func (p *Poller) loop(ctx context.Context, endpoint string, fn func(context.Context, string) error) {
	interval := p.Interval
	maxBackoff := p.MaxBackoff
	if interval <= 0 || maxBackoff < interval {
		p.logSourceFailure(endpoint, "", errors.New("polling interval must be positive and retry maximum must cover it"))
		return
	}
	failures := 0
	for ctx.Err() == nil {
		delay := interval
		if p.Ready == nil || p.Ready() {
			if st, ok := p.Store.(RecoveryStore); ok {
				status, err := st.PollStatus(ctx, endpoint)
				if err != nil {
					p.logSourceFailure(endpoint, "", err)
				} else if status.NextAttempt != nil && status.NextAttempt.After(time.Now()) {
					if !sleep(ctx, time.Until(*status.NextAttempt)) {
						return
					}
					continue
				}
			}
			cid := uuid.NewString()
			err := fn(ctx, cid)
			if err != nil {
				p.logSourceFailure(endpoint, cid, err)
				failures++
			} else {
				failures = 0
			}
			if st, ok := p.Store.(RecoveryStore); ok {
				if e := st.RecordAttempt(ctx, endpoint, cid, err == nil, interval, maxBackoff); e != nil {
					p.logSourceFailure(endpoint, cid, e)
				}
			}
			for i := 1; i < failures && delay < maxBackoff; i++ {
				delay *= 2
			}
			if delay > maxBackoff {
				delay = maxBackoff
			}
		}
		if !sleep(ctx, delay) {
			return
		}
	}
}
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (p *Poller) pollSeismic(ctx context.Context, cid string) error {
	events, err := p.BMKG.FetchSeismicEvents(ctx, cid, time.Time{})
	if err != nil {
		return err
	}
	if st, ok := p.Store.(RecoveryStore); ok {
		return st.CommitSeismic(ctx, events, cid, 0)
	}
	// Minimal saver is used by standalone mapping tests. Potential events require assessment.
	for _, e := range events {
		if !e.PotentialTsunami {
			if _, err = p.Store.SaveDedup(ctx, mapping.SeismicToHazard(e, nil), cid); err != nil {
				return err
			}
		}
	}
	return nil
}
func (p *Poller) pollWarnings(ctx context.Context, cid string) error {
	started := domain.Now()
	warnings, err := p.BMKG.FetchTsunamiWarnings(ctx, cid, time.Time{})
	if err != nil {
		return err
	}
	if st, ok := p.Store.(RecoveryStore); ok {
		// Live absence is authoritative only after the durable warning archive has caught up.
		cursor, err := st.HistoryCursor(ctx, "BMKG", "tsunami-warnings")
		if err != nil {
			return err
		}
		page, err := p.BMKG.FetchHistory(ctx, cid, "tsunami-warnings", cursor)
		complete := err == nil && !page.HasMore && len(page.Data) == 0
		return st.CommitWarnings(ctx, warnings, cid, 0, started, complete)
	}
	return nil
}
func (p *Poller) pollVolcanic(ctx context.Context, cid string) error {
	reports, err := p.PVMBG.FetchVolcanicReports(ctx, cid, time.Time{})
	if err != nil {
		if p.Status != nil {
			p.Status.MarkUnhealthy(domain.Now())
		}
		return err
	}
	hazards := make([]domain.HazardEvent, 0, len(reports))
	for _, r := range reports {
		h, err := mapping.VolcanicToHazard(ctx, p.Lookup, r)
		if err != nil {
			p.logSourceFailure("volcanic_mapping", cid, err)
			h = domain.HazardEvent{}
		}
		hazards = append(hazards, h)
	}
	if st, ok := p.Store.(RecoveryStore); ok {
		err = st.CommitVolcanic(ctx, reports, hazards, cid, 0)
	} else {
		for _, h := range hazards {
			if h.HazardID == "" {
				continue
			}
			if _, err = p.Store.SaveDedup(ctx, h, cid); err != nil {
				break
			}
		}
	}
	if err == nil && p.Status != nil {
		p.Status.MarkHealthy()
	}
	return err
}

// PollBMKGOnce is also useful for tests and one-shot diagnostics. Save observations as soon
// as their response arrives, even while the independent warning request is still running.
func (p *Poller) PollBMKGOnce(ctx context.Context) {
	cid := uuid.NewString()
	done := make(chan error, 2)
	go func() { done <- p.pollSeismic(ctx, cid) }()
	go func() { done <- p.pollWarnings(ctx, cid) }()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			p.logSourceFailure("bmkg", cid, err)
		}
	}
}
func (p *Poller) PollPVMBGOnce(ctx context.Context) {
	cid := uuid.NewString()
	if err := p.pollVolcanic(ctx, cid); err != nil {
		p.logSourceFailure("pvmbg", cid, err)
	}
}
func (p *Poller) replay(ctx context.Context, cid, entity string) error {
	st := p.Store.(RecoveryStore)
	source := "BMKG"
	if entity == "volcanic-reports" {
		source = "PVMBG"
	}
	after, err := st.HistoryCursor(ctx, source, entity)
	if err != nil {
		return err
	}
	started := domain.Now()
	var page clients.HistoryPage
	if source == "BMKG" {
		page, err = p.BMKG.FetchHistory(ctx, cid, entity, after)
	} else {
		page, err = p.PVMBG.FetchHistory(ctx, cid, entity, after)
	}
	if err != nil {
		return err
	}
	// Commit each replay cursor with its observation and resulting outbox work.
	for _, item := range page.Data {
		switch entity {
		case "seismic-events":
			e, eErr := clients.DecodeSeismic(item.Payload)
			if eErr != nil {
				return eErr
			}
			err = st.CommitSeismic(ctx, []clients.SeismicEvent{e}, cid, item.Cursor)
		case "tsunami-warnings":
			w, eErr := clients.DecodeWarning(item.Payload)
			if eErr != nil {
				return eErr
			}
			err = st.CommitWarnings(ctx, []clients.TsunamiWarning{w}, cid, item.Cursor, started, false)
		case "volcanic-reports":
			r, eErr := clients.DecodeVolcanic(item.Payload)
			if eErr != nil {
				return eErr
			}
			h, eErr := mapping.VolcanicToHazard(ctx, p.Lookup, r)
			if eErr != nil {
				p.logSourceFailure("volcanic_mapping", cid, eErr)
				h = domain.HazardEvent{}
			}
			err = st.CommitVolcanic(ctx, []clients.VolcanicReport{r}, []domain.HazardEvent{h}, cid, item.Cursor)
		}
		if err != nil {
			return err
		}
	}
	if entity == "tsunami-warnings" && !page.HasMore {
		// Do not resolve observations received after this history request began.
		return st.CommitWarnings(ctx, nil, cid, 0, started, true)
	}
	return nil
}
func (p *Poller) logSourceFailure(source, cid string, err error) {
	if p.Logger == nil {
		return
	}
	if errors.Is(err, clients.ErrContractViolation) {
		p.Logger.Error("source contract violation; retained for retry", "service", "aggregator", "source", source, "correlation_id", cid, "error", err)
	} else {
		p.Logger.Warn("source unavailable; retry scheduled", "service", "aggregator", "source", source, "correlation_id", cid, "error", err)
	}
}

func (p *Poller) retryVolcanicMapping(ctx context.Context, cid string) error {
	reader := p.Store.(interface {
		PendingVolcanic(context.Context) ([]clients.VolcanicReport, error)
	})
	reports, err := reader.PendingVolcanic(ctx)
	if err != nil {
		return err
	}
	// Resolver retries do not refresh last_seen_at: these observations came from storage.
	for _, r := range reports {
		h, err := mapping.VolcanicToHazard(ctx, p.Lookup, r)
		if err != nil {
			continue
		}
		resolver := p.Store.(interface {
			ResolveVolcanic(context.Context, clients.VolcanicReport, domain.HazardEvent, string) error
		})
		if err = resolver.ResolveVolcanic(ctx, r, h, cid); err != nil {
			return err
		}
	}
	return nil
}
