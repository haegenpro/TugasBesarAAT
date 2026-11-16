package poller

import (
	"aggregator/internal/avrowire"
	"aggregator/internal/clients"
	"aggregator/internal/domain"
	"aggregator/internal/mapping"
	"context"
	"errors"
)

type LegacyReplayStore interface {
	NextLegacyReplay(context.Context) (*clients.ParkedPayload, error)
	FinishLegacyReplay(context.Context, string, error) error
	CommitVolcanic(context.Context, []clients.VolcanicReport, []domain.HazardEvent, string, int64) error
}

const legacyReplayBatch = 32

func (p *Poller) replayParkedVolcanic(ctx context.Context, st LegacyReplayStore, cid string) error {
	for range legacyReplayBatch {
		parked, err := st.NextLegacyReplay(ctx)
		if err != nil || parked == nil {
			return err
		}
		reports, err := p.PVMBG.Reprocess(ctx, *parked)
		if err != nil {
			if !permanent(err) {
				return err
			}
			p.logSourceFailure("replay:volcanic", cid, err)
			return st.FinishLegacyReplay(ctx, parked.ID, err)
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
		if err = st.CommitVolcanic(ctx, reports, hazards, cid, 0); err != nil {
			return err
		}
		if err = st.FinishLegacyReplay(ctx, parked.ID, nil); err != nil {
			return err
		}
	}
	return nil
}

func permanent(err error) bool {
	return errors.Is(err, clients.ErrContractViolation) || errors.Is(err, avrowire.ErrIncompatible)
}
