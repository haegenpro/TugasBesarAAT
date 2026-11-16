package coastal

import (
	"aggregator/internal/canonical"
	"aggregator/internal/domain"
	"aggregator/internal/ownership"
	"aggregator/internal/platform"
	"aggregator/internal/rawinput"
	"context"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
)

type Writer interface {
	canonical.OwnershipRepository
	ApplyBatch(context.Context, canonical.Batch) (canonical.Result, error)
	Reconcile(context.Context, domain.Source, string) (canonical.Entity, error)
}

type CanonicalWriter struct {
	Validator *Validator
	Store     Writer
	Logger    *slog.Logger
}

func (w *CanonicalWriter) Run(ctx context.Context, consumer jetstream.Consumer) {
	ownership.Run(ctx, w.Store, "canonical:"+Source, func(owned context.Context, owner canonical.Ownership) {
		consume(owned, consumer, w.Logger, "coastal canonical delivery", func(ctx context.Context, msg jetstream.Msg) error {
			return w.handle(ctx, owner, msg)
		})
	})
}

func Mutation(e rawinput.Envelope, o Observation, s Station, revision int64) canonical.Mutation {
	severity := domain.SeverityNormal
	if o.Critical {
		severity = domain.SeverityAwas
	}
	return canonical.Mutation{
		OperationID: e.MessageID, Source: domain.SourceSensorPantai, SourceRefID: o.ID, Operation: "upsert",
		SourceOrder: canonical.SourceOrder{Namespace: "raw:" + e.Namespace, Position: *e.Position}, ExpectedRevision: revision,
		Event: &canonical.MutableEvent{HazardType: domain.HazardCoastal, Severity: severity, AreaName: s.Province, Latitude: s.Latitude, Longitude: s.Longitude, OccurredAt: o.ObservedAt.UTC(),
			Attributes: map[string]any{"station_id": s.ID, "station_name": s.Name, "sea_level_anomaly_m": o.SeaLevel, "wave_height_m": o.WaveHeight, "quality": o.Quality, "critical": o.Critical}},
	}
}

func (w *CanonicalWriter) handle(ctx context.Context, owner canonical.Ownership, msg jetstream.Msg) error {
	e, err := rawinput.Decode(ctx, w.Validator.Registry, w.Validator.Reader, msg.Data())
	if err != nil {
		return terminal(msg, w.Logger, err)
	}
	o, s, result, err := w.Validator.Check(ctx, e)
	switch result {
	case rejected:
		return msg.DoubleAck(ctx)
	case retry:
		return err
	}
	revision := int64(0)
	for attempt := 0; attempt < 2; attempt++ {
		batch := canonical.Batch{ContractVersion: 1, BatchID: rawinput.Identity("coastal-batch-v1", e.MessageID, revision), Ownership: owner, Mutations: []canonical.Mutation{Mutation(e, o, s, revision)}}
		_, err = w.Store.ApplyBatch(ctx, batch)
		var problem *canonical.Error
		if !errors.As(err, &problem) {
			break
		}
		if problem.Code == "revision_conflict" && attempt == 0 {
			entity, rerr := w.Store.Reconcile(ctx, domain.SourceSensorPantai, o.ID)
			if rerr != nil {
				return rerr
			}
			revision = entity.Revision
			continue
		}
		if problem.Code == "stale_operation" || problem.Status == 400 {
			w.Logger.Warn("coastal mutation not applied", "observation_id", o.ID, "error", err)
			platform.Add("coastal_canonical_rejected_total", 1)
			return msg.DoubleAck(ctx)
		}
		break
	}
	if err != nil {
		return err
	}
	platform.Add("coastal_canonical_applied_total", 1)
	return msg.DoubleAck(ctx)
}
