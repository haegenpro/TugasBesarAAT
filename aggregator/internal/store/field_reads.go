package store

import (
	"aggregator/internal/platform"
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

// NewFieldReadView reserves a read-only part of the existing primary/observer
// connection budgets. It never shares a data connection with ordinary traffic.
// Only register the returned repository on the authenticated Field read routes.
func (s *Store) NewFieldReadView(ctx context.Context) (*Store, func(), error) {
	if !platform.Bool("FIELD_READ_PRIORITY_ENABLED", false) {
		return nil, nil, errors.New("Field read isolation is not enabled")
	}
	config := s.Pool.Config()
	config.MaxConns = int32(platform.Int("FIELD_READ_DB_RESERVED", 4))
	config.MinConns = 0
	config.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, nil, err
	}
	view := &Store{Pool: pool, logger: s.logger, control: s.control, Canonical: s.Canonical, Inputs: s.Inputs, Reads: s.Reads, Acquisition: s.Acquisition, Processor: s.Processor, idleTransactionTimeout: s.idleTransactionTimeout, snapshots: s.snapshots}
	var observer *replicatedDurability
	closeView := func() {
		pool.Close()
		if observer != nil {
			observer.Close()
		}
	}
	if original, ok := s.Canonical.Durability.(*replicatedDurability); ok {
		observer = &replicatedDurability{s: view, cfg: original.cfg}
		for _, source := range original.pools {
			config := source.Config()
			config.MaxConns = 1
			config.MinConns = 0
			config.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
			p, e := pgxpool.NewWithConfig(ctx, config)
			if e != nil {
				closeView()
				return nil, nil, e
			}
			observer.pools = append(observer.pools, p)
		}
		view.Canonical.Durability = observer
		if observer.cfg.Mode == "citus" {
			// Live Field reads fan out only to worker observers. Use the
			// reserved coordinator observer for the visibility lock so Field
			// requests do not wait behind the general read/admission pool.
			view.readGuard = observer.pools[0]
		}
		if s.Reads.Inventory != nil {
			if err := view.ConfigureLiveReads(s.Reads.Signer.Key); err != nil {
				closeView()
				return nil, nil, err
			}
		} else {
			view.Reads.Groups = map[int]*pgxpool.Pool{}
			for i, g := range observer.cfg.Groups {
				view.Reads.Groups[g.ID] = observer.pools[i]
			}
		}
	} else if s.Reads.Inventory != nil {
		closeView()
		return nil, nil, errors.New("Field read isolation requires captured replication observers")
	}
	return view, closeView, nil
}

func (s *Store) CollectFieldReadMetrics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := s.Pool.Stat()
			platform.Gauge("field_read_db_connections_in_use", float64(stats.AcquiredConns()))
			platform.Gauge("field_read_db_connections_max", float64(stats.MaxConns()))
		}
	}
}
