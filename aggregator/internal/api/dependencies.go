package api

import "sync/atomic"

// Dependencies tracks live connectivity without tying process liveness to it.
type Dependencies struct {
	storeReady     atomic.Bool
	brokerReady    atomic.Bool
	lifecycleReady atomic.Bool
	warming        atomic.Bool
}

func (d *Dependencies) StoreReady() bool {
	return d.storeReady.Load()
}

func (d *Dependencies) BrokerReady() bool {
	return d.brokerReady.Load()
}

func (d *Dependencies) SetStoreReady(ready bool) {
	d.storeReady.Store(ready)
}

func (d *Dependencies) SetBrokerReady(ready bool) {
	d.brokerReady.Store(ready)
}

func (d *Dependencies) LifecycleReady() bool     { return d.lifecycleReady.Load() }
func (d *Dependencies) SetLifecycleReady(v bool) { d.lifecycleReady.Store(v) }

// Warming is true while a starting instance opens its database connections and runs its first
// reads. Readiness waits for it, so a new instance only receives traffic once that startup cost
// is paid (the warm-up itself is bounded, see the server's startup).
func (d *Dependencies) Warming() bool     { return d.warming.Load() }
func (d *Dependencies) SetWarming(v bool) { d.warming.Store(v) }
