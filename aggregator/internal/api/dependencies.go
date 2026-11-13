package api

import "sync/atomic"

// Dependencies tracks live connectivity without tying process liveness to it.
type Dependencies struct {
	storeReady     atomic.Bool
	brokerReady    atomic.Bool
	lifecycleReady atomic.Bool
	draining       atomic.Bool
	readReady      atomic.Bool
	separateReads  atomic.Bool
}

func (d *Dependencies) ReadReady() bool {
	if d.separateReads.Load() {
		return d.readReady.Load()
	}
	return d.StoreReady()
}
func (d *Dependencies) SetReadReady(v bool) { d.separateReads.Store(true); d.readReady.Store(v) }

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
func (d *Dependencies) Draining() bool           { return d.draining.Load() }
func (d *Dependencies) SetDraining(v bool)       { d.draining.Store(v) }
