package api

import "sync/atomic"

// Dependencies tracks live connectivity without tying process liveness to it.
type Dependencies struct {
	storeReady     atomic.Bool
	brokerReady    atomic.Bool
	lifecycleReady atomic.Bool
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
