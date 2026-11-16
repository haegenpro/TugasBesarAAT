package coastal

import (
	"aggregator/internal/platform"
	"errors"
	"sync"
	"time"
)

var ErrOpen = errors.New("sensor endpoint circuit open")

type State int

const (
	Closed State = iota
	HalfOpen
	Open
)

func (s State) String() string {
	return [...]string{"closed", "half_open", "open"}[s]
}

type Breaker struct {
	Name        string
	Threshold   int
	Cooldown    time.Duration
	MaxCooldown time.Duration

	mu       sync.Mutex
	state    State
	failures int
	backoff  time.Duration
	until    time.Time
	probing  bool
	now      func() time.Time
}

func (b *Breaker) clock() time.Time {
	if b.now != nil {
		return b.now()
	}
	return time.Now()
}

func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return nil
	case Open:
		if b.clock().Before(b.until) {
			platform.Add("coastal_"+b.Name+"_short_circuited_total", 1)
			return ErrOpen
		}
		b.set(HalfOpen)
	}
	if b.probing {
		platform.Add("coastal_"+b.Name+"_short_circuited_total", 1)
		return ErrOpen
	}
	b.probing = true
	return nil
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures, b.backoff, b.probing = 0, 0, false
	b.set(Closed)
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == HalfOpen || b.failures >= max(b.Threshold, 1) {
		if b.backoff == 0 {
			b.backoff = b.Cooldown
		} else {
			b.backoff = min(2*b.backoff, max(b.MaxCooldown, b.Cooldown))
		}
		b.until = b.clock().Add(b.backoff)
		b.probing = false
		b.set(Open)
	}
}

func (b *Breaker) Release() {
	b.mu.Lock()
	b.probing = false
	b.mu.Unlock()
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) set(s State) {
	if b.state != s {
		b.state = s
		platform.Gauge("coastal_"+b.Name+"_breaker_state", float64(s))
	}
}
