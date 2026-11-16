package coastal

import (
	"context"
	"sync"
	"time"
)

type Limiter struct {
	Rate  float64
	Burst float64

	mu     sync.Mutex
	tokens float64
	last   time.Time
	ready  bool
}

func (l *Limiter) reserve() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if !l.ready {
		l.tokens, l.last, l.ready = l.Burst, now, true
	}
	l.tokens = min(l.Burst, l.tokens+now.Sub(l.last).Seconds()*l.Rate)
	l.last = now
	if l.tokens >= 1 {
		l.tokens--
		return 0
	}
	wait := time.Duration((1 - l.tokens) / l.Rate * float64(time.Second))
	l.tokens--
	return wait
}

func (l *Limiter) Wait(ctx context.Context) error {
	if l == nil || l.Rate <= 0 {
		return ctx.Err()
	}
	wait := l.reserve()
	if wait == 0 {
		return ctx.Err()
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
