package wildfireprocessor

import (
	"aggregator/internal/canonical"
	"aggregator/internal/platform"
	"aggregator/internal/wildfire"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

func LoadConfig(path string) (Config, error) {
	var c Config
	read := func(path string, limit int64) ([]byte, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, limit+1))
		if err == nil && int64(len(b)) > limit {
			return nil, fmt.Errorf("configuration file exceeds explicit limit")
		}
		return b, err
	}
	b, err := read(path, 1<<20)
	if err != nil {
		return c, err
	}
	if err = wildfire.Decode(b, &c); err != nil {
		return c, err
	}
	province := c.ProvinceFile
	if !filepath.IsAbs(province) {
		province = filepath.Join(filepath.Dir(path), province)
	}
	c.ProvinceJSON, err = read(province, 1<<20)
	if err != nil {
		return c, err
	}
	_, _, err = c.validate()
	return c, err
}
func (e *Engine) RunPreparation(ctx context.Context, owner canonical.Ownership, logger *slog.Logger) {
	e.run(ctx, owner, logger, func(call context.Context) (bool, error) {
		if err := e.Check(call); err != nil {
			return false, err
		}
		return e.Step(call, owner)
	})
}
func (e *Engine) RunDelivery(ctx context.Context, p, d canonical.Ownership, logger *slog.Logger) {
	e.run(ctx, p, logger, func(call context.Context) (bool, error) { return e.StepDelivery(call, p, d) })
}
func (e *Engine) run(ctx context.Context, owner canonical.Ownership, logger *slog.Logger, step func(context.Context) (bool, error)) {
	failures := 0
	last := ""
	lastMetrics := time.Time{}
	for ctx.Err() == nil {
		started := time.Now()
		worked, err := step(ctx)
		platform.Observe("wildfire_step_seconds", time.Since(started).Seconds())
		delay := 25 * time.Millisecond
		if err != nil {
			platform.Add("wildfire_retries_total", 1)
			failures = min(failures+1, 8)
			ceiling := min(30*time.Second, 250*time.Millisecond*time.Duration(1<<uint(failures-1)))
			delay = time.Duration(rand.Int64N(int64(ceiling) + 1))
			if last != err.Error() {
				last = err.Error()
				if logger != nil {
					logger.Warn("wildfire obligation pending", "reason", last)
				}
			}
			call, cancel := context.WithTimeout(ctx, canonical.Deadline)
			e.recordPending(call, owner, err.Error())
			cancel()
		} else {
			failures = 0
			last = ""
			if worked {
				delay = 0
			}
		}
		if time.Since(lastMetrics) > time.Second {
			e.metrics(ctx)
			lastMetrics = time.Now()
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}
func (e *Engine) metrics(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var received, prepared, applied, late, pending, blocked int64
	var oldest float64
	err := e.Store.Pool.QueryRow(ctx, `SELECT s.received_through,r.prepared_through,r.applied_through,r.late_count FROM input_sessions s JOIN wildfire_runs r USING(namespace) WHERE s.source='SATELIT' AND s.namespace=$1`, e.Config.Namespace).Scan(&received, &prepared, &applied, &late)
	if err != nil {
		return
	}
	err = e.Store.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE late),coalesce(extract(epoch FROM clock_timestamp()-min(created_at) FILTER (WHERE late)),0)::float8,count(*) FILTER (WHERE blocked_reason IS NOT NULL) FROM wildfire_jobs WHERE namespace=$1 AND NOT applied`, e.Config.Namespace).Scan(&pending, &oldest, &blocked)
	if err != nil {
		return
	}
	for name, value := range map[string]int64{"received_through": received, "prepared_through": prepared, "applied_through": applied, "late_mutations_total": late, "correction_pending": pending, "prepared_lag": received - prepared, "applied_lag": received - applied} {
		platform.Gauge("wildfire_"+name, float64(value))
	}
	platform.Gauge("wildfire_oldest_correction_age_seconds", oldest)
	platform.Gauge("wildfire_blocked_output", float64(boolInt(blocked > 0)))
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	platform.Gauge("wildfire_heap_bytes", float64(mem.HeapAlloc))
}
