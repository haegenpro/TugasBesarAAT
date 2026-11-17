package main

import (
	"consumernotifier/internal/platform"
	"context"
	"math"
	"time"
)

// Collection is independent of delivery. Failure preserves explicit missing
// measurements rather than publishing zero pending/lost operations. This is
// the acquired-warning cohort; source/reference reconciliation remains required.
func (r *warningRecipient) collectSLOMetrics(ctx context.Context) {
	var last time.Time
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		value, err := r.reconciledSLO(call, time.Now().UTC().Add(-time.Hour))
		cancel()
		if err != nil {
			platform.Gauge("coastal_slo_collection_success", 0)
			if !last.IsZero() {
				platform.Gauge("coastal_slo_collection_age_seconds", time.Since(last).Seconds())
			}
		} else {
			last = time.Now()
			platform.Gauge("coastal_slo_collection_success", 1)
			platform.Gauge("coastal_slo_collection_age_seconds", 0)
			exportWarningSLO(value)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func exportWarningSLO(value warningSLO) {
	for name, count := range map[string]int64{"expected": value.Expected, "delivered": value.Delivered, "pending": value.Pending, "pending_overdue": value.PendingOverdue, "rejected": value.Rejected, "evaluated": value.Evaluated, "clock_anomalies": value.ClockAnomalies, "receipt_only": value.ReceiptOnly} {
		platform.Gauge("coastal_slo_"+name, float64(count))
	}
	for name, valid := range map[string]bool{"coverage_complete": value.CoverageComplete, "p95_complete": value.P95Complete} {
		n := 0.
		if valid {
			n = 1
		}
		platform.Gauge("coastal_slo_"+name, n)
	}
	for name, number := range map[string]*float64{"on_time_ratio": value.OnTimeRatio, "error_budget_used": value.BudgetUsed, "p95_delivered_seconds": value.P95Seconds, "p95_lower_bound_seconds": value.P95LowerBoundSeconds} {
		n := math.NaN()
		if number != nil {
			n = *number
		}
		platform.Gauge("coastal_slo_"+name, n)
	}
}
