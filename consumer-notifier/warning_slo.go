package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type expectedWarning struct {
	Sequence        int64     `json:"sequence"`
	NotificationID  string    `json:"notification_id"`
	FirstReceivedAt time.Time `json:"first_received_at"`
	Rejected        bool      `json:"rejected"`
}
type expectedWarningPage struct {
	Items         []expectedWarning `json:"items"`
	Through       int64             `json:"through"`
	AsOf          time.Time         `json:"as_of"`
	Next          int64             `json:"next"`
	CoverageSince *time.Time        `json:"coverage_since"`
}

func (r *warningRecipient) reconciledSLO(ctx context.Context, since time.Time) (warningSLO, error) {
	out := warningSLO{TargetRatio: 0.95, TargetP95Sec: onTime.Seconds(), Scope: "durably_acquired_critical_observations", P95Scope: "delivered_only", ClockBasis: "ingestion_database_to_recipient_database; synchronized database clocks required", SourceReconciliationRequired: true}
	if r.cfg.ExpectationsURL == "" || r.cfg.ExpectationsToken == "" {
		return out, errors.New("expected-warning authority is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Do not follow redirects carrying this read-only service credential.
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	limit := r.cfg.MaxExpectations
	if limit <= 0 {
		limit = 100000
	}
	var after, through int64
	var asOf time.Time
	latencies := []float64{}
	lowerBounds := []float64{}
	seen := map[string]bool{}
	for {
		q := url.Values{"since": {since.UTC().Format(time.RFC3339Nano)}}
		if !asOf.IsZero() {
			q.Set("after", strconv.FormatInt(after, 10))
			q.Set("through", strconv.FormatInt(through, 10))
			q.Set("as_of", asOf.Format(time.RFC3339Nano))
		}
		req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(r.cfg.ExpectationsURL, "/")+"/internal/coastal/warnings/expected?"+q.Encode(), nil)
		if err != nil {
			return out, err
		}
		req.Header.Set("Authorization", "Bearer "+r.cfg.ExpectationsToken)
		resp, err := client.Do(req)
		if err != nil {
			return out, err
		}
		var page expectedWarningPage
		body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
		resp.Body.Close()
		if err != nil || len(body) > 1<<20 || resp.StatusCode != 200 {
			return out, errors.New("expected-warning authority unavailable")
		}
		if err = json.Unmarshal(body, &page); err != nil {
			return out, err
		}
		if page.Items == nil || len(page.Items) > 500 || page.Through < 0 || page.AsOf.IsZero() {
			return out, errors.New("invalid expected-warning page")
		}
		if asOf.IsZero() {
			through, asOf = page.Through, page.AsOf
			out.CoverageSince = page.CoverageSince
			out.CoverageComplete = page.CoverageSince != nil && !since.Before(*page.CoverageSince)
		} else if through != page.Through || !asOf.Equal(page.AsOf) {
			return out, errors.New("expected-warning snapshot changed")
		}
		last := after
		for _, item := range page.Items {
			if item.Sequence <= last || item.Sequence > through || item.NotificationID == "" || seen[item.NotificationID] || item.FirstReceivedAt.Before(since) || item.FirstReceivedAt.After(asOf) {
				return out, errors.New("invalid expected-warning identity or position")
			}
			last = item.Sequence
			seen[item.NotificationID] = true
			if len(seen) > limit {
				return out, fmt.Errorf("SLO window exceeds %d expected warnings; narrow since", limit)
			}
		}
		if len(page.Items) > 0 && page.Next != last {
			return out, errors.New("invalid expected-warning cursor")
		}
		if len(page.Items) > 0 {
			b, _ := json.Marshal(page.Items)
			rows, err := r.db.QueryContext(ctx, `SELECT e.first_received_at,e.rejected,r.committed_at FROM jsonb_to_recordset($1::jsonb) AS e(notification_id text,first_received_at timestamptz,rejected boolean) LEFT JOIN coastal_warning_receipts r ON r.notification_id=e.notification_id AND r.committed_at<=$2`, string(b), asOf)
			if err != nil {
				return out, err
			}
			for rows.Next() {
				var first time.Time
				var rejected bool
				var committed sql.NullTime
				if err = rows.Scan(&first, &rejected, &committed); err != nil {
					rows.Close()
					return out, err
				}
				out.Expected++
				if committed.Valid {
					out.Delivered++
					out.Evaluated++
					latency := committed.Time.Sub(first).Seconds()
					if latency < 0 {
						out.ClockAnomalies++
						continue
					}
					latencies = append(latencies, latency)
					lowerBounds = append(lowerBounds, latency)
					if latency <= onTime.Seconds() {
						out.OnTime++
					}
				} else {
					out.Pending++
					lowerBounds = append(lowerBounds, asOf.Sub(first).Seconds())
					if rejected {
						out.Rejected++
						out.Evaluated++
					} else if asOf.Sub(first) >= onTime {
						out.PendingOverdue++
						out.Evaluated++
					}
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return out, err
			}
		}
		if len(page.Items) < 500 {
			break
		}
		after = last
	}
	out.Window = since.UTC().Format(time.RFC3339Nano) + "/" + asOf.Format(time.RFC3339Nano)
	var receipts int64
	if err := r.db.QueryRowContext(ctx, `SELECT count(*),(SELECT count(*) FROM coastal_warning_conflicts WHERE detected_at>=$1 AND detected_at<=$2) FROM coastal_warning_receipts WHERE first_received_at>=$1 AND committed_at<=$2`, since, asOf).Scan(&receipts, &out.Conflicts); err != nil {
		return out, err
	}
	out.ReceiptOnly = max(receipts-out.Delivered, 0)
	out.P95Complete = out.CoverageComplete && out.Expected > 0 && out.Pending == 0 && out.ClockAnomalies == 0 && out.ReceiptOnly == 0
	out.AllAcquiredDelivered = out.CoverageComplete && out.Expected > 0 && out.Expected == out.Delivered && out.ReceiptOnly == 0
	if out.Evaluated > 0 {
		ratio := float64(out.OnTime) / float64(out.Evaluated)
		used := (1 - ratio) / (1 - out.TargetRatio)
		out.OnTimeRatio, out.BudgetUsed = &ratio, &used
	}
	if len(latencies) > 0 && out.ClockAnomalies == 0 {
		sort.Float64s(latencies)
		p95 := percentile95(latencies)
		maximum := latencies[len(latencies)-1]
		out.P95Seconds, out.MaxSeconds = &p95, &maximum
	}
	if len(lowerBounds) > 0 && out.ClockAnomalies == 0 {
		sort.Float64s(lowerBounds)
		bound := percentile95(lowerBounds)
		out.P95LowerBoundSeconds = &bound
	}
	return out, nil
}

// Match PostgreSQL percentile_cont's interpolation, not a rounded rank.
func percentile95(sorted []float64) float64 {
	rank := 0.95 * float64(len(sorted)-1)
	lo, hi := int(math.Floor(rank)), int(math.Ceil(rank))
	return sorted[lo] + (sorted[hi]-sorted[lo])*(rank-float64(lo))
}
