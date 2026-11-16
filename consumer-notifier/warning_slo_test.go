package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func expectedServer(t *testing.T, items []expectedWarning, asOf time.Time) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer slo-test-token" {
			w.WriteHeader(401)
			return
		}
		after, _ := strconv.ParseInt(req.URL.Query().Get("after"), 10, 64)
		if after > 0 && (req.URL.Query().Get("through") != strconv.Itoa(len(items)) || req.URL.Query().Get("as_of") != asOf.Format(time.RFC3339Nano)) {
			t.Error("snapshot cursor not retained")
			w.WriteHeader(400)
			return
		}
		coverage := asOf.Add(-24 * time.Hour)
		page := expectedWarningPage{Items: []expectedWarning{}, Through: int64(len(items)), AsOf: asOf, CoverageSince: &coverage}
		for _, item := range items {
			if item.Sequence > after && len(page.Items) < 500 {
				page.Items = append(page.Items, item)
				page.Next = item.Sequence
			}
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestWarningSLOIncludesLatePendingRejectedAndUntracked(t *testing.T) {
	db, err := openDB(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	first := now.Add(-20 * time.Second)
	items := []expectedWarning{{1, "ontime", first, false}, {2, "late", first, false}, {3, "pending", first, false}, {4, "inflight", now.Add(-time.Second), false}, {5, "invalid", now.Add(-time.Second), true}}
	authority := expectedServer(t, items, now)
	for _, v := range []struct {
		id      string
		latency time.Duration
	}{{"ontime", time.Second}, {"late", 8 * time.Second}, {"legacy", 2 * time.Second}} {
		_, err = db.Exec(`INSERT INTO coastal_warning_receipts(notification_id,digest,observation_id,observed_at,first_received_at,committed_at) VALUES($1,'digest',$1,$2,$2,$3)`, v.id, first, first.Add(v.latency))
		if err != nil {
			t.Fatal(err)
		}
	}
	r := &warningRecipient{db: db, cfg: warningConfig{ExpectationsURL: authority.URL, ExpectationsToken: "slo-test-token"}}
	report, err := r.slo(context.Background(), now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if report.Expected != 5 || report.Delivered != 2 || report.Pending != 3 || report.PendingOverdue != 1 || report.Rejected != 1 || report.Evaluated != 4 || report.OnTime != 1 || report.ReceiptOnly != 1 {
		t.Fatalf("bad reconciliation: %+v", report)
	}
	if report.OnTimeRatio == nil || *report.OnTimeRatio != 0.25 || math.Abs(*report.BudgetUsed-15) > 0.0001 {
		t.Fatalf("pending failures excluded from budget: %+v", report)
	}
	if report.P95Complete || report.AllAcquiredDelivered || !report.SourceReconciliationRequired {
		t.Fatalf("incomplete delivery falsely certified: %+v", report)
	}
	if report.P95Seconds == nil || math.Abs(*report.P95Seconds-7.65) > 0.001 {
		t.Fatalf("wrong delivered latency quantile: %+v", report)
	}
	if report.P95LowerBoundSeconds == nil || math.Abs(*report.P95LowerBoundSeconds-17.6) > 0.001 {
		t.Fatalf("pending age missing from full-cohort lower bound: %+v", report)
	}
}

func TestWarningSLOPagesExpectationsAndEnforcesWindowLimit(t *testing.T) {
	db, err := openDB(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	var items []expectedWarning
	for i := 1; i <= 501; i++ {
		items = append(items, expectedWarning{int64(i), fmt.Sprint(i), now.Add(-time.Minute), false})
	}
	authority := expectedServer(t, items, now)
	r := &warningRecipient{db: db, cfg: warningConfig{ExpectationsURL: authority.URL, ExpectationsToken: "slo-test-token"}}
	report, err := r.slo(context.Background(), now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if report.Expected != 501 || report.PendingOverdue != 501 || report.Evaluated != 501 || *report.OnTimeRatio != 0 || report.P95Seconds != nil {
		t.Fatalf("pagination lost expected warnings: %+v", report)
	}
	r.cfg.MaxExpectations = 500
	if _, err = r.slo(context.Background(), now.Add(-time.Hour)); err == nil {
		t.Fatal("oversized accounting window silently accepted")
	}
}

func TestWarningSLOAuthorityFailureNeverReturnsReceiptOnlySuccess(t *testing.T) {
	r := &warningRecipient{}
	if _, err := r.slo(context.Background(), time.Now().Add(-time.Hour)); err == nil {
		t.Fatal("unconfigured authority accepted")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer srv.Close()
	r.cfg.ExpectationsURL, r.cfg.ExpectationsToken = srv.URL, "slo-test-token"
	if _, err := r.slo(context.Background(), time.Now().Add(-time.Hour)); err == nil {
		t.Fatal("unreachable authority became success")
	}
}

func TestWarningSLORejectsNegativeClockLatency(t *testing.T) {
	db, err := openDB(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	first := now.Add(-time.Second)
	authority := expectedServer(t, []expectedWarning{{1, "skew", first, false}}, now)
	_, err = db.Exec(`INSERT INTO coastal_warning_receipts VALUES('skew','digest','obs',$1,$1,$2)`, first, first.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r := &warningRecipient{db: db, cfg: warningConfig{ExpectationsURL: authority.URL, ExpectationsToken: "slo-test-token"}}
	report, err := r.slo(context.Background(), now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if report.ClockAnomalies != 1 || report.OnTime != 0 || report.P95Seconds != nil || report.P95Complete {
		t.Fatalf("negative latency counted as healthy: %+v", report)
	}
}
