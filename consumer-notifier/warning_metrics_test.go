package main

import (
	"consumernotifier/internal/platform"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWarningMetricsPreserveUnknownRatioAndIncompleteCoverage(t *testing.T) {
	exportWarningSLO(warningSLO{Expected: 3, Pending: 3, CoverageComplete: false})
	w := httptest.NewRecorder()
	platform.Handler("consumer-notifier", http.NotFoundHandler()).ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, line := range []string{"app_coastal_slo_pending 3", "app_coastal_slo_on_time_ratio NaN", "app_coastal_slo_error_budget_used NaN", "app_coastal_slo_coverage_complete 0", "app_coastal_slo_p95_complete 0"} {
		if !strings.Contains(w.Body.String(), line) {
			t.Errorf("missing truthful SLO metric %s", line)
		}
	}
}
