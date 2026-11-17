package main

import (
	"net/http"
	"strings"
	"testing"
)

func TestSituationReportShowsSummaryAndFacts(t *testing.T) {
	body := `{
		"status": "stale",
		"reasons": ["data berubah sejak ringkasan dibuat"],
		"generated_at": "2026-11-17T08:11:20Z",
		"model": "baidu/ernie-5.5-flash-preview:free",
		"ai_summary": {
			"headline": "Gempa AWAS di Aceh",
			"overview": "Terdapat 3 kejadian.",
			"highlights": [{"hazard_id": "hz-eq-aceh-01", "summary": "Gempa berstatus AWAS."}],
			"source_notes": ["SENSOR_PANTAI stale."]
		},
		"facts": {
			"window": "2026-11-17T06:11:07Z/2026-11-17T08:11:07Z",
			"counts": {
				"total": 3,
				"by_hazard_type": {"VOLCANIC": 1, "EARTHQUAKE": 2},
				"by_severity": {"NORMAL": 1, "AWAS": 2},
				"top_areas": [{"area_name": "Aceh", "count": 2}, {"area_name": "Sleman", "count": 1}]
			},
			"critical_events": [{"hazard_id": "hz-eq-aceh-01", "hazard_type": "EARTHQUAKE", "severity": "AWAS", "area_name": "Aceh", "occurred_at": "2026-11-17T07:59:07Z"}],
			"source_status": {"SENSOR_PANTAI": {"state": "stale", "stale_since": "2026-11-17T07:51:07Z"}, "BMKG": {"state": "ok"}},
			"partial": true
		},
		"disclaimer": "Bukan peringatan resmi."
	}`
	report := situationReport(http.StatusOK, []byte(body))
	for _, want := range []string{
		"TERTINGGAL",
		"! data berubah sejak ringkasan dibuat",
		"Gempa AWAS di Aceh",
		"[hz-eq-aceh-01] Gempa berstatus AWAS.",
		"Total 3 kejadian | level: AWAS 2, NORMAL 1 | jenis: EARTHQUAKE 2, VOLCANIC 1",
		"Wilayah terbanyak: Aceh (2), Sleman (1)",
		"Status sumber: BMKG ok, SENSOR_PANTAI stale sejak 2026-11-17T07:51:07Z",
		"Data belum lengkap",
		"Bukan peringatan resmi.",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("report is missing %q:\n%s", want, report)
		}
	}
}

func TestSituationReportWithoutSummary(t *testing.T) {
	report := situationReport(http.StatusOK, []byte(`{"status":"unavailable","reasons":["ringkasan AI tidak merespons"],"ai_summary":null,"facts":null,"disclaimer":"Gunakan /hazards."}`))
	if !strings.Contains(report, "TIDAK TERSEDIA") || !strings.Contains(report, "ringkasan AI tidak merespons") || strings.Contains(report, "FAKTA SISTEM") {
		t.Fatalf("unexpected report:\n%s", report)
	}
	if !strings.Contains(situationReport(http.StatusNotFound, nil), "belum diaktifkan") {
		t.Fatal("disabled feature not explained")
	}
	if !strings.Contains(situationReport(http.StatusTooManyRequests, []byte("busy")), "HTTP 429") {
		t.Fatal("error status not shown")
	}
	if !strings.Contains(situationReport(http.StatusOK, []byte("{")), "tidak valid") {
		t.Fatal("broken body not reported")
	}
}
