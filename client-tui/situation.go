package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

type situationEvent struct {
	HazardID   string `json:"hazard_id"`
	HazardType string `json:"hazard_type"`
	Severity   string `json:"severity"`
	AreaName   string `json:"area_name"`
	OccurredAt string `json:"occurred_at"`
}

type situation struct {
	Status      string   `json:"status"`
	Reasons     []string `json:"reasons"`
	GeneratedAt string   `json:"generated_at"`
	Model       string   `json:"model"`
	AI          *struct {
		Headline   string `json:"headline"`
		Overview   string `json:"overview"`
		Highlights []struct {
			HazardID string `json:"hazard_id"`
			Summary  string `json:"summary"`
		} `json:"highlights"`
		SourceNotes []string `json:"source_notes"`
	} `json:"ai_summary"`
	Facts *struct {
		Window string `json:"window"`
		Counts struct {
			Total      int            `json:"total"`
			ByType     map[string]int `json:"by_hazard_type"`
			BySeverity map[string]int `json:"by_severity"`
			TopAreas   []struct {
				AreaName string `json:"area_name"`
				Count    int    `json:"count"`
			} `json:"top_areas"`
		} `json:"counts"`
		CriticalEvents []situationEvent `json:"critical_events"`
		SourceStatus   map[string]struct {
			State      string `json:"state"`
			StaleSince string `json:"stale_since"`
		} `json:"source_status"`
		Partial bool `json:"partial"`
	} `json:"facts"`
	Disclaimer string `json:"disclaimer"`
}

var statusLabels = map[string]string{
	"fresh":       "TERKINI",
	"stale":       "TERTINGGAL - data sudah berubah sejak ringkasan dibuat",
	"unavailable": "TIDAK TERSEDIA",
}

func (a *app) situation() {
	status, data, err := a.protected(http.MethodGet, "/situation")
	if err != nil {
		fmt.Println("Gagal:", err)
	} else {
		fmt.Print(situationReport(status, data))
	}
	a.pause()
}

func situationReport(status int, data []byte) string {
	switch {
	case status == http.StatusNotFound:
		return "Ringkasan AI belum diaktifkan di client-api (SITUATION_BASE_URL kosong).\n"
	case status != http.StatusOK:
		return fmt.Sprintf("HTTP %d: %s\n", status, data)
	}
	var s situation
	if err := json.Unmarshal(data, &s); err != nil || s.Status == "" {
		return "Respons ringkasan tidak valid.\n"
	}

	var b strings.Builder
	label := statusLabels[s.Status]
	if label == "" {
		label = strings.ToUpper(s.Status)
	}
	fmt.Fprintf(&b, "\nRINGKASAN SITUASI (AI) | %s\n", label)
	if s.GeneratedAt != "" {
		fmt.Fprintf(&b, "Dibuat %s oleh %s\n", s.GeneratedAt, s.Model)
	}
	for _, reason := range s.Reasons {
		fmt.Fprintln(&b, "  !", reason)
	}

	if s.AI != nil {
		fmt.Fprintf(&b, "\n%s\n%s\n", s.AI.Headline, s.AI.Overview)
		if len(s.AI.Highlights) > 0 {
			fmt.Fprintln(&b, "\nKejadian penting:")
			for _, h := range s.AI.Highlights {
				fmt.Fprintf(&b, "  - [%s] %s\n", h.HazardID, h.Summary)
			}
		}
		if len(s.AI.SourceNotes) > 0 {
			fmt.Fprintln(&b, "\nCatatan sumber:")
			for _, note := range s.AI.SourceNotes {
				fmt.Fprintln(&b, "  -", note)
			}
		}
	}

	if f := s.Facts; f != nil {
		fmt.Fprintf(&b, "\nFAKTA SISTEM | %s\n", f.Window)
		fmt.Fprintf(&b, "Total %d kejadian | level: %s | jenis: %s\n", f.Counts.Total, severityCounts(f.Counts.BySeverity), sortedCounts(f.Counts.ByType))
		if len(f.Counts.TopAreas) > 0 {
			areas := make([]string, len(f.Counts.TopAreas))
			for i, area := range f.Counts.TopAreas {
				areas[i] = fmt.Sprintf("%s (%d)", area.AreaName, area.Count)
			}
			fmt.Fprintln(&b, "Wilayah terbanyak:", strings.Join(areas, ", "))
		}
		if len(f.CriticalEvents) > 0 {
			fmt.Fprintln(&b, "Kejadian AWAS:")
			for _, e := range f.CriticalEvents {
				fmt.Fprintf(&b, "  %-36s  %-10s  %s | %s\n", e.HazardID, e.HazardType, e.AreaName, e.OccurredAt)
			}
		}
		names := make([]string, 0, len(f.SourceStatus))
		for name := range f.SourceStatus {
			names = append(names, name)
		}
		sort.Strings(names)
		for i, name := range names {
			state := f.SourceStatus[name]
			names[i] = name + " " + state.State
			if state.StaleSince != "" {
				names[i] += " sejak " + state.StaleSince
			}
		}
		if len(names) > 0 {
			fmt.Fprintln(&b, "Status sumber:", strings.Join(names, ", "))
		}
		if f.Partial {
			fmt.Fprintln(&b, "Data belum lengkap; angka di atas hanya sebagian.")
		}
	}

	fmt.Fprintf(&b, "\n%s\n", s.Disclaimer)
	return b.String()
}

func severityCounts(counts map[string]int) string {
	parts := make([]string, 0, len(counts))
	for _, level := range []string{"AWAS", "SIAGA", "WASPADA", "NORMAL"} {
		if n := counts[level]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", level, n))
		}
	}
	return orDash(parts)
}

func sortedCounts(counts map[string]int) string {
	parts := make([]string, 0, len(counts))
	for name, n := range counts {
		parts = append(parts, fmt.Sprintf("%s %d", name, n))
	}
	sort.Strings(parts)
	return orDash(parts)
}

func orDash(parts []string) string {
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ", ")
}
