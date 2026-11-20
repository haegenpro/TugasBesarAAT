package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const disclaimer = "Ringkasan ini dibuat AI dari data BNPB dan bukan peringatan resmi. Ikuti peringatan dari kanal resmi."

type Highlight struct {
	HazardID string `json:"hazard_id"`
	Summary  string `json:"summary"`
}

type Narrative struct {
	Headline    string      `json:"headline"`
	Overview    string      `json:"overview"`
	Highlights  []Highlight `json:"highlights"`
	SourceNotes []string    `json:"source_notes"`
}

type Summary struct {
	Narrative   Narrative
	Model       string
	GeneratedAt time.Time
	Snapshot    *Snapshot
}

type Invalid struct{ Reason string }

func (e *Invalid) Error() string { return "invalid summary: " + e.Reason }

func invalid(format string, args ...any) error { return &Invalid{Reason: fmt.Sprintf(format, args...)} }

func extractJSON(content string) string {
	content = strings.TrimSpace(content)
	content = strings.TrimPrefix(content, "```json")
	content = strings.TrimPrefix(content, "```")
	content = strings.TrimSuffix(content, "```")
	if start, end := strings.Index(content, "{"), strings.LastIndex(content, "}"); start >= 0 && end > start {
		return content[start : end+1]
	}
	return content
}

func text(value string, limit int) bool {
	value = strings.TrimSpace(value)
	return value != "" && utf8.ValidString(value) && utf8.RuneCountInString(value) <= limit
}

type draft struct {
	Headline   string `json:"headline"`
	Overview   string `json:"overview"`
	Highlights []struct {
		Ref     string `json:"ref"`
		Summary string `json:"summary"`
	} `json:"highlights"`
	SourceNotes []string `json:"source_notes"`
}

func validate(content string, s *Snapshot, maxHighlights int) (Narrative, error) {
	var a draft
	d := json.NewDecoder(bytes.NewReader([]byte(extractJSON(content))))
	d.DisallowUnknownFields()
	if err := d.Decode(&a); err != nil {
		return Narrative{}, invalid("not the expected JSON object: %v", err)
	}
	n := Narrative{Headline: a.Headline, Overview: a.Overview, Highlights: make([]Highlight, 0, len(a.Highlights)), SourceNotes: a.SourceNotes}
	if !text(n.Headline, 200) {
		return n, invalid("headline is empty or too long")
	}
	if !text(n.Overview, 1200) {
		return n, invalid("overview is empty or too long")
	}
	seen := map[string]bool{}
	for _, h := range a.Highlights {
		id, ok := s.refs[h.Ref]
		if !ok {
			return n, invalid("highlight refers to unknown event %q", h.Ref)
		}
		if seen[h.Ref] {
			return n, invalid("event %q is highlighted twice", h.Ref)
		}
		seen[h.Ref] = true
		if !text(h.Summary, 300) {
			return n, invalid("highlight %q is empty or too long", h.Ref)
		}
		n.Highlights = append(n.Highlights, Highlight{HazardID: id, Summary: h.Summary})
	}
	if len(n.SourceNotes) > 5 {
		return n, invalid("too many source notes")
	}
	for _, note := range n.SourceNotes {
		if !text(note, 240) {
			return n, invalid("source note is empty or too long")
		}
	}
	if s.Counts.Total == 0 && len(n.Highlights) > 0 {
		return n, invalid("highlights reported for an empty window")
	}
	n.Highlights = n.Highlights[:min(len(n.Highlights), maxHighlights)]
	return n, nil
}
