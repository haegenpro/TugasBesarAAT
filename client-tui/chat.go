package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

const chatMemory = 8

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Question string        `json:"question"`
	History  []chatMessage `json:"history"`
}

type chatResponse struct {
	Answer     string           `json:"answer"`
	References []situationEvent `json:"references"`
	Model      string           `json:"model"`
	DataAsOf   string           `json:"data_as_of"`
	Disclaimer string           `json:"disclaimer"`
	Reason     string           `json:"reason"`
}

func (a *app) chat() {
	a.clear()
	fmt.Println("TANYA AI | situasi kebencanaan dari data BNPB 2 jam terakhir")
	fmt.Println("Ketik pertanyaan lalu Enter. Ketik 'keluar' untuk kembali ke menu.")
	history := []chatMessage{}
	for {
		question := a.prompt("\nAnda")
		switch strings.ToLower(question) {
		case "":
			continue
		case "keluar", "exit", "0":
			return
		}
		fmt.Println("AI sedang menjawab...")
		status, data, err := a.protectedCall(a.chatClient, http.MethodPost, "/situation/chat", chatRequest{Question: question, History: history})
		if err != nil {
			fmt.Println("Gagal:", err)
			continue
		}
		report, answer := chatReport(status, data)
		fmt.Print(report)
		if answer != "" {
			history = append(history, chatMessage{"user", question}, chatMessage{"assistant", answer})
			history = history[max(0, len(history)-chatMemory):]
		}
	}
}

func chatReport(status int, data []byte) (string, string) {
	var r chatResponse
	decoded := json.Unmarshal(data, &r) == nil
	switch {
	case status == http.StatusNotFound:
		return "Chat AI belum diaktifkan di client-api (SITUATION_BASE_URL kosong).\n", ""
	case status != http.StatusOK && decoded && r.Reason != "":
		return fmt.Sprintf("Chat AI tidak tersedia: %s. Pertanyaan bisa dikirim ulang.\n", r.Reason), ""
	case status != http.StatusOK:
		return fmt.Sprintf("Chat AI tidak tersedia: HTTP %d.\n", status), ""
	case !decoded || strings.TrimSpace(r.Answer) == "":
		return "Respons chat AI tidak valid.\n", ""
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\nAI: %s\n", r.Answer)
	if len(r.References) > 0 {
		fmt.Fprintln(&b, "\nRujukan:")
		for _, e := range r.References {
			fmt.Fprintf(&b, "  - %-7s  %-10s  %s | %s | %s\n", e.Severity, e.HazardType, e.AreaName, e.OccurredAt, e.HazardID)
		}
	}
	fmt.Fprintf(&b, "\n[%s | data per %s] %s\n", r.Model, r.DataAsOf, r.Disclaimer)
	return b.String(), r.Answer
}
