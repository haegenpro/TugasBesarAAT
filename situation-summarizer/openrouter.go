package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const systemPrompt = `Anda adalah analis pusat data BNPB. Tulis ringkasan situasi kebencanaan dalam bahasa Indonesia untuk Tim Lapangan dan Media berdasarkan data JSON dari pengguna.

Aturan:
- Gunakan hanya fakta yang ada di data. Jangan menambah lokasi, angka, korban, atau prediksi yang tidak tertulis.
- Setiap sorotan wajib merujuk satu hazard_id yang ada di notable_events.
- Utamakan kejadian AWAS dan SIAGA, lalu sebutkan sumber data yang berstatus stale atau unavailable.
- Jika data_partial bernilai true, nyatakan bahwa data belum lengkap.
- Kalimat singkat dan faktual. Jawab hanya dengan JSON sesuai skema.`

var summarySchema = map[string]any{
	"type":                 "object",
	"additionalProperties": false,
	"required":             []string{"headline", "overview", "highlights", "source_notes"},
	"properties": map[string]any{
		"headline": map[string]any{"type": "string", "description": "Satu kalimat utama, maksimal 160 karakter."},
		"overview": map[string]any{"type": "string", "description": "Ringkasan 2-4 kalimat."},
		"highlights": map[string]any{
			"type": "array",
			"items": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"hazard_id", "summary"},
				"properties": map[string]any{
					"hazard_id": map[string]any{"type": "string"},
					"summary":   map[string]any{"type": "string"},
				},
			},
		},
		"source_notes": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
	},
}

type OpenRouter struct {
	BaseURL   string
	APIKey    string
	Model     string
	Fallbacks []string
	HTTP      *http.Client
}

type Completion struct {
	Content string
	Model   string
}

type RateLimited struct{ RetryAfter time.Duration }

func (e *RateLimited) Error() string {
	return "openrouter rate limited; retry after " + e.RetryAfter.String()
}

func (o *OpenRouter) Complete(ctx context.Context, input []byte) (Completion, error) {
	body := map[string]any{
		"model": o.Model,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": string(input)},
		},
		"response_format": map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "situation_summary", "strict": true, "schema": summarySchema}},
		"temperature":     0.2,
		"max_tokens":      4000,
		"reasoning":       map[string]any{"effort": "low", "exclude": true},
	}
	if len(o.Fallbacks) > 0 {
		body["models"] = append([]string{o.Model}, o.Fallbacks...)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return Completion{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.BaseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return Completion{}, err
	}
	req.Header.Set("Authorization", "Bearer "+o.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Title", "BNPB Situation Summarizer")
	resp, err := o.HTTP.Do(req)
	if err != nil {
		return Completion{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Completion{}, err
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		delay := time.Minute
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
			delay = time.Duration(s) * time.Second
		}
		return Completion{}, &RateLimited{RetryAfter: delay}
	}
	var out struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Error *struct {
			Message string `json:"message"`
			Code    any    `json:"code"`
		} `json:"error"`
	}
	if err = json.Unmarshal(raw, &out); err != nil {
		return Completion{}, fmt.Errorf("openrouter returned %d with an unreadable body", resp.StatusCode)
	}
	if out.Error != nil {
		return Completion{}, fmt.Errorf("openrouter error %v: %s", out.Error.Code, out.Error.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return Completion{}, fmt.Errorf("openrouter returned %d", resp.StatusCode)
	}
	if len(out.Choices) == 0 || strings.TrimSpace(out.Choices[0].Message.Content) == "" {
		return Completion{}, errors.New("openrouter returned no content")
	}
	return Completion{Content: out.Choices[0].Message.Content, Model: out.Model}, nil
}
