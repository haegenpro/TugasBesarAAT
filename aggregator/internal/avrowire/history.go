package avrowire

import (
	"encoding/json"
	"fmt"
	"os"
)

// HistorySchema embeds the live record definition, including custom rules.
func HistorySchema(live, name string) (string, error) {
	var root struct {
		Type  string          `json:"type"`
		Items json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal([]byte(live), &root); err != nil {
		return "", err
	}
	if root.Type != "array" || len(root.Items) == 0 {
		return "", fmt.Errorf("history requires an array source schema")
	}
	fields := []any{
		map[string]any{"name": "data", "type": map[string]any{"type": "array", "items": map[string]any{"type": "record", "name": name + "Item", "fields": []any{map[string]any{"name": "cursor", "type": "long"}, map[string]any{"name": "payload", "type": root.Items}}}}},
		map[string]any{"name": "next_cursor", "type": "long"}, map[string]any{"name": "has_more", "type": "boolean"},
	}
	b, err := json.Marshal(map[string]any{"type": "record", "name": name, "fields": fields})
	return string(b), err
}

// SourceReader pins the same record policy for live and history ingestion.
type SourceReader struct{ Live, History *Reader }

func NewSourceReader(text, pageName string) (*SourceReader, error) {
	live, err := NewReader(text)
	if err != nil {
		return nil, err
	}
	historyText, err := HistorySchema(text, pageName)
	if err != nil {
		return nil, err
	}
	history, err := NewReader(historyText)
	if err != nil {
		return nil, err
	}
	return &SourceReader{Live: live, History: history}, nil
}
func LoadSourceReader(path, pageName string) (*SourceReader, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reader schema %s: %w", path, err)
	}
	r, err := NewSourceReader(string(b), pageName)
	if err != nil {
		return nil, fmt.Errorf("reader schema %s: %w", path, err)
	}
	return r, nil
}
