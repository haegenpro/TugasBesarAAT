package avrowire

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// EncodeJSON checks ordinary JSON against the registered writer schema before
// framing it. It never falls back to sending unvalidated JSON as Avro.
func EncodeJSON(schema *Schema, value any) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	native, rest, err := schema.JSONCodec.NativeFromTextual(b)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("trailing history JSON")
	}
	return Encode(schema, native)
}
