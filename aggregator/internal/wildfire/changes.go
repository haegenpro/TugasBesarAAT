package wildfire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// MergeDetection applies source changes to the retained detection. Unused source
// fields (e.g. brightness) remain in the original input rather than the mapper.
// The caller validates the resulting detection before committing any state.
func MergeDetection(previous Detection, raw json.RawMessage) (Detection, error) {
	var changes map[string]json.RawMessage
	if err := json.Unmarshal(raw, &changes); err != nil || changes == nil {
		return Detection{}, fmt.Errorf("update changes must be an object")
	}
	base, _ := json.Marshal(previous)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(base, &fields)
	if _, a := changes["frp"]; a {
		if _, b := changes["frp_mw"]; b {
			return Detection{}, fmt.Errorf("update has ambiguous frp aliases")
		}
	}
	for name, value := range changes {
		if name == "frp" {
			name = "frp_mw"
		}
		if _, used := fields[name]; !used {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return Detection{}, fmt.Errorf("update %s cannot be null", name)
		}
		if name == "frp_mw" && len(value) > 0 && value[0] == '"' {
			var text string
			if json.Unmarshal(value, &text) != nil {
				return Detection{}, fmt.Errorf("update frp must be numeric")
			}
			number, err := strconv.ParseFloat(text, 64)
			if err != nil {
				return Detection{}, fmt.Errorf("update frp must be numeric")
			}
			value, err = json.Marshal(number)
			if err != nil {
				return Detection{}, fmt.Errorf("update frp must be finite")
			}
		}
		fields[name] = value
	}
	merged, err := json.Marshal(fields)
	if err != nil {
		return Detection{}, err
	}
	var detection Detection
	err = json.Unmarshal(merged, &detection)
	return detection, err
}
