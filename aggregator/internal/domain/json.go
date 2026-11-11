package domain

import (
	"bytes"
	"encoding/json"
)

// Preserve exact JSON numbers when canonical snapshots are read/re-published.
func (e *HazardEvent) UnmarshalJSON(raw []byte) error {
	type plain HazardEvent
	var p plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&p); err != nil {
		return err
	}
	*e = HazardEvent(p)
	return nil
}
