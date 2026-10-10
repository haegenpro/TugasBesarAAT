package history

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestArchiveSurvivesRestartAndReplaysBeyondLiveRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 125; n++ {
		if err = a.Append(Record{Entity: "events", ID: string(rune(n + 1000)), Payload: map[string]any{"number": n}}); err != nil {
			t.Fatal(err)
		}
	}
	// Identical seed/live repeats do not create new revisions.
	if err = a.Append(Record{Entity: "events", ID: string(rune(1124)), Payload: map[string]any{"number": 124}}); err != nil {
		t.Fatal(err)
	}
	if err = a.DB.Close(); err != nil {
		t.Fatal(err)
	}
	a, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.DB.Close()
	cursor := int64(0)
	seen := 0
	for {
		page, err := a.Read("events", cursor, 17)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Data {
			if item.Cursor <= cursor {
				t.Fatal("cursor did not advance")
			}
			var p map[string]int
			if err = json.Unmarshal(item.Payload, &p); err != nil {
				t.Fatal(err)
			}
			if p["number"] != seen {
				t.Fatalf("lost/reordered observation: %v", p)
			}
			seen++
			cursor = item.Cursor
		}
		if cursor != page.NextCursor {
			t.Fatal("cursor mismatch")
		}
		if !page.HasMore {
			break
		}
	}
	if seen != 125 {
		t.Fatalf("got %d records", seen)
	}
	empty, err := a.Read("events", cursor, 100)
	if err != nil || len(empty.Data) != 0 || empty.HasMore || empty.NextCursor != cursor {
		t.Fatalf("invalid empty page: %#v %v", empty, err)
	}
}
func TestDependentObservationArchiveIsAtomic(t *testing.T) {
	a, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer a.DB.Close()
	err = a.Append(Record{Entity: "events", ID: "quake", Payload: map[string]any{"id": "quake"}}, Record{Entity: "warnings", ID: "warning", Payload: make(chan int)})
	if err == nil {
		t.Fatal("expected serialization failure")
	}
	page, err := a.Read("events", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Data) != 0 {
		t.Fatal("partially archived dependent observation")
	}
}

func TestImmutableArchiveRejectsChangesAndRollsBackDependents(t *testing.T) {
	a, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer a.DB.Close()
	original := Record{Entity: "events", ID: "existing", Payload: map[string]any{"value": 1}}
	if err = a.AppendImmutable(original); err != nil {
		t.Fatal(err)
	}
	if err = a.AppendImmutable(original); err != nil {
		t.Fatal(err)
	}
	err = a.AppendImmutable(Record{Entity: "events", ID: "new", Payload: map[string]any{"value": 3}}, Record{Entity: "events", ID: "existing", Payload: map[string]any{"value": 2}})
	if err == nil {
		t.Fatal("immutable ID was rewritten")
	}
	page, err := a.Read("events", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Data) != 1 {
		t.Fatalf("partial immutable publication: %#v", page)
	}
	var payload map[string]int
	if err = json.Unmarshal(page.Data[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["value"] != 1 {
		t.Fatalf("changed immutable value: %#v", payload)
	}
}
