package avrowire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/sync/semaphore"
)

var ErrReaderCandidateUnavailable = errors.New("second reader schema has not been discovered")

const readerSlots int64 = 1 << 30

// ReaderSwitch retains a baseline and one discovered reader for the demo.
// The gate covers ingestion through commit; its separate mutex lets discovery
// happen inside an ingestion lease while an operator is waiting to switch.
type ReaderSwitch struct {
	gate     *semaphore.Weighted
	mu       sync.Mutex
	readers  [2]*SourceReader
	keys     [2]string
	active   int
	pageName string
}

func NewReaderSwitch(baseline *SourceReader, pageName string) (*ReaderSwitch, error) {
	if baseline == nil || baseline.Live == nil || baseline.History == nil {
		return nil, fmt.Errorf("baseline live and history readers are required")
	}
	key, err := schemaKey(baseline.Live.codec.Schema())
	if err != nil {
		return nil, err
	}
	return &ReaderSwitch{gate: semaphore.NewWeighted(readerSlots), readers: [2]*SourceReader{baseline, nil}, keys: [2]string{key, ""}, pageName: pageName}, nil
}

func schemaKey(text string) (string, error) {
	var value any
	d := json.NewDecoder(strings.NewReader(text))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		return "", err
	}
	b, err := json.Marshal(value)
	return string(b), err
}

// Observe uses schema contents, not globally assigned Registry ID ordering.
// Both directions must resolve so repeated switching is safe. Writer data is
// validated by ingestion before this is called. Further variants do not replace
// the two cached demo snapshots.
func (s *ReaderSwitch) Observe(writer *Schema) error {
	key, err := schemaKey(writer.Text)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == s.keys[0] || key == s.keys[1] || s.readers[1] != nil {
		return nil
	}
	candidate, err := NewSourceReader(writer.Text, s.pageName)
	if err != nil {
		return err
	}
	for _, pair := range [][2]*Reader{{candidate.Live, s.readers[0].Live}, {s.readers[0].Live, candidate.Live}} {
		if err := checkReadable(pair[0].plan, pair[1].plan, "$", map[schemaPair]bool{}); err != nil {
			return fmt.Errorf("reader candidate is incompatible with baseline: %w", err)
		}
	}
	s.readers[1], s.keys[1] = candidate, key
	return nil
}

type readerLeaseKey struct{ owner *ReaderSwitch }

// Begin pins a reader in the context. Nested client calls reuse the poller's
// lease instead of reacquiring the gate (which could deadlock a waiting toggle).
func (s *ReaderSwitch) Begin(ctx context.Context) (context.Context, *SourceReader, func(), error) {
	if reader, ok := ctx.Value(readerLeaseKey{s}).(*SourceReader); ok {
		return ctx, reader, func() {}, nil
	}
	if err := s.gate.Acquire(ctx, 1); err != nil {
		return ctx, nil, nil, err
	}
	s.mu.Lock()
	reader := s.readers[s.active]
	s.mu.Unlock()
	return context.WithValue(ctx, readerLeaseKey{s}, reader), reader, func() { s.gate.Release(1) }, nil
}

// Toggle waits for all previous-reader ingestion to commit. Cancellation while
// draining leaves the selection unchanged and releases queued ingestion.
func (s *ReaderSwitch) Toggle(ctx context.Context) (string, error) {
	if err := s.gate.Acquire(ctx, readerSlots); err != nil {
		return "", err
	}
	defer s.gate.Release(readerSlots)
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readers[1] == nil {
		return "", ErrReaderCandidateUnavailable
	}
	s.active = 1 - s.active
	return fmt.Sprintf("v%d", s.active+1), nil
}
