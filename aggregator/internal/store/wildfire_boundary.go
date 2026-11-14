package store

import "context"

// ProcessorStorageBoundary reuses the canonical storage owner's topology and
// distributed-decision checks. It grants no external caller query authority.
func (s *Store) ProcessorStorageBoundary(ctx context.Context, expected string) (string, error) {
	topology, _, err := s.primaryTopology(ctx)
	if err != nil {
		return "", err
	}
	if expected == "" {
		expected = topology
	}
	return topology, s.distributedSettled(ctx, expected)
}
func (s *Store) ConfirmProcessorRecord(ctx context.Context, query string, args ...any) error {
	return s.confirmRecord(ctx, query, args...)
}
