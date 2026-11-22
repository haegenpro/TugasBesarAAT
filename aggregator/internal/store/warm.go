package store

import "context"

// Warm opens up to n pool connections at the same time and runs a distributed read on each, then
// one real hazard page, so a starting instance does not pay connection setup and first-query cost
// on its first client requests. It stops at ctx's deadline; any error only ends the warm-up early.
func (s *Store) Warm(ctx context.Context, n int) error {
	if n < 1 {
		n = 1
	}
	// Every worker reports exactly once (nil after its read, or the error); successful workers keep
	// their connection until all n have reported, so n distinct connections are opened, not one
	// connection reused n times.
	reported := make(chan error, n)
	release := make(chan struct{})
	for range n {
		go func() {
			conn, e := s.Pool.Acquire(ctx)
			if e != nil {
				reported <- e
				return
			}
			defer conn.Release()
			var count int
			e = conn.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM hazard_events LIMIT 50) x`).Scan(&count)
			reported <- e
			if e != nil {
				return
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
		}()
	}
	var first error
	for range n {
		if e := <-reported; e != nil && first == nil {
			first = e
		}
	}
	close(release)
	if first != nil {
		return first
	}
	_, e := s.QueryPage(ctx, Filters{}, PageRequest{Limit: 50})
	return e
}
