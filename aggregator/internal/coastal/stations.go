package coastal

import (
	"context"
	"sync"
	"time"
)

type StationCache struct {
	Client interface {
		FetchStations(context.Context) ([]Station, error)
	}
	TTL        time.Duration
	MissDelay  time.Duration
	mu         sync.Mutex
	stations   map[string]Station
	fetched    time.Time
	lastMissAt time.Time
}

func (s *StationCache) Lookup(ctx context.Context, id string) (Station, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	station, known := s.stations[id]
	expired := time.Since(s.fetched) > s.TTL
	missRefresh := !known && time.Since(s.lastMissAt) > s.MissDelay
	if s.stations == nil || expired || missRefresh {
		if !known {
			s.lastMissAt = time.Now()
		}
		items, err := s.Client.FetchStations(ctx)
		switch {
		case err == nil:
			s.stations = make(map[string]Station, len(items))
			for _, item := range items {
				s.stations[item.ID] = item
			}
			s.fetched = time.Now()
			station, known = s.stations[id]
		case s.stations == nil || !known:
			return Station{}, err
		}
	}
	if !known {
		return Station{}, ErrUnknownStation
	}
	return station, nil
}
