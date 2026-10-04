// Package seed builds deterministic historical BMKG data at boot and the
// live events generated while the mock runs.
package seed

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"bmkg-mock/internal/domain"
	"bmkg-mock/internal/timeutil"
)

var regions = []string{
	"Kab. Cianjur", "Kab. Sukabumi", "Kota Padang", "Kab. Bengkulu Utara",
	"Kab. Lombok Timur", "Kota Ambon", "Kab. Sigi", "Kab. Mamuju",
	"Kab. Nabire", "Kota Ternate",
}

var zones = [][]string{
	{"Pesisir Cianjur", "Pesisir Sukabumi"},
	{"Pesisir Padang", "Kep. Mentawai"},
	{"Pesisir Bengkulu"},
	{"Pesisir Lombok Timur"},
	{"Pesisir Ambon"},
}

// Base epoch for deterministic seed timestamps: 2026-08-01T00:00:00Z.
var base = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

// NextEvent builds the n-th live seismic event occurring at now, plus its
// tsunami warning when the event has tsunami potential. boot (the process
// start time) keeps IDs unique across restarts, since the Aggregator
// deduplicates on event_id.
func NextEvent(boot int64, n int, now time.Time, rng *rand.Rand) (domain.SeismicEvent, *domain.TsunamiWarning) {
	// Squaring skews the distribution toward small quakes.
	x := rng.Float64()
	mag := math.Round((3.0+4.5*x*x)*10) / 10
	region := rng.Intn(len(regions))

	ev := domain.SeismicEvent{
		EventID:          fmt.Sprintf("evt-%d-%05d", boot, n),
		Magnitude:        mag,
		DepthKM:          10 + float64(rng.Intn(8))*7.5,
		EpicenterLat:     -6.0 - float64(rng.Intn(10))*0.35,
		EpicenterLon:     104.5 + float64(rng.Intn(12))*1.1,
		RegionName:       regions[region],
		OccurredAt:       timeutil.New(now),
		PotentialTsunami: mag >= 6.0 && rng.Intn(3) != 0,
	}
	if !ev.PotentialTsunami {
		return ev, nil
	}

	level := domain.ThreatWaspada
	switch {
	case mag >= 7.0:
		level = domain.ThreatAwas
	case mag >= 6.5:
		level = domain.ThreatSiaga
	}
	return ev, &domain.TsunamiWarning{
		WarningID:        fmt.Sprintf("warn-%d-%05d", boot, n),
		RelatedEventID:   ev.EventID,
		ThreatLevel:      level,
		AffectedZones:    zones[region%len(zones)],
		EstimatedArrival: timeutil.New(now.Add(45 * time.Minute)),
	}
}

// Build returns 20 deterministic seismic events and a set of tsunami warnings
// related to some of them, spanning magnitudes below 5.0, 5.0-6.5, and >=6.5,
// with and without tsunami potential.
func Build() ([]domain.SeismicEvent, []domain.TsunamiWarning) {
	events := make([]domain.SeismicEvent, 0, 20)
	warnings := make([]domain.TsunamiWarning, 0, 20)

	// Deterministic magnitude cycle covering all three severity bands.
	magnitudes := []float64{
		3.2, 4.8, 5.0, 5.4, 5.9, 6.1, 6.5, 6.8, 7.2, 4.1,
		3.9, 5.2, 6.4, 6.6, 4.4, 5.7, 6.9, 3.5, 5.95, 6.5,
	}

	for i := 0; i < 20; i++ {
		mag := magnitudes[i]
		region := regions[i%len(regions)]
		occurred := base.Add(time.Duration(i) * 36 * time.Hour)
		potentialTsunami := mag >= 6.0 && (i%3 != 2 || i == 8)

		ev := domain.SeismicEvent{
			EventID:          fmt.Sprintf("evt-%05d", i+1),
			Magnitude:        mag,
			DepthKM:          10 + float64(i%8)*7.5,
			EpicenterLat:     -6.0 - float64(i%10)*0.35,
			EpicenterLon:     104.5 + float64(i%12)*1.1,
			RegionName:       region,
			OccurredAt:       timeutil.New(occurred),
			PotentialTsunami: potentialTsunami,
		}
		events = append(events, ev)

		if potentialTsunami {
			level := domain.ThreatWaspada
			switch {
			case mag >= 7.0:
				level = domain.ThreatAwas
			case mag >= 6.5:
				level = domain.ThreatSiaga
			default:
				level = domain.ThreatWaspada
			}
			w := domain.TsunamiWarning{
				WarningID:        fmt.Sprintf("warn-%05d", i+1),
				RelatedEventID:   ev.EventID,
				ThreatLevel:      level,
				AffectedZones:    zones[i%len(zones)],
				EstimatedArrival: timeutil.New(occurred.Add(45 * time.Minute)),
			}
			warnings = append(warnings, w)
		}
	}

	// Ensure at least one explicit example of each threat level exists,
	// beyond whatever the magnitude cycle above already produced.
	extra := []struct {
		idx   int
		level domain.ThreatLevel
	}{
		{12, domain.ThreatWaspada},
		{6, domain.ThreatSiaga},
		{8, domain.ThreatAwas},
	}
	for _, e := range extra {
		ev := events[e.idx]
		id := fmt.Sprintf("warn-extra-%05d", e.idx+1)
		exists := false
		for _, w := range warnings {
			if w.RelatedEventID == ev.EventID && w.ThreatLevel == e.level {
				exists = true
				break
			}
		}
		if !exists {
			warnings = append(warnings, domain.TsunamiWarning{
				WarningID:        id,
				RelatedEventID:   ev.EventID,
				ThreatLevel:      e.level,
				AffectedZones:    zones[e.idx%len(zones)],
				EstimatedArrival: timeutil.New(ev.OccurredAt.Time.Add(30 * time.Minute)),
			})
		}
	}

	return events, warnings
}
