// Package seed builds deterministic historical PVMBG data at boot and the
// live reports generated while the mock runs. The volcano_id values here
// must match the Aggregator's volcano_reference seed data (store/init.sql)
// so mapping can resolve them.
package seed

import (
	"fmt"
	"math/rand"
	"time"

	"pvmbg-mock/internal/domain"
	"pvmbg-mock/internal/timeutil"
)

var volcanoIDs = []string{
	"merapi", "semeru", "kelud", "agung", "sinabung",
	"krakatau", "bromo", "raung", "dukono", "ibu",
}

var base = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

var levels = []domain.AlertLevel{
	domain.AlertNormal, domain.AlertWaspada, domain.AlertSiaga, domain.AlertAwas,
}

func f(v float64) *float64 { return &v }

// NextReport builds the n-th live volcanic report at now. boot (the process
// start time) keeps IDs unique across restarts, since the Aggregator
// deduplicates on report_id. confidence_level is always populated and stays
// hidden on the wire until the schema toggle is on.
func NextReport(boot int64, n int, now time.Time, rng *rand.Rand) domain.VolcanicReport {
	return domain.VolcanicReport{
		ReportID:         fmt.Sprintf("rep-%d-%05d", boot, n),
		VolcanoID:        volcanoIDs[rng.Intn(len(volcanoIDs))],
		AlertLevel:       levels[rng.Intn(len(levels))],
		EruptionCount24h: rng.Intn(5),
		AshColumnHeightM: 300 + float64(rng.Intn(10))*150,
		ReportedAt:       timeutil.New(now),
		ConfidenceLevel:  f(float64(50+5*rng.Intn(10)) / 100),
	}
}

// Build returns 20 deterministic volcanic reports spanning all four alert
// levels, with some records already carrying a confidence_level value (to
// demonstrate old/new record coexistence once the schema toggle is flipped).
func Build() []domain.VolcanicReport {
	reports := make([]domain.VolcanicReport, 0, 20)
	for i := 0; i < 20; i++ {
		volcano := volcanoIDs[i%len(volcanoIDs)]
		level := levels[i%len(levels)]
		reportedAt := base.Add(time.Duration(i) * 30 * time.Hour)

		r := domain.VolcanicReport{
			ReportID:         fmt.Sprintf("rep-%05d", i+1),
			VolcanoID:        volcano,
			AlertLevel:       level,
			EruptionCount24h: i % 5,
			AshColumnHeightM: 300 + float64(i%10)*150,
			ReportedAt:       timeutil.New(reportedAt),
		}
		// Every third record already carries confidence_level in storage,
		// demonstrating that old and new shapes coexist independent of the
		// schema-version toggle, which only controls wire visibility.
		if i%3 == 0 {
			r.ConfidenceLevel = f(0.75 + float64(i%4)*0.05)
		}
		reports = append(reports, r)
	}
	return reports
}
