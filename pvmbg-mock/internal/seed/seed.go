// Package seed builds deterministic historical PVMBG data at boot. The
// volcano_id values here must match the Aggregator's volcano_reference
// seed data (store/init.sql) so mapping can resolve them.
package seed

import (
	"fmt"
	"time"

	"pvmbg-mock/internal/domain"
	"pvmbg-mock/internal/timeutil"
)

var volcanoIDs = []string{
	"merapi", "semeru", "kelud", "agung", "sinabung",
	"krakatau", "bromo", "raung", "dukono", "ibu",
}

var base = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

func f(v float64) *float64 { return &v }

// Build returns 20 deterministic volcanic reports spanning all four alert
// levels, with some records already carrying a confidence_level value (to
// demonstrate old/new record coexistence once the schema toggle is flipped).
func Build() []domain.VolcanicReport {
	levels := []domain.AlertLevel{
		domain.AlertNormal, domain.AlertWaspada, domain.AlertSiaga, domain.AlertAwas,
	}

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
