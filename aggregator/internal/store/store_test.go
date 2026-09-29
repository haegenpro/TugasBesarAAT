// Store tests require a real PostgreSQL instance (JSONB, unique-constraint
// dedup behavior are not meaningfully faked). Set TEST_DATABASE_URL to run
// them; they are skipped otherwise so unit tests for the rest of the
// service don't require Docker.
package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"aggregator/internal/domain"
	"aggregator/internal/store"
	"aggregator/internal/timeutil"
)

func testStore(t *testing.T) *store.Store {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping store integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := store.New(ctx, url)
	require.NoError(t, err)
	require.NoError(t, s.Ping(ctx))
	require.NoError(t, s.CheckSchema(ctx))
	t.Cleanup(s.Close)
	return s
}

func sampleEvent(source domain.Source, refID string) domain.HazardEvent {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return domain.HazardEvent{
		HazardID:    uuid.NewString(),
		Source:      source,
		SourceRefID: refID,
		HazardType:  domain.HazardSeismic,
		Severity:    domain.SeverityWaspada,
		AreaName:    "Test Area " + refID,
		Latitude:    -6.2,
		Longitude:   106.8,
		OccurredAt:  timeutil.New(now),
		IngestedAt:  timeutil.New(now),
		Attributes:  map[string]interface{}{"magnitude": 5.5},
	}
}

func TestSaveDedup_InsertAndRead(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	e := sampleEvent(domain.SourceBMKG, "store-test-"+uuid.NewString())
	inserted, err := s.SaveDedup(ctx, e, "test-correlation")
	require.NoError(t, err)
	require.True(t, inserted)

	got, err := s.GetByID(ctx, e.HazardID)
	require.NoError(t, err)
	require.Equal(t, e.SourceRefID, got.SourceRefID)
	require.Equal(t, 5.5, got.Attributes["magnitude"])
}

func TestSaveDedup_DuplicateSourceRefID(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	refID := "dedup-test-" + uuid.NewString()
	e1 := sampleEvent(domain.SourceBMKG, refID)
	e2 := sampleEvent(domain.SourceBMKG, refID) // different hazard_id, same (source, source_ref_id)

	inserted1, err := s.SaveDedup(ctx, e1, "test-correlation")
	require.NoError(t, err)
	require.True(t, inserted1)

	inserted2, err := s.SaveDedup(ctx, e2, "test-correlation")
	require.NoError(t, err)
	require.False(t, inserted2, "second insert with same (source, source_ref_id) must be rejected by the unique constraint")
}

func TestJSONB_OldAndNewAttributeShapesCoexist(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	oldShape := sampleEvent(domain.SourcePVMBG, "jsonb-old-"+uuid.NewString())
	oldShape.HazardType = domain.HazardVolcanic
	oldShape.Attributes = map[string]interface{}{"eruption_count_24h": float64(2), "ash_column_height_m": float64(1500)}

	newShape := sampleEvent(domain.SourcePVMBG, "jsonb-new-"+uuid.NewString())
	newShape.HazardType = domain.HazardVolcanic
	newShape.Attributes = map[string]interface{}{"eruption_count_24h": float64(2), "ash_column_height_m": float64(1500), "confidence_level": 0.93}

	_, err := s.SaveDedup(ctx, oldShape, "test-correlation")
	require.NoError(t, err)
	_, err = s.SaveDedup(ctx, newShape, "test-correlation")
	require.NoError(t, err)

	got1, err := s.GetByID(ctx, oldShape.HazardID)
	require.NoError(t, err)
	_, hasConfidence := got1.Attributes["confidence_level"]
	require.False(t, hasConfidence)

	got2, err := s.GetByID(ctx, newShape.HazardID)
	require.NoError(t, err)
	require.Equal(t, 0.93, got2.Attributes["confidence_level"])
}

func TestQuery_Filtering(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	unique := "filter-test-" + uuid.NewString()
	e := sampleEvent(domain.SourceBMKG, unique)
	e.Severity = domain.SeverityAwas
	e.AreaName = unique
	_, err := s.SaveDedup(ctx, e, "test-correlation")
	require.NoError(t, err)

	results, err := s.Query(ctx, store.Filters{Severity: string(domain.SeverityAwas), AreaName: unique})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, e.HazardID, results[0].HazardID)
}

func TestVolcanoReferenceLookup(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	var areaName string
	err := s.Pool.QueryRow(ctx, `SELECT area_name FROM volcano_reference WHERE volcano_id = $1`, "merapi").Scan(&areaName)
	require.NoError(t, err)
	require.Equal(t, "Gunung Merapi", areaName)
}
