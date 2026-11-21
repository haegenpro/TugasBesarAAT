package store_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"aggregator/internal/store"
	"github.com/stretchr/testify/require"
)

func TestWorkerReadsReserveConnectionsWithinExistingBudget(t *testing.T) {
	for _, workers := range []string{"1", "2"} {
		t.Run(workers, func(t *testing.T) {
			t.Setenv("M2_WORKER_READS", "true")
			t.Setenv("AGGREGATOR_WORKER_READ_CONNS", workers)
			t.Setenv("DB_MAX_OPEN_CONNS", "6")
			s, err := store.New(context.Background(), "postgres://test:test@127.0.0.1:1/test?sslmode=disable", slog.New(slog.NewJSONHandler(io.Discard, nil)))
			require.NoError(t, err)
			defer s.Close()
			field, err := s.ReadPool(context.Background(), 3)
			require.NoError(t, err)
			defer field.Close()
			generalWorkers := int32(workers[0] - '0')
			require.EqualValues(t, 6, s.Pool.Config().MaxConns+generalWorkers)
			require.EqualValues(t, 3, field.Pool.Config().MaxConns+1)
			require.EqualValues(t, 54, 6*(s.Pool.Config().MaxConns+generalWorkers+field.Pool.Config().MaxConns+1), "six pods during rolling update must fit the existing per-member client budget")
			_, err = s.ReadPool(context.Background(), 1)
			require.Error(t, err)
		})
	}
	t.Run("disabled retains existing pools", func(t *testing.T) {
		t.Setenv("M2_WORKER_READS", "false")
		t.Setenv("DB_MAX_OPEN_CONNS", "6")
		s, err := store.New(context.Background(), "postgres://test:test@127.0.0.1:1/test?sslmode=disable", slog.New(slog.NewJSONHandler(io.Discard, nil)))
		require.NoError(t, err)
		defer s.Close()
		field, err := s.ReadPool(context.Background(), 3)
		require.NoError(t, err)
		defer field.Close()
		require.EqualValues(t, 6, s.Pool.Config().MaxConns)
		require.EqualValues(t, 3, field.Pool.Config().MaxConns)
	})
}
