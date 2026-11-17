package ingestion

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPlatformIngestionBootstrapMigration(t *testing.T) {
	raw := os.Getenv("TEST_POSTGRES_URL")
	if raw == "" {
		t.Skip("isolated PostgreSQL required")
	}
	u, err := url.Parse(raw)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, u.Hostname())
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, raw)
	require.NoError(t, err)
	defer admin.Close(ctx)
	root, err := filepath.Abs("../../..")
	require.NoError(t, err)
	probe, err := os.ReadFile(filepath.Join(root, "store/migrations/0005_replication_probe.sql"))
	require.NoError(t, err)
	for _, scenario := range []string{"fresh", "platform-bootstrap", "foreign-data", "canonical-bootstrap", "wrong-group", "empty-identity", "checksum-mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			name := "platform_ingestion_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			_, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
			require.NoError(t, err)
			t.Cleanup(func() {
				_, e := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
				require.NoError(t, e)
			})
			dsn := *u
			dsn.Path = "/" + name
			c, err := pgx.Connect(ctx, dsn.String())
			require.NoError(t, err)
			defer c.Close(ctx)
			if scenario != "fresh" {
				_, err = c.Exec(ctx, string(probe))
				require.NoError(t, err)
				if scenario != "empty-identity" {
					formation, group := "fixture-ingestion", 0
					if scenario == "canonical-bootstrap" {
						formation = "fixture-storage"
					}
					if scenario == "wrong-group" {
						group = 1
					}
					_, err = c.Exec(ctx, `INSERT INTO platform_storage_identity(singleton,formation,group_id) VALUES(true,$1,$2)`, formation, group)
					require.NoError(t, err)
				}
			}
			if scenario == "foreign-data" {
				_, err = c.Exec(ctx, `CREATE TABLE accepted_data(id int);INSERT INTO accepted_data VALUES(42)`)
				require.NoError(t, err)
			}
			err = Migrate(ctx, c, root, "")
			if scenario == "fresh" || scenario == "platform-bootstrap" || scenario == "checksum-mismatch" {
				require.NoError(t, err)
				require.NoError(t, Migrate(ctx, c, root, ""))
				if scenario == "platform-bootstrap" {
					var formation string
					require.NoError(t, c.QueryRow(ctx, `SELECT formation FROM platform_storage_identity`).Scan(&formation))
					require.Equal(t, "fixture-ingestion", formation)
				}
				if scenario == "checksum-mismatch" {
					_, err = c.Exec(ctx, `UPDATE ingestion_migrations SET checksum='changed' WHERE version='0001_ingestion.sql'`)
					require.NoError(t, err)
					require.ErrorContains(t, Migrate(ctx, c, root, ""), "checksum mismatch")
				}
			} else {
				require.Error(t, err)
				var journal bool
				require.NoError(t, c.QueryRow(ctx, `SELECT to_regclass('ingestion_migrations') IS NOT NULL`).Scan(&journal))
				require.False(t, journal)
				if scenario == "foreign-data" {
					var value int
					require.NoError(t, c.QueryRow(ctx, `SELECT id FROM accepted_data`).Scan(&value))
					require.Equal(t, 42, value)
				}
			}
		})
	}
}
