// Package ingestion owns acquisition, publication and failure state. Its pool
// must point at its own database; it never receives canonical table grants.
package ingestion

import (
	"context"
	"crypto/sha256"
	"embed"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"path/filepath"
	"strings"
)

//go:embed migrations/*.sql
var schema embed.FS

// Migrate is administrator-only. Reuse the exact Role 2 authority/probe SQL in
// this independently owned database; no service startup calls this function.
func Migrate(ctx context.Context, c *pgx.Conn, repositoryRoot, role string) error {
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(726103)`); err != nil {
		return err
	}
	var journal, occupied, bootstrap bool
	if err = tx.QueryRow(ctx, `SELECT to_regclass('public.ingestion_migrations') IS NOT NULL,EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p') AND c.relname<>'platform_storage_identity'),to_regclass('public.platform_storage_identity') IS NOT NULL`).Scan(&journal, &occupied, &bootstrap); err != nil {
		return err
	}
	if occupied && !journal {
		return fmt.Errorf("ingestion requires an empty database or its recognized journal")
	}
	if bootstrap && !journal {
		// Patroni installs the restricted replication probe before application
		// migrations. Recognize only an ingestion coordinator's bootstrap row;
		// a mistaken canonical/service DSN must not acquire ingestion tables.
		var formation string
		var group int
		var singleton bool
		rows, e := tx.Query(ctx, `SELECT singleton,formation,group_id FROM public.platform_storage_identity`)
		if e != nil {
			return e
		}
		count := 0
		for rows.Next() {
			count++
			if e = rows.Scan(&singleton, &formation, &group); e != nil {
				rows.Close()
				return e
			}
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if count != 1 || !singleton || !strings.HasSuffix(formation, "-ingestion") || group != 0 {
			return fmt.Errorf("unrecognized ingestion bootstrap identity")
		}
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS ingestion_migrations(version text PRIMARY KEY,checksum text NOT NULL)`); err != nil {
		return err
	}
	names := []string{"0001_ingestion.sql", "0004_ownership.sql", "0005_replication_probe.sql", "0002_r3.sql", "0003_source_envelopes.sql", "0004_coastal_accounting.sql", "0014_acquisition_dispatch.sql", "0005_dispatch_progress.sql", "0016_acquisition_recovery.sql"}
	embedded := map[string]bool{"0001_ingestion.sql": true, "0002_r3.sql": true, "0003_source_envelopes.sql": true, "0004_coastal_accounting.sql": true, "0005_dispatch_progress.sql": true}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM ingestion_migrations WHERE version<>ALL($1)`, names).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("unknown ingestion migration history")
	}
	for _, name := range names {
		var b []byte
		if embedded[name] {
			b, err = schema.ReadFile("migrations/" + name)
		} else {
			b, err = os.ReadFile(filepath.Join(repositoryRoot, "store", "migrations", name))
		}
		if err != nil {
			return err
		}
		sum := fmt.Sprintf("%x", sha256.Sum256(b))
		var old string
		err = tx.QueryRow(ctx, `SELECT checksum FROM ingestion_migrations WHERE version=$1`, name).Scan(&old)
		if err == nil {
			if old != sum {
				return fmt.Errorf("ingestion migration checksum mismatch: %s", name)
			}
			continue
		}
		if err != pgx.ErrNoRows {
			return err
		}
		if _, err = tx.Exec(ctx, string(b)); err != nil {
			return err
		}
		if name == "0004_ownership.sql" {
			if _, err = tx.Exec(ctx, `INSERT INTO canonical_ownership(scope,source) VALUES('acquisition:BMKG','BMKG')`); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(ctx, `INSERT INTO ingestion_migrations(version,checksum) VALUES($1,$2)`, name, sum); err != nil {
			return err
		}
	}
	if role != "" {
		q := pgx.Identifier{role}.Sanitize()
		_, err = tx.Exec(ctx, `GRANT USAGE ON SCHEMA public TO `+q+`; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO `+q+`; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO `+q+`; REVOKE ALL ON ingestion_migrations,platform_storage_identity FROM `+q+`; GRANT EXECUTE ON FUNCTION platform_replication_status() TO `+q)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
