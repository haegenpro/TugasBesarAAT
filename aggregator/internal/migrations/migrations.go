// Package migrations is an offline administrator tool. Runtime services never call Up.
package migrations

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// Up supports an empty database or the exact checked-in M1 schema. All steps,
// including Citus conversion, run under one administrator transaction and lock.
func Up(ctx context.Context, c *pgx.Conn, dir, target, runtimeRole string) error {
	if target != "postgres" && target != "citus" {
		return fmt.Errorf("target must be postgres or citus")
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(726102)"); err != nil {
		return err
	}
	var journal, existing, partial bool
	err = tx.QueryRow(ctx, "SELECT to_regclass('public.canonical_schema_migrations') IS NOT NULL,to_regclass('public.hazard_events') IS NOT NULL,to_regclass('public.canonical_entities') IS NOT NULL").Scan(&journal, &existing, &partial)
	if err != nil {
		return err
	}
	baseline, err := os.ReadFile(filepath.Join(dir, "0001_m1.sql"))
	if err != nil {
		return err
	}
	if !journal && partial {
		return fmt.Errorf("unrecognized partial upgrade: canonical tables without migration journal")
	}
	if !journal && existing {
		if err = recognizeM1(ctx, tx, string(baseline)); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS public.canonical_schema_migrations(version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())")
	if err != nil {
		return err
	}
	var unknown int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM public.canonical_schema_migrations WHERE version NOT IN ('0001_m1.sql','0002_canonical.sql','0003_citus.sql','0004_ownership.sql')").Scan(&unknown); err != nil {
		return err
	}
	if unknown != 0 {
		return fmt.Errorf("unsupported migration history")
	}
	files := []string{"0001_m1.sql", "0002_canonical.sql"}
	if target == "citus" {
		files = append(files, "0003_citus.sql")
	}
	files = append(files, "0004_ownership.sql")
	if target == "postgres" {
		var n int
		if err = tx.QueryRow(ctx, "SELECT count(*) FROM public.canonical_schema_migrations WHERE version='0003_citus.sql'").Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("cannot downgrade a Citus target")
		}
	}
	for _, name := range files {
		body, e := os.ReadFile(filepath.Join(dir, name))
		if e != nil {
			return e
		}
		checksum := fmt.Sprintf("%x", sha256.Sum256(body))
		var old string
		e = tx.QueryRow(ctx, "SELECT checksum FROM public.canonical_schema_migrations WHERE version=$1", name).Scan(&old)
		if e == nil {
			if old != checksum {
				return fmt.Errorf("migration checksum mismatch: %s", name)
			}
			continue
		}
		if e != pgx.ErrNoRows {
			return e
		}
		if name == "0003_citus.sql" {
			var version string
			if e = tx.QueryRow(ctx, "SELECT citus_version()").Scan(&version); e != nil {
				return e
			}
			if !strings.HasPrefix(version, "Citus 14.2.0") {
				return fmt.Errorf("unvalidated Citus version %s", version)
			}
		}
		if !(name == "0001_m1.sql" && existing && !journal) {
			if _, e = tx.Exec(ctx, string(body)); e != nil {
				return fmt.Errorf("%s: %w", name, e)
			}
		}
		if _, e = tx.Exec(ctx, "INSERT INTO public.canonical_schema_migrations(version,checksum) VALUES($1,$2)", name, checksum); e != nil {
			return e
		}
	}
	if runtimeRole != "" {
		q := pgx.Identifier{runtimeRole}.Sanitize()
		if _, err = tx.Exec(ctx, "GRANT USAGE ON SCHEMA public TO "+q+"; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO "+q+"; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO "+q); err != nil {
			return err
		}
		// A runtime credential must not be the owner/admin used for migration.
		if _, err = tx.Exec(ctx, "REVOKE ALL ON public.canonical_schema_migrations FROM "+q); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func recognizeM1(ctx context.Context, tx pgx.Tx, sql string) error {
	actual, err := signature(ctx, tx, "public")
	if err != nil {
		return err
	}
	schema := "canonical_baseline_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = tx.Exec(ctx, "CREATE SCHEMA "+q+"; SET LOCAL search_path TO "+q+",pg_catalog"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, sql); err != nil {
		return err
	}
	expected, err := signature(ctx, tx, schema)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("unsupported M1 schema: columns, defaults, constraints or indexes differ from 0001_m1.sql")
	}
	_, err = tx.Exec(ctx, "DROP SCHEMA "+q+" CASCADE; SET LOCAL search_path TO public,pg_catalog")
	return err
}
func signature(ctx context.Context, tx pgx.Tx, schema string) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT 'c:'||c.relname||':'||a.attnum||':'||a.attname||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull||':'||coalesce(pg_get_expr(d.adbin,d.adrelid),'')
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
 WHERE n.nspname=$1 AND c.relname=ANY($2) AND a.attnum>0 AND NOT a.attisdropped
 UNION ALL SELECT 'k:'||c.relname||':'||k.conname||':'||pg_get_constraintdef(k.oid) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=ANY($2)
 UNION ALL SELECT 'i:'||tablename||':'||indexdef FROM pg_indexes WHERE schemaname=$1 AND tablename=ANY($2) ORDER BY 1`, schema, []string{"hazard_events", "hazard_outbox", "volcano_reference", "source_observations", "source_history_cursors", "source_poll_state", "report_notices", "report_lifecycle_events", "report_lifecycle_outbox", "failed_ingestions"})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var s string
		if err = rows.Scan(&s); err != nil {
			return nil, err
		}
		s = strings.ReplaceAll(s, schema+".", "")
		s = strings.ReplaceAll(s, "public.", "")
		out = append(out, s)
	}
	return out, rows.Err()
}
