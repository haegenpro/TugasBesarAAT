// Explicit, offline, idempotent schema initialization. Runtime startup never
// acquires a cluster-wide DDL lock or upgrades a live application schema.
package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "migration failed:", e)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, e := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if e != nil {
		return e
	}
	defer c.Close(ctx)
	var exists bool
	if e = c.QueryRow(ctx, `SELECT to_regclass('hazard_events') IS NOT NULL`).Scan(&exists); e != nil {
		return e
	}
	files := []string{"/migrations/m2.sql"}
	if !exists {
		files = append([]string{"/migrations/init.sql"}, files...)
	}
	for _, p := range files {
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		if _, e = c.Exec(ctx, string(b)); e != nil {
			return e
		}
	}
	fmt.Println("Canonical M2 schema ready.")
	return nil
}
