// migrate is an explicitly offline administrator command, never application startup DDL.
package main

import (
	"aggregator/internal/migrations"
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"time"
)

func main() {
	dir := flag.String("dir", "../store/migrations", "migration directory")
	target := flag.String("target", "postgres", "postgres or candidate citus")
	role := flag.String("runtime-role", "", "existing restricted application role to grant DML")
	offline := flag.Bool("offline", false, "confirm all application writers and publishers are stopped")
	flag.Parse()
	if !*offline {
		fmt.Fprintln(os.Stderr, "migration requires --offline and stopped writers/publishers")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c, err := pgx.Connect(ctx, os.Getenv("MIGRATION_DATABASE_URL"))
	if err == nil {
		defer c.Close(context.Background())
		err = migrations.Up(ctx, c, *dir, *target, *role)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "migration failed:", err)
		os.Exit(1)
	}
	fmt.Println("migration complete:", *target)
}
