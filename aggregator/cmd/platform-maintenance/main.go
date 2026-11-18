// platform-maintenance constructs admin DSNs from Secret env values without
// shell interpolation and runs the existing explicit offline migration tools.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"time"
)

func command(action, namespace string) (string, []string, error) {
	switch action {
	case "canonical":
		return "/usr/local/bin/migrate", []string{"--offline", "--dir", "/repository/store/migrations", "--target", "citus", "--runtime-role", "aggregator_app"}, nil
	case "ingestion":
		return "/usr/local/bin/raw-ingestion", []string{"migrate", "--offline", "/repository", "aggregator_app"}, nil
	case "cutover":
		if namespace == "" {
			return "", nil, errors.New("BMKG namespace required")
		}
		return "/usr/local/bin/input-cutover", []string{"--offline", "BMKG", namespace}, nil
	}
	return "", nil, errors.New("unknown maintenance action")
}

func run() error {
	action := flag.String("action", "", "canonical, ingestion or cutover")
	namespace := flag.String("namespace", "", "agreed BMKG namespace for cutover")
	flag.Parse()
	binary, args, err := command(*action, *namespace)
	if err != nil {
		return err
	}
	for _, key := range []string{"PGHOST", "PGDATABASE", "PGPASSWORD"} {
		if os.Getenv(key) == "" {
			return errors.New("administrator connection environment is incomplete")
		}
	}
	dsn := adminDSN(os.Getenv("PGHOST"), os.Getenv("PGDATABASE"), os.Getenv("PGPASSWORD"))
	key := "MIGRATION_DATABASE_URL"
	if *action == "ingestion" {
		key = "INGESTION_MIGRATION_DATABASE_URL"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), key+"="+dsn)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if cmd.Run() != nil {
		return errors.New("offline maintenance tool failed; inspect retained maintenance evidence")
	}
	return nil
}

func adminDSN(host, database, password string) string {
	u := url.URL{Scheme: "postgresql", Host: net.JoinHostPort(host, "5432"), User: url.UserPassword("postgres", password), Path: "/" + database, RawQuery: "sslmode=disable"}
	return u.String()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Maintenance stopped:", err)
		os.Exit(1)
	}
}
