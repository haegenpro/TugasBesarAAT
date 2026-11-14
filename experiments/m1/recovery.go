package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// replaceEmptyMember never erases old data. Both volume identities and the
// stopped member are checked before replacing just that member's mount.
func (l *Lab) replaceEmptyMember(ctx context.Context, member string) (map[string]string, error) {
	if l.Suite != "recovery-ha" || !contains(l.DB, member) {
		return nil, errors.New("replacement requires a run-owned recovery member")
	}
	id, err := l.container(ctx, member)
	if err != nil {
		return nil, err
	}
	raw, err := l.cmd(ctx, "docker", "inspect", "--format", "{{json .}}", id)
	if err != nil {
		return nil, err
	}
	var container struct {
		State  struct{ Running, Paused bool }
		Mounts []struct{ Type, Name, Destination string }
	}
	if err = json.Unmarshal(raw, &container); err != nil {
		return nil, err
	}
	if container.State.Running || container.State.Paused {
		return nil, errors.New("failed member is not stopped")
	}
	old := ""
	for _, m := range container.Mounts {
		if m.Destination == "/var/lib/postgresql/data" && m.Type == "volume" {
			old = m.Name
		}
	}
	if old != l.Project+"_"+member {
		return nil, errors.New("expected named data volume missing")
	}
	raw, err = l.cmd(ctx, "docker", "volume", "inspect", "--format", "{{json .}}", old)
	if err != nil {
		return nil, err
	}
	var volume struct {
		Labels  map[string]string
		Driver  string
		Options map[string]string
	}
	if json.Unmarshal(raw, &volume) != nil || !owned(volume.Labels, l.Project, l.ID) || volume.Driver != "local" || len(volume.Options) != 0 {
		return nil, errors.New("refusing replacement of an unowned/nonlocal data volume")
	}
	fresh := l.Project + "_" + member + "-replacement-1"
	raw, err = l.cmd(ctx, "docker", "volume", "ls", "--format", "{{.Name}}")
	if err != nil {
		return nil, err
	}
	for _, name := range strings.Fields(string(raw)) {
		if name == fresh {
			return nil, errors.New("fresh replacement volume already exists")
		}
	}
	_, err = l.cmd(ctx, "docker", "volume", "create", "--label", ownerLabel+"="+ownerValue,
		"--label", "io.itb.role1-m1.run="+l.ID, "--label", "com.docker.compose.project="+l.Project, fresh)
	if err != nil {
		return nil, err
	}
	// Docker volume create is idempotent: another creator may have won after
	// our absence check. Verify the returned resource before mounting it.
	raw, err = l.cmd(ctx, "docker", "volume", "inspect", "--format", "{{json .}}", fresh)
	if err != nil {
		return nil, err
	}
	var created struct {
		Labels  map[string]string
		Driver  string
		Options map[string]string
	}
	if json.Unmarshal(raw, &created) != nil || !owned(created.Labels, l.Project, l.ID) || created.Driver != "local" || len(created.Options) != 0 {
		return nil, errors.New("fresh volume identity/driver differs; refusing mount")
	}
	// Read-only, networkless emptiness check; its exact owned helper is removed
	// even when the Docker client is cancelled. No shell or host path is used.
	helper := l.Project + "-empty-check"
	defer func() {
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		labelsRaw, inspectErr := l.cmd(clean, "docker", "inspect", "--format", "{{json .Config.Labels}}", helper)
		var labels map[string]string
		if inspectErr == nil && json.Unmarshal(labelsRaw, &labels) == nil && owned(labels, l.Project, l.ID) {
			_, _ = l.cmd(clean, "docker", "rm", "-f", helper)
		}
	}()
	raw, err = l.cmd(ctx, "docker", "run", "--name", helper, "--label", ownerLabel+"="+ownerValue,
		"--label", "io.itb.role1-m1.run="+l.ID, "--label", "com.docker.compose.project="+l.Project,
		"--network", "none", "--read-only", "--memory", "32m", "--mount", "type=volume,src="+fresh+",dst=/inspect,readonly",
		"--entrypoint", "/usr/bin/find", l.Image, "/inspect", "-mindepth", "1", "-maxdepth", "1", "-print", "-quit")
	if err != nil || len(strings.TrimSpace(string(raw))) != 0 {
		return nil, errors.New("fresh-volume emptiness check failed")
	}
	configPath := filepath.Join(l.Dir, "compose.json")
	raw, err = os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var cfg map[string]any
	if err = json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	services := cfg["services"].(map[string]any)
	service := services[member].(map[string]any)
	service["volumes"] = []string{member + "-replacement-1:/var/lib/postgresql/data"}
	cfg["volumes"].(map[string]any)[member+"-replacement-1"] = map[string]any{"name": fresh, "external": true}
	if err = writeJSON(configPath, cfg); err != nil {
		return nil, err
	}
	if _, err = l.compose(ctx, "up", "-d", "--no-deps", "--force-recreate", member); err != nil {
		return nil, err
	}
	// Random loopback port changes on recreation.
	raw, err = l.compose(ctx, "port", member, "5432")
	if err != nil {
		return nil, err
	}
	host := strings.TrimSpace(string(raw))
	if _, _, err = net.SplitHostPort(host); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword("postgres", l.Password), Host: host, Path: "/m1", RawQuery: "sslmode=disable&connect_timeout=2"}
	l.DSN[member] = u.String()
	if _, err = l.container(ctx, member); err != nil {
		return nil, err
	}
	// A retained old volume must not be mounted by any running container.
	raw, err = l.cmd(ctx, "docker", "ps", "--filter", "volume="+old, "--format", "{{.ID}}")
	if err != nil || len(strings.TrimSpace(string(raw))) != 0 {
		return nil, errors.New("old data volume remains accessible to a running container")
	}
	return map[string]string{"member": member, "retained_old_volume": old, "fresh_volume": fresh, "empty_before_start": "true"}, nil
}

func (l *Lab) recoveryHA(ctx context.Context, d map[string]any) error {
	d["scope"] = "single three-replica group; no Kubernetes, integrated canonical failover, or distributed decision durability claim"
	d["input_lag_ms"], d["canonical_lag_ms"] = nil, nil
	ready, cancel := context.WithTimeout(ctx, 60*time.Second)
	primary, err := l.waitLeader(ready, "")
	if err == nil {
		err = until(ready, func() error { return l.streaming(ready, primary, 2) })
	}
	cancel()
	if err != nil {
		return err
	}
	raw, err := l.compose(ctx, "exec", "-T", primary, "/opt/patroni/bin/patronictl", "-c", "/tmp/patroni-m1.yml", "show-config")
	if err != nil {
		return err
	}
	config := string(raw)
	if !strings.Contains(config, "synchronous_mode: quorum") || !strings.Contains(config, "synchronous_mode_strict: true") || !strings.Contains(config, "synchronous_node_count: 1") {
		return errors.New("required strict synchronous quorum configuration absent")
	}
	d["patroni_runtime_config"] = config
	c, err := l.connect(ctx, primary)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	if _, err = c.Exec(ctx, "CREATE TABLE durability_operations(operation_id text PRIMARY KEY,payload text NOT NULL)"); err != nil {
		return err
	}
	for i := 0; i < 50; i++ {
		if _, err = c.Exec(ctx, "INSERT INTO durability_operations VALUES($1,$2)", fmt.Sprintf("recovery-%03d", i), fmt.Sprintf("independent-payload-%03d", i)); err != nil {
			return err
		}
	}
	hash, count, err := ledgerHash(ctx, c)
	if err != nil {
		return err
	}
	// Independently calculated UTF-8 lines recovery-NNN|independent-payload-NNN\n.
	if count != 50 || hash != "1e562bee415ca7632f89cc98b93bfd2ab82c34eff2a9eb3f1d7c1013b812e3ea" {
		return errors.New("fixture differs from independent group ledger vector")
	}
	d["acknowledged_before_loss"] = map[string]any{"count": count, "sha256": hash}
	var systemID string
	if err = c.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&systemID); err != nil {
		return err
	}
	d["system_id"] = systemID
	check := func(conn *pgx.Conn, expectedCount int, expectedHash string) error {
		got, n, e := ledgerHash(ctx, conn)
		if e != nil {
			return e
		}
		if n != expectedCount || got != expectedHash {
			return errors.New("acknowledged identities/content changed")
		}
		return nil
	}
	confirm := func(name string) error {
		return until(ctx, func() error {
			conn, e := l.connect(ctx, name)
			if e != nil {
				return e
			}
			defer conn.Close(context.Background())
			var safe bool
			e = conn.QueryRow(ctx, `SELECT current_setting('fsync')='on' AND current_setting('full_page_writes')='on' AND current_setting('synchronous_commit')='on' AND current_setting('synchronous_standby_names') ~ '^ANY 1 ' AND EXISTS(SELECT 1 FROM pg_stat_replication WHERE state='streaming' AND sync_state IN ('sync','quorum') AND flush_lsn>=pg_current_wal_flush_lsn())`).Scan(&safe)
			if e == nil && !safe {
				return errors.New("waiting for two flushed synchronous copies")
			}
			return e
		})
	}
	if err = confirm(primary); err != nil {
		return err
	}
	var wal string
	if err = c.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()::text").Scan(&wal); err != nil {
		return err
	}
	d["before_loss_flush_lsn"] = wal
	loss := time.Now()
	if err = l.fault(ctx, "kill", primary); err != nil {
		return err
	}
	successor, err := l.waitLeader(ctx, primary)
	if err != nil {
		return err
	}
	if err = confirm(successor); err != nil {
		return err
	}
	next, err := l.connect(ctx, successor)
	if err != nil {
		return err
	}
	defer next.Close(context.Background())
	if err = check(next, count, hash); err != nil {
		return err
	}
	if _, err = next.Exec(ctx, "INSERT INTO durability_operations VALUES('survivor-write','acknowledged before replacement')"); err != nil {
		return err
	}
	if err = confirm(successor); err != nil {
		return err
	}
	d["failover_interruption_ms"] = time.Since(loss).Milliseconds()
	d["resumed_before_replacement"] = true
	hash, count, err = ledgerHash(ctx, next)
	if err != nil {
		return err
	}
	rebuild := time.Now()
	replacement, err := l.replaceEmptyMember(ctx, primary)
	if err != nil {
		return err
	}
	d["replacement"] = replacement
	if err = until(ctx, func() error {
		if e := l.streaming(ctx, successor, 2); e != nil {
			return e
		}
		replica, e := l.connect(ctx, primary)
		if e != nil {
			return e
		}
		defer replica.Close(context.Background())
		var actual string
		var recovery bool
		if e = replica.QueryRow(ctx, "SELECT system_identifier::text,pg_is_in_recovery() FROM pg_control_system()").Scan(&actual, &recovery); e != nil {
			return e
		}
		if actual != systemID || !recovery {
			return errors.New("replacement system identity/replica role differs")
		}
		return check(replica, count, hash)
	}); err != nil {
		return err
	}
	d["replica_rebuild_ms"] = time.Since(rebuild).Milliseconds()
	d["restored_count"], d["restored_sha256"] = count, hash
	secondLoss := time.Now()
	if err = l.fault(ctx, "kill", successor); err != nil {
		return err
	}
	second, err := l.waitLeader(ctx, successor)
	if err != nil {
		return err
	}
	if err = confirm(second); err != nil {
		return err
	}
	last, err := l.connect(ctx, second)
	if err != nil {
		return err
	}
	defer last.Close(context.Background())
	if err = check(last, count, hash); err != nil {
		return err
	}
	if _, err = last.Exec(ctx, "INSERT INTO durability_operations VALUES('second-survivor-write','acknowledged after restored protection')"); err != nil {
		return err
	}
	if err = confirm(second); err != nil {
		return err
	}
	d["second_loss_interruption_ms"] = time.Since(secondLoss).Milliseconds()
	d["first_primary"], d["first_successor"], d["second_successor"] = primary, successor, second
	d["second_loss_acknowledged_before_rebuild"] = true
	return nil
}
