package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (l *Lab) leader(ctx context.Context) (string, error) {
	var found string
	for _, name := range l.DB {
		c, e := l.connect(ctx, name)
		if e != nil {
			continue
		}
		var replica bool
		e = c.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&replica)
		c.Close(context.Background())
		if e == nil && !replica {
			if found != "" {
				return "", errors.New("multiple writable primaries")
			}
			found = name
		}
	}
	if found == "" {
		return "", errors.New("no writable primary")
	}
	return found, nil
}
func (l *Lab) waitLeader(ctx context.Context, exclude string) (string, error) {
	var name string
	err := until(ctx, func() error {
		n, e := l.leader(ctx)
		if e != nil {
			return e
		}
		if n == exclude {
			return errors.New("former primary still reported")
		}
		name = n
		return nil
	})
	return name, err
}
func (l *Lab) streaming(ctx context.Context, primary string, min int) error {
	c, e := l.connect(ctx, primary)
	if e != nil {
		return e
	}
	defer c.Close(context.Background())
	var n int
	e = c.QueryRow(ctx, "SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND sync_state IN ('sync','quorum')").Scan(&n)
	if e != nil {
		return e
	}
	if n < min {
		return fmt.Errorf("synchronous replicas %d, require %d", n, min)
	}
	return nil
}
func ledgerHash(ctx context.Context, c *pgx.Conn) (string, int, error) {
	rows, err := c.Query(ctx, "SELECT operation_id,payload FROM durability_operations ORDER BY operation_id")
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	h := sha256.New()
	n := 0
	for rows.Next() {
		var id, payload string
		if err = rows.Scan(&id, &payload); err != nil {
			return "", 0, err
		}
		fmt.Fprintf(h, "%s|%s\n", id, payload)
		n++
	}
	return hex.EncodeToString(h.Sum(nil)), n, rows.Err()
}
func (l *Lab) ha(ctx context.Context, d map[string]any) error {
	ready, cancel := context.WithTimeout(ctx, 60*time.Second)
	primary, err := l.waitLeader(ready, "")
	if err == nil {
		err = until(ready, func() error { return l.streaming(ready, primary, 2) })
	}
	cancel()
	if err != nil {
		return err
	}
	c, err := l.connect(ctx, primary)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	settings := map[string]string{}
	for _, name := range []string{"synchronous_standby_names", "synchronous_commit", "fsync", "full_page_writes", "shared_buffers", "max_connections"} {
		var value string
		if err = c.QueryRow(ctx, "SELECT current_setting($1)", name).Scan(&value); err != nil {
			return err
		}
		settings[name] = value
	}
	d["settings"] = settings
	if settings["synchronous_commit"] != "on" || settings["fsync"] != "on" || settings["full_page_writes"] != "on" {
		return errors.New("durability settings weakened")
	}
	if _, err = c.Exec(ctx, "CREATE TABLE durability_operations(operation_id text PRIMARY KEY,payload text NOT NULL)"); err != nil {
		return err
	}
	for i := 0; i < 50; i++ {
		if _, err = c.Exec(ctx, "INSERT INTO durability_operations VALUES($1,'acknowledged')", fmt.Sprintf("op-%03d", i)); err != nil {
			return err
		}
	}
	before, n, err := ledgerHash(ctx, c)
	if err != nil {
		return err
	}
	d["acknowledged_before_loss"] = n
	d["acknowledged_hash"] = before
	var lsn string
	if err = c.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()::text").Scan(&lsn); err != nil {
		return err
	}
	d["primary_flush_lsn_before_loss"] = lsn
	loss := time.Now()
	if err = l.fault(ctx, "kill", primary); err != nil {
		return err
	}
	// The killed member stays down until resumed writes have been proved.
	failover, stop := context.WithTimeout(ctx, 60*time.Second)
	successor, err := l.waitLeader(failover, primary)
	if err == nil {
		err = until(failover, func() error { return l.streaming(failover, successor, 1) })
	}
	stop()
	if err != nil {
		return err
	}
	d["former_primary"] = primary
	d["successor"] = successor
	d["failover_ms"] = time.Since(loss).Milliseconds()
	next, err := l.connect(ctx, successor)
	if err != nil {
		return err
	}
	defer next.Close(context.Background())
	after, count, err := ledgerHash(ctx, next)
	if err != nil {
		return err
	}
	if after != before || count != n {
		return errors.New("acknowledged mutations lost during primary loss")
	}
	d["after_failover_hash"] = after
	if _, err = next.Exec(ctx, "INSERT INTO durability_operations VALUES('after-failover','two surviving copies')"); err != nil {
		return err
	}
	d["resumed_with_third_still_down"] = true
	var standby string
	for _, name := range l.DB {
		if name != primary && name != successor {
			standby = name
		}
	}
	if err = l.fault(ctx, "stop", standby); err != nil {
		return err
	}
	defer func() {
		r, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = l.fault(r, "start", standby)
		_ = l.fault(r, "start", primary)
	}()
	oneCopy, finish := context.WithTimeout(ctx, 2*time.Second)
	single, err := l.connect(oneCopy, successor)
	if err != nil {
		finish()
		return err
	}
	_, writeErr := single.Exec(oneCopy, "INSERT INTO durability_operations VALUES('unknown-one-copy','retry same identity') ON CONFLICT DO NOTHING")
	single.Close(context.Background())
	finish()
	if writeErr == nil {
		return errors.New("unsafe success with only one durable copy")
	}
	d["insufficient_replicas"] = "no success returned; local commit outcome is unknown and must be reconciled"
	if err = l.fault(ctx, "start", standby); err != nil {
		return err
	}
	repair, finishRepair := context.WithTimeout(ctx, 45*time.Second)
	err = until(repair, func() error { return l.streaming(repair, successor, 1) })
	finishRepair()
	if err != nil {
		return err
	}
	if _, err = next.Exec(ctx, "INSERT INTO durability_operations VALUES('unknown-one-copy','retry same identity') ON CONFLICT DO NOTHING"); err != nil {
		return err
	}
	var copies int
	if err = next.QueryRow(ctx, "SELECT count(*) FROM durability_operations WHERE operation_id='unknown-one-copy'").Scan(&copies); err != nil || copies != 1 {
		return errors.New("unknown-outcome retry duplicated identity")
	}
	d["one_copy_retry_count"] = copies
	if err = l.fault(ctx, "stop", "etcd2"); err != nil {
		return err
	}
	if err = l.fault(ctx, "stop", "etcd3"); err != nil {
		return err
	}
	defer func() {
		r, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = l.fault(r, "start", "etcd2")
		_ = l.fault(r, "start", "etcd3")
	}()
	dcsLoss := time.Now()
	expiry, finishExpiry := context.WithTimeout(ctx, 40*time.Second)
	err = until(expiry, func() error {
		probe, e := l.connect(expiry, successor)
		if e != nil {
			return nil
		}
		defer probe.Close(context.Background())
		var replica bool
		e = probe.QueryRow(expiry, "SELECT pg_is_in_recovery()").Scan(&replica)
		if e != nil || replica {
			return nil
		}
		return errors.New("waiting for DCS lease loss to demote primary")
	})
	finishExpiry()
	if err != nil {
		return err
	}
	d["dcs_loss_demote_ms"] = time.Since(dcsLoss).Milliseconds()
	probeCtx, finishProbe := context.WithTimeout(ctx, 2*time.Second)
	probe, e := l.connect(probeCtx, successor)
	if e == nil {
		_, e = probe.Exec(probeCtx, "INSERT INTO durability_operations VALUES('unsafe-dcs','must not acknowledge')")
		probe.Close(context.Background())
	}
	finishProbe()
	if e == nil {
		return errors.New("write acknowledged after observed DCS demotion")
	}
	d["dcs_quorum_loss"] = "primary demoted; post-demotion write could not acknowledge"
	if err = l.fault(ctx, "start", "etcd2"); err != nil {
		return err
	}
	if err = l.fault(ctx, "start", "etcd3"); err != nil {
		return err
	}
	recoverCtx, recoverStop := context.WithTimeout(ctx, 60*time.Second)
	recovered, err := l.waitLeader(recoverCtx, "")
	if err == nil {
		err = until(recoverCtx, func() error { return l.streaming(recoverCtx, recovered, 1) })
	}
	recoverStop()
	if err != nil {
		return err
	}
	final, err := l.connect(ctx, recovered)
	if err != nil {
		return err
	}
	defer final.Close(context.Background())
	if err = final.QueryRow(ctx, "SELECT count(*) FROM durability_operations WHERE payload='acknowledged'").Scan(&count); err != nil || count != 50 {
		return errors.New("DCS recovery lost acknowledged operations")
	}
	d["scope"] = "one three-member Citus/Patroni group, local ledger; not integrated worker routing or distributed decision-state recovery"
	return nil
}
