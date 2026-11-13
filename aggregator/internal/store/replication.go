package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"aggregator/internal/canonical"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReplicationConfig describes an existing formation, not a validation result.
// System IDs must be recorded from bootstrap, never inferred from the endpoint
// currently answering during an application retry. DSNs stay in secret env vars.
type ReplicationConfig struct {
	Version   int                `json:"version"`
	Mode      string             `json:"mode"`
	Formation string             `json:"formation"`
	Groups    []ReplicationGroup `json:"groups"`
}

type ReplicationGroup struct {
	ID       int                 `json:"id"`
	SystemID string              `json:"system_id"`
	DSNEnv   string              `json:"dsn_env"`
	Members  []ReplicationMember `json:"members"`
}

type ReplicationMember struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

type replicaStatus struct {
	Member    string `json:"member"`
	State     string `json:"state"`
	SyncState string `json:"sync_state"`
	FlushLSN  string `json:"flush_lsn"`
}

type replicationStatus struct {
	Formation  string          `json:"formation"`
	GroupID    int             `json:"group_id"`
	SystemID   string          `json:"system_id"`
	Member     string          `json:"member"`
	Primary    bool            `json:"primary"`
	Started    string          `json:"started"`
	Timeline   string          `json:"timeline"`
	InsertLSN  string          `json:"insert_lsn"`
	FlushLSN   string          `json:"flush_lsn"`
	Fsync      string          `json:"fsync"`
	FullPages  string          `json:"full_page_writes"`
	SyncCommit string          `json:"synchronous_commit"`
	Standbys   string          `json:"standby_names"`
	Replicas   []replicaStatus `json:"replicas"`
}

type replicatedDurability struct {
	s     *Store
	cfg   ReplicationConfig
	pools []*pgxpool.Pool
}

var memberPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
var envPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
var systemPattern = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
var quorumPattern = regexp.MustCompile(`^ANY\s+([1-2])\s*\((.+)\)$`)

// ConfigureReplication installs the WAL confirmation adapter without contacting
// PostgreSQL. Startup remains possible during an outage; operations fail closed
// until all configured primary groups can provide matching replication evidence.
func (s *Store) ConfigureReplication(ctx context.Context, path string) error {
	if s.Canonical.Durability != nil {
		return errors.New("durability adapter already configured")
	}
	f, err := os.Open(path)
	if err != nil {
		return errors.New("cannot read CANONICAL_DURABILITY_CONFIG")
	}
	defer f.Close()
	var cfg ReplicationConfig
	raw, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil || len(raw) > 64<<10 {
		return errors.New("replication config must fit within 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cfg) != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("invalid CANONICAL_DURABILITY_CONFIG JSON")
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	r := &replicatedDurability{s: s, cfg: cfg}
	for _, group := range cfg.Groups {
		pc, err := pgxpool.ParseConfig(os.Getenv(group.DSNEnv))
		if os.Getenv(group.DSNEnv) == "" || err != nil {
			r.Close()
			return fmt.Errorf("missing or invalid replication DSN for group %d", group.ID)
		}
		pc.MaxConns, pc.MinConns = 2, 0
		pc.ConnConfig.ConnectTimeout = time.Second
		pc.ConnConfig.RuntimeParams["statement_timeout"] = "1500"
		pc.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
		pool, err := pgxpool.NewWithConfig(ctx, pc)
		if err != nil {
			r.Close()
			return fmt.Errorf("cannot configure replication observer for group %d", group.ID)
		}
		r.pools = append(r.pools, pool)
	}
	s.Canonical.Durability = r
	return nil
}

func (cfg ReplicationConfig) validate() error {
	wanted := 1
	if cfg.Mode == "citus" {
		wanted = 5
	}
	if cfg.Version != 1 || (cfg.Mode != "postgres" && cfg.Mode != "citus") || !memberPattern.MatchString(cfg.Formation) || len(cfg.Groups) != wanted {
		return errors.New("replication config requires version 1, a formation, and postgres group 0 or citus groups 0..4")
	}
	names, hosts, systems := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i, group := range cfg.Groups {
		if group.ID != i || !systemPattern.MatchString(group.SystemID) || systems[group.SystemID] || !envPattern.MatchString(group.DSNEnv) || len(group.Members) != 3 {
			return fmt.Errorf("invalid identity, DSN reference or three-member configuration for group %d", i)
		}
		systems[group.SystemID] = true
		for _, member := range group.Members {
			if !memberPattern.MatchString(member.Name) || member.Host == "" || strings.ContainsAny(member.Host, " /:@\n\r\t") || member.Port != 5432 || names[member.Name] || hosts[member.Host] {
				return fmt.Errorf("invalid or reused member in group %d", i)
			}
			names[member.Name], hosts[member.Host] = true, true
		}
	}
	return nil
}

func (r *replicatedDurability) Close() {
	for _, pool := range r.pools {
		pool.Close()
	}
}

func readReplication(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (replicationStatus, error) {
	var status replicationStatus
	var raw []byte
	err := q.QueryRow(ctx, "SELECT public.platform_replication_status()").Scan(&raw)
	if err == nil {
		err = json.Unmarshal(raw, &status)
	}
	return status, err
}

func lsn(raw string) (uint64, error) {
	parts := strings.Split(raw, "/")
	if len(parts) != 2 {
		return 0, errors.New("missing WAL position")
	}
	hi, e1 := strconv.ParseUint(parts[0], 16, 32)
	lo, e2 := strconv.ParseUint(parts[1], 16, 32)
	if e1 != nil || e2 != nil {
		return 0, errors.New("invalid WAL position")
	}
	return hi<<32 | lo, nil
}

// quorum accepts only Patroni's explicit ANY N member list. Wildcards and
// unknown/duplicate members cannot stand in for copies on independent nodes.
func (r *replicatedDurability) quorum(status replicationStatus, group ReplicationGroup) (int, map[string]bool, error) {
	allowed := map[string]bool{}
	for _, member := range group.Members {
		allowed[member.Name] = true
	}
	if !status.Primary || status.Formation != r.cfg.Formation || status.GroupID != group.ID || status.SystemID != group.SystemID || !allowed[status.Member] || status.Started == "" || len(status.Timeline) != 8 || status.Fsync != "on" || status.FullPages != "on" || status.SyncCommit != "on" {
		return 0, nil, errors.New("primary identity or synchronous durability settings unavailable")
	}
	match := quorumPattern.FindStringSubmatch(strings.TrimSpace(status.Standbys))
	if match == nil {
		return 0, nil, errors.New("explicit synchronous quorum required")
	}
	n, _ := strconv.Atoi(match[1])
	candidates := map[string]bool{}
	for _, raw := range strings.Split(match[2], ",") {
		name := strings.Trim(strings.TrimSpace(raw), `"`)
		if !allowed[name] || name == status.Member || candidates[name] {
			return 0, nil, errors.New("unrecognized synchronous member")
		}
		candidates[name] = true
	}
	if len(candidates) < n {
		return 0, nil, errors.New("insufficient synchronous candidates")
	}
	return n, candidates, nil
}

func (r *replicatedDurability) snapshot(ctx context.Context) ([]replicationStatus, error) {
	statuses := make([]replicationStatus, len(r.pools))
	errorsByGroup := make([]error, len(r.pools))
	var wg sync.WaitGroup
	for i, pool := range r.pools {
		wg.Add(1)
		go func(i int, pool *pgxpool.Pool) {
			defer wg.Done()
			statuses[i], errorsByGroup[i] = readReplication(ctx, pool)
			if errorsByGroup[i] == nil {
				_, _, errorsByGroup[i] = r.quorum(statuses[i], r.cfg.Groups[i])
			}
		}(i, pool)
	}
	wg.Wait()
	for _, err := range errorsByGroup {
		if err != nil {
			return nil, canonical.Unavailable()
		}
	}
	return statuses, nil
}

func (r *replicatedDurability) Prepare(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit TO 'on'"); err != nil {
		return canonical.Unavailable()
	}
	if r.cfg.Mode == "citus" {
		if _, err := tx.Exec(ctx, "SET LOCAL citus.multi_shard_commit_protocol TO '2pc'"); err != nil {
			return canonical.Unavailable()
		}
	}
	// Check the actual transaction's backend as well as the observer endpoints.
	status, err := readReplication(ctx, tx)
	if err != nil {
		return canonical.Unavailable()
	}
	if _, _, err = r.quorum(status, r.cfg.Groups[0]); err != nil {
		return canonical.Unavailable()
	}
	topology, err := r.topology(ctx)
	if err != nil {
		return canonical.Unavailable()
	}
	all, err := r.snapshot(ctx)
	if err != nil || !samePrimary(status, all[0]) || r.matchTopology(topology, all) != nil {
		return canonical.Unavailable()
	}
	return nil
}

func samePrimary(a, b replicationStatus) bool {
	return a.SystemID == b.SystemID && a.Member == b.Member && a.Started == b.Started && a.Timeline == b.Timeline
}

// ConfirmStorage captures INSERT positions only after the caller has observed
// its committed records. Waiting for local + synchronous peer FLUSH at those
// positions covers the commit WAL prefix; sampling only current flush positions
// could accidentally confirm a prefix older than the observed commit.
func (r *replicatedDurability) ConfirmStorage(ctx context.Context) error {
	return r.confirmStorage(ctx, nil)
}

func (r *replicatedDurability) confirmStorage(ctx context.Context, anchor *replicationStatus) error {
	ctx, cancel := context.WithTimeout(ctx, canonical.Deadline)
	defer cancel()
	topology, err := r.topology(ctx)
	if err != nil {
		return canonical.Unavailable()
	}
	before, err := r.snapshot(ctx)
	if err != nil {
		return err
	}
	if (anchor != nil && !samePrimary(*anchor, before[0])) || r.matchTopology(topology, before) != nil {
		return canonical.Unavailable()
	}
	targets := make([]uint64, len(before))
	for i := range before {
		if targets[i], err = lsn(before[i].InsertLSN); err != nil || targets[i] == 0 {
			return canonical.Unavailable()
		}
	}
	for {
		after, err := r.snapshot(ctx)
		if err != nil {
			return err
		}
		ready := true
		for i, status := range after {
			if !samePrimary(before[i], status) || before[i].Standbys != status.Standbys {
				return canonical.Unavailable()
			}
			n, candidates, err := r.quorum(status, r.cfg.Groups[i])
			if err != nil {
				return canonical.Unavailable()
			}
			local, err := lsn(status.FlushLSN)
			if err != nil || local < targets[i] {
				ready = false
			}
			copies := map[string]bool{}
			for _, peer := range status.Replicas {
				flush, err := lsn(peer.FlushLSN)
				if err == nil && candidates[peer.Member] && peer.State == "streaming" && (peer.SyncState == "quorum" || peer.SyncState == "sync") && flush >= targets[i] {
					copies[peer.Member] = true
				}
			}
			if len(copies) < n {
				ready = false
			}
		}
		if ready {
			current, err := r.topology(ctx)
			if err != nil || current != topology || r.matchTopology(current, after) != nil {
				return canonical.Unavailable()
			}
			return nil
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return canonical.Unavailable()
		case <-timer.C:
		}
	}
}

func (r *replicatedDurability) Confirm(ctx context.Context, batchID string) error {
	conn, err := r.s.Pool.Acquire(ctx)
	if err != nil {
		return canonical.Unavailable()
	}
	defer conn.Release()
	anchor, err := readReplication(ctx, conn)
	if err != nil {
		return canonical.Unavailable()
	}
	var before, after string
	query := "SELECT jsonb_build_array(fingerprint,result)::text FROM canonical_batches WHERE batch_id=$1"
	if err := conn.QueryRow(ctx, query, batchID).Scan(&before); err != nil {
		return canonical.Unavailable()
	}
	if err := r.confirmStorage(ctx, &anchor); err != nil {
		return err
	}
	if err := conn.QueryRow(ctx, query, batchID).Scan(&after); err != nil || before != after {
		return canonical.Unavailable()
	}
	current, err := readReplication(ctx, conn)
	if err != nil || !current.Primary || !samePrimary(anchor, current) {
		return canonical.Unavailable()
	}
	return nil
}

func (r *replicatedDurability) ConfirmNamespace(ctx context.Context, route, namespace string) error {
	return r.confirmRecord(ctx, "SELECT EXISTS(SELECT 1 FROM canonical_entities WHERE route_key=$1 AND source_namespace=$2)", route, namespace)
}

func (r *replicatedDurability) ConfirmCapture(ctx context.Context, id, state string) error {
	return r.confirmRecord(ctx, "SELECT EXISTS(SELECT 1 FROM platform_audit_captures WHERE capture_id=$1 AND state=$2 AND ($2<>'ready' OR (idle_expires_at>clock_timestamp() AND absolute_expires_at>clock_timestamp())))", id, state)
}

func (r *replicatedDurability) ConfirmOwnership(ctx context.Context, owner canonical.Ownership, released bool) error {
	if released {
		return r.confirmRecord(ctx, "SELECT EXISTS(SELECT 1 FROM canonical_ownership WHERE scope=$1 AND epoch=$2 AND holder IS NULL AND expires_at IS NULL)", owner.Scope, owner.Epoch)
	}
	return r.confirmRecord(ctx, "SELECT EXISTS(SELECT 1 FROM canonical_ownership WHERE scope=$1 AND epoch=$2 AND holder=$3 AND expires_at>clock_timestamp())", owner.Scope, owner.Epoch, owner.Holder)
}

func (r *replicatedDurability) confirmRecord(ctx context.Context, query string, args ...any) error {
	conn, err := r.s.Pool.Acquire(ctx)
	if err != nil {
		return canonical.Unavailable()
	}
	defer conn.Release()
	anchor, err := readReplication(ctx, conn)
	if err != nil {
		return canonical.Unavailable()
	}
	check := func() bool {
		var exists bool
		err := conn.QueryRow(ctx, query, args...).Scan(&exists)
		return err == nil && exists
	}
	if !check() {
		return canonical.Unavailable()
	}
	if err := r.confirmStorage(ctx, &anchor); err != nil {
		return err
	}
	if !check() {
		return canonical.Unavailable()
	}
	current, err := readReplication(ctx, conn)
	if err != nil || !current.Primary || !samePrimary(anchor, current) {
		return canonical.Unavailable()
	}
	return nil
}

// ConfirmTransaction anchors publication/decision evidence to the backend that
// holds the row lock. A connection pool can still contain a former primary;
// checking an unrelated connection to the new primary would not cover its rows.
func (r *replicatedDurability) ConfirmTransaction(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit TO 'on'"); err != nil {
		return canonical.Unavailable()
	}
	anchor, err := readReplication(ctx, tx)
	if err != nil {
		return canonical.Unavailable()
	}
	if err = r.confirmStorage(ctx, &anchor); err != nil {
		return err
	}
	current, err := readReplication(ctx, tx)
	if err != nil || !current.Primary || !samePrimary(anchor, current) {
		return canonical.Unavailable()
	}
	return nil
}

type topologyMember struct {
	Group int
	Host  string
	Port  int
}

func (r *replicatedDurability) validateTopology(raw string) error {
	if r.cfg.Mode == "postgres" {
		if raw != "postgres" {
			return canonical.Unavailable()
		}
		return nil
	}
	var entries [][]json.RawMessage
	if json.Unmarshal([]byte(raw), &entries) != nil || len(entries) != 4 {
		return canonical.Unavailable()
	}
	seen := map[int]bool{}
	for _, entry := range entries {
		var node topologyMember
		if len(entry) != 4 || json.Unmarshal(entry[1], &node.Host) != nil || json.Unmarshal(entry[2], &node.Port) != nil || json.Unmarshal(entry[3], &node.Group) != nil || node.Group < 1 || node.Group > 4 || seen[node.Group] {
			return canonical.Unavailable()
		}
		seen[node.Group] = true
		known := false
		for _, member := range r.cfg.Groups[node.Group].Members {
			known = known || (member.Host == node.Host && member.Port == node.Port)
		}
		if !known {
			return canonical.Unavailable()
		}
	}
	return nil
}

func (r *replicatedDurability) topology(ctx context.Context) (string, error) {
	// The caller can already hold an application transaction/connection. Avoid
	// waiting for another application slot when every reader has one checked out.
	raw, _, err := readPrimaryTopology(ctx, r.pools[0])
	if err != nil {
		return "", err
	}
	return raw, r.validateTopology(raw)
}

func (r *replicatedDurability) matchTopology(raw string, statuses []replicationStatus) error {
	if r.cfg.Mode == "postgres" {
		return nil
	}
	var entries [][]json.RawMessage
	if json.Unmarshal([]byte(raw), &entries) != nil {
		return canonical.Unavailable()
	}
	for _, entry := range entries {
		var host string
		var group int
		if len(entry) != 4 || json.Unmarshal(entry[1], &host) != nil || json.Unmarshal(entry[3], &group) != nil || group < 1 || group >= len(statuses) {
			return canonical.Unavailable()
		}
		matched := false
		for _, member := range r.cfg.Groups[group].Members {
			matched = matched || (member.Host == host && member.Name == statuses[group].Member)
		}
		if !matched {
			return canonical.Unavailable()
		}
	}
	return nil
}

// Promotions may change a primary endpoint within the configured group. Adding,
// deleting or replacing groups is a separate administrator recovery operation.
func (r *replicatedDurability) PermitTopologyChange(ctx context.Context, before, after string) error {
	if r.validateTopology(before) != nil || r.validateTopology(after) != nil {
		return canonical.Unavailable()
	}
	return r.ConfirmStorage(ctx)
}

var _ Durability = (*replicatedDurability)(nil)

// CheckDurability is an ordinary readiness check for the configured adapter,
// not an acceptance-test flag. Legacy development without an adapter retains
// its existing behavior; private canonical writes still require Durability.
func (s *Store) CheckDurability(ctx context.Context) error {
	if adapter, ok := s.Canonical.Durability.(interface {
		ConfirmStorage(context.Context) error
	}); ok {
		return adapter.ConfirmStorage(ctx)
	}
	return nil
}
