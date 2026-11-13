package store

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// These checks are authored for the later validation phase; writing them does
// not establish replication or failover correctness.
func replicationFixture() (*replicatedDurability, replicationStatus) {
	group := ReplicationGroup{ID: 0, SystemID: "123456789", DSNEnv: "STORAGE_GROUP_0_URL", Members: []ReplicationMember{
		{Name: "member-a", Host: "member-a.example", Port: 5432},
		{Name: "member-b", Host: "member-b.example", Port: 5432},
		{Name: "member-c", Host: "member-c.example", Port: 5432},
	}}
	r := &replicatedDurability{cfg: ReplicationConfig{Version: 1, Mode: "postgres", Formation: "formation", Groups: []ReplicationGroup{group}}}
	status := replicationStatus{Formation: "formation", GroupID: 0, SystemID: group.SystemID, Member: "member-a", Primary: true,
		Started: "2026-01-01 00:00:00+00", Timeline: "00000001", Fsync: "on", FullPages: "on", SyncCommit: "on",
		Standbys: `ANY 1 ("member-b","member-c")`}
	return r, status
}

func TestReplicationWALPositions(t *testing.T) {
	for _, raw := range []string{"", "0", "0/", "/0", "G/1", "100000000/0", "0/100000000", "0/0/0"} {
		_, err := lsn(raw)
		require.Error(t, err, raw)
	}
	before, err := lsn("0/FFFFFFFF")
	require.NoError(t, err)
	after, err := lsn("1/0")
	require.NoError(t, err)
	require.Equal(t, before+1, after)
	max, err := lsn("FFFFFFFF/FFFFFFFF")
	require.NoError(t, err)
	require.Equal(t, ^uint64(0), max)
}

func TestReplicationRejectsUnsafeQuorum(t *testing.T) {
	r, status := replicationFixture()
	for _, names := range []string{"", "*", "FIRST 1 (member-b)", "ANY 0 (member-b)", "ANY 2 (member-b)",
		"ANY 1 (member-a)", "ANY 1 (member-b,member-b)", "ANY 1 (foreign)", `ANY 1 ("*")`} {
		bad := status
		bad.Standbys = names
		_, _, err := r.quorum(bad, r.cfg.Groups[0])
		require.Error(t, err, names)
	}
	for _, mutate := range []func(*replicationStatus){
		func(s *replicationStatus) { s.Primary = false },
		func(s *replicationStatus) { s.SystemID = "987654321" },
		func(s *replicationStatus) { s.Formation = "replacement" },
		func(s *replicationStatus) { s.GroupID = 1 },
		func(s *replicationStatus) { s.Member = "foreign" },
		func(s *replicationStatus) { s.Fsync = "off" },
		func(s *replicationStatus) { s.FullPages = "off" },
		func(s *replicationStatus) { s.SyncCommit = "local" },
	} {
		bad := status
		mutate(&bad)
		_, _, err := r.quorum(bad, r.cfg.Groups[0])
		require.Error(t, err)
	}
	n, members, err := r.quorum(status, r.cfg.Groups[0])
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, map[string]bool{"member-b": true, "member-c": true}, members)
	status.Standbys = "ANY 1 (member-c)"
	_, _, err = r.quorum(status, r.cfg.Groups[0])
	require.NoError(t, err, "one missing member may leave two functioning copies")
}

func TestReplicationIdentityConfiguration(t *testing.T) {
	r, _ := replicationFixture()
	require.NoError(t, r.cfg.validate())
	r.cfg.Groups[0].SystemID = "RECORD_AFTER_BOOTSTRAP"
	require.Error(t, r.cfg.validate())
	r.cfg.Groups[0].SystemID = "123456789"
	r.cfg.Groups[0].Members[1] = r.cfg.Groups[0].Members[0]
	require.Error(t, r.cfg.validate())
}

func TestReplicationTopologyAllowsPromotionWithinOriginalGroup(t *testing.T) {
	r, _ := replicationFixture()
	r.cfg.Mode = "citus"
	for group := 1; group <= 4; group++ {
		copy := r.cfg.Groups[0]
		copy.ID = group
		copy.SystemID = fmt.Sprint(123456789 + group)
		copy.DSNEnv = fmt.Sprintf("STORAGE_GROUP_%d_URL", group)
		copy.Members = append([]ReplicationMember(nil), copy.Members...)
		for i := range copy.Members {
			copy.Members[i].Name = fmt.Sprintf("group-%d-member-%d", group, i)
			copy.Members[i].Host = copy.Members[i].Name + ".example"
		}
		r.cfg.Groups = append(r.cfg.Groups, copy)
	}
	require.NoError(t, r.cfg.validate())
	entries := [][]any{}
	for group := 1; group <= 4; group++ {
		entries = append(entries, []any{group, r.cfg.Groups[group].Members[0].Host, 5432, group})
	}
	encode := func() string { raw, err := json.Marshal(entries); require.NoError(t, err); return string(raw) }
	require.NoError(t, r.validateTopology(encode()))
	entries[0][1] = r.cfg.Groups[1].Members[1].Host
	require.NoError(t, r.validateTopology(encode()))
	entries[0][1] = r.cfg.Groups[2].Members[0].Host
	require.Error(t, r.validateTopology(encode()), "a member from another group is not a promotion")
	entries[0][1] = r.cfg.Groups[1].Members[0].Host
	entries[0][3] = 2
	require.Error(t, r.validateTopology(encode()), "missing/duplicate groups cannot be recovered as endpoint changes")
}
