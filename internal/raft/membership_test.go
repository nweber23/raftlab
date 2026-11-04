package raft_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
)

type ids = []raft.NodeID

// startChange asks whichever node leads to move to voters, retrying while the
// leader is busy or newly elected. It returns once a leader accepted the change.
func startChange(c *raftest.Cluster, voters ids) {
	c.T.Helper()
	started := false
	if !c.RunUntil(10*time.Second, func() bool {
		if l := c.Leader(); l != raft.NoNode && !started {
			_, err := c.Nodes[l].ChangeMembership(voters)
			started = err == nil
		}
		return started
	}) {
		c.Fatalf("no leader accepted the change to %v", voters)
	}
}

// settled reports whether every node in voters runs the committed, non-joint
// configuration voters.
func settled(c *raftest.Cluster, voters ids) bool {
	for _, id := range voters {
		st := c.Nodes[id].Status()
		if st.Config.Joint() || !slices.Equal(st.Config.Voters, voters) || st.ConfigIndex > st.CommitIndex {
			return false
		}
	}
	return true
}

func changeTo(c *raftest.Cluster, voters ids) {
	c.T.Helper()
	startChange(c, voters)
	if !c.RunUntil(10*time.Second, func() bool { return settled(c, voters) }) {
		c.Fatalf("membership never settled on %v: %+v", voters, c.Nodes[voters[0]].Status().Config)
	}
}

func TestAddServerCatchesUpAndVotes(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 61, SnapshotThreshold: 10})
	want := cmds("a", 25) // enough to force a snapshot, so the newcomer needs InstallSnapshot
	for _, w := range want {
		c.Propose(w)
		c.Run(20 * time.Millisecond)
	}
	c.AddNode(4)
	changeTo(c, ids{1, 2, 3, 4})
	want = append(want, cmds("b", 5)...)
	for _, w := range cmds("b", 5) {
		c.Propose(w)
	}
	if !c.RunUntil(5*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("not converged: %v %v %v %v", len(c.Applied(1)), len(c.Applied(2)), len(c.Applied(3)), len(c.Applied(4)))
	}
	// The new server counts: with 4 voters a quorum is 3, so two crashes stall commits.
	l := c.Leader()
	crashed := 0
	for _, id := range c.IDs {
		if id != l && crashed < 2 {
			c.Crash(id)
			crashed++
		}
	}
	idx, _, _ := c.Nodes[l].Propose([]byte("stalls"))
	c.Run(2 * time.Second)
	if c.Nodes[l].Status().CommitIndex >= idx {
		c.Fatalf("committed with only 2 of 4 voters up")
	}
}

func TestRemoveFollowersKeepsClusterStable(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 5, Seed: 62})
	l := c.WaitLeader()
	var keep ids
	var gone ids
	for _, id := range c.IDs {
		if id == l || len(keep) < 3 {
			keep = append(keep, id)
		} else {
			gone = append(gone, id)
		}
	}
	slices.Sort(keep)
	changeTo(c, keep)
	term := c.Nodes[c.Leader()].Status().Term
	c.Run(8 * time.Second) // removed servers keep running and keep timing out
	if got := c.Nodes[c.Leader()].Status().Term; got != term {
		c.Fatalf("removed servers %v disrupted the cluster: term %d -> %d", gone, term, got)
	}
	c.Propose("after")
	if !c.RunUntil(3*time.Second, func() bool { return converged(c, keep, []string{"after"}) }) {
		c.Fatalf("remaining members did not apply the write")
	}
	// Two of the three remaining can still form a quorum.
	for _, id := range keep {
		if id != c.Leader() {
			c.Crash(id)
			break
		}
	}
	c.Propose("with-two")
	if !c.RunUntil(3*time.Second, func() bool { return c.Nodes[c.Leader()].Status().CommitIndex >= c.Nodes[c.Leader()].Status().LastIndex }) {
		c.Fatalf("2 of 3 remaining voters should still commit")
	}
}

func TestLeaderRemovesItself(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 63})
	c.Propose("before")
	old := c.Leader()
	var rest ids
	for _, id := range c.IDs {
		if id != old {
			rest = append(rest, id)
		}
	}
	changeTo(c, rest)
	if c.Nodes[old].Status().Role == raft.Leader {
		c.Fatalf("removed leader %d still leads", old)
	}
	if !c.RunUntil(5*time.Second, func() bool { l := c.Leader(); return l != raft.NoNode && l != old }) {
		c.Fatalf("remaining members elected nobody")
	}
	c.Propose("after")
	if !c.RunUntil(3*time.Second, func() bool { return converged(c, rest, []string{"before", "after"}) }) {
		c.Fatalf("remaining members: %v %v", c.Applied(rest[0]), c.Applied(rest[1]))
	}
}

func TestReplaceEveryServer(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 64, SnapshotThreshold: 8})
	want := cmds("x", 20)
	for _, w := range want {
		c.Propose(w)
	}
	for _, id := range (ids{4, 5, 6}) {
		c.AddNode(id)
	}
	changeTo(c, ids{4, 5, 6})
	for _, id := range (ids{1, 2, 3}) {
		c.Crash(id)
	}
	if !c.RunUntil(5*time.Second, func() bool { return converged(c, ids{4, 5, 6}, want) }) {
		c.Fatalf("new generation missing data: %d %d %d", len(c.Applied(4)), len(c.Applied(5)), len(c.Applied(6)))
	}
	c.Propose("new-era")
	want = append(want, "new-era")
	if !c.RunUntil(3*time.Second, func() bool { return converged(c, ids{4, 5, 6}, want) }) {
		c.Fatalf("new generation cannot commit")
	}
}

// During C(old,new) both halves need a majority. The leader is cut off from the
// rest of the old cluster but can reach the new servers, so the new half alone
// has a quorum. That must not be enough to commit the change.
func TestJointConsensusNeedsBothMajorities(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 65})
	l := c.WaitLeader()
	c.Run(500 * time.Millisecond)
	c.AddNode(4)
	c.AddNode(5)
	var others ids
	for _, id := range c.IDs[:3] {
		if id != l {
			others = append(others, id)
		}
	}
	target := ids{l, 4, 5}
	slices.Sort(target)
	c.Net.Partition(target, others)
	if _, err := c.Nodes[l].ChangeMembership(target); err != nil { // before the leader notices the split
		c.Fatalf("%v", err)
	}
	idx := c.Nodes[l].Status().ConfigIndex
	c.Run(100 * time.Millisecond) // long enough to replicate to 4 and 5, short of CheckQuorum
	st := c.Nodes[l].Status()
	if st.Role != raft.Leader || st.LastIndex < idx {
		c.Fatalf("setup went wrong: %+v", st.Role)
	}
	if st.CommitIndex >= idx {
		c.Fatalf("C(old,new) committed with only the new half reachable")
	}
	c.Net.Heal()
	changeTo(c, target) // whichever history survives, the change can be redone and completes
}

func TestMembershipErrors(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 66})
	l := c.WaitLeader()
	for _, id := range c.IDs {
		if id != l {
			if _, err := c.Nodes[id].ChangeMembership(ids{1, 2}); err != raft.ErrNotLeader {
				t.Fatalf("follower: err=%v", err)
			}
		}
	}
	if _, err := c.Nodes[l].ChangeMembership(nil); err != raft.ErrBadConfig {
		t.Fatalf("empty: err=%v", err)
	}
	if _, err := c.Nodes[l].ChangeMembership(ids{0, 1}); err != raft.ErrBadConfig {
		t.Fatalf("zero id: err=%v", err)
	}
	c.Run(time.Second)
	c.AddNode(4)
	if _, err := c.Nodes[l].ChangeMembership(ids{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Nodes[l].ChangeMembership(ids{1, 2}); err != raft.ErrConfigPending {
		t.Fatalf("second change while one is running: err=%v", err)
	}
}

// Membership is part of the log and of snapshots, so a restart (even of every
// node) must come back with the same cluster, not the original -peers list.
func TestMembershipSurvivesRestart(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 67, SnapshotThreshold: 6})
	c.AddNode(4)
	changeTo(c, ids{1, 2, 4})
	for _, w := range cmds("m", 15) { // compact config into snapshots
		c.Propose(w)
		c.Run(20 * time.Millisecond)
	}
	c.Run(time.Second)
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}
	c.WaitLeader()
	c.Run(2 * time.Second)
	if !settled(c, ids{1, 2, 4}) {
		c.Fatalf("after restart: %+v %+v", c.Nodes[1].Status().Config, c.Nodes[4].Status().Config)
	}
	if st := c.Nodes[3].Status(); st.Config.Has(3) && st.Role != raft.Follower {
		c.Fatalf("removed node 3 is %v", st.Role)
	}
}

func TestMembershipChurn(t *testing.T) {
	for seed := uint64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprint("seed-", seed), func(t *testing.T) { membershipChurn(t, seed) })
	}
}

// Servers come and go, nodes crash and the network drops traffic, while
// clients keep proposing; Run checks election safety and log agreement on every
// step, and at the end every surviving member must hold every acknowledged entry.
func membershipChurn(t *testing.T, seed uint64) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: seed, SnapshotThreshold: 12})
	members := ids{1, 2, 3}
	next := raft.NodeID(4)
	for round := 0; round < 6; round++ {
		// Grow or shrink, alternating, never below 3 or above 5.
		var target ids
		if len(members) < 5 && (round%2 == 0 || len(members) == 3) {
			c.AddNode(next)
			target = append(slices.Clone(members), next)
			next++
		} else {
			target = slices.Clone(members)[1:] // drop the lowest ID
		}
		slices.Sort(target)
		c.Net.SetLoss(0.05, 0.05)
		startChange(c, target)
		for i := 0; i < 5; i++ {
			cmd := fmt.Sprintf("r%d-%d", round, i)
			if l := c.Leader(); l != raft.NoNode {
				c.Nodes[l].Propose([]byte(cmd))
			}
			c.Run(100 * time.Millisecond)
		}
		if round == 3 { // crash and restart a current member mid-churn
			victim := target[len(target)-1]
			c.Crash(victim)
			c.Run(500 * time.Millisecond)
			c.Restart(victim)
		}
		c.Net.SetLoss(0, 0)
		if !c.RunUntil(15*time.Second, func() bool { return settled(c, target) }) {
			c.Fatalf("round %d: never settled on %v", round, target)
		}
		members = target
	}
	c.Run(3 * time.Second)
	// Everything any leader applied is applied identically by all members.
	ref := c.Applied(members[0])
	for _, id := range members {
		if !slices.Equal(c.Applied(id), ref) {
			c.Fatalf("members diverged: node %d has %d entries, node %d has %d", members[0], len(ref), id, len(c.Applied(id)))
		}
	}
	if len(ref) == 0 {
		c.Fatalf("nothing was ever committed")
	}
}
