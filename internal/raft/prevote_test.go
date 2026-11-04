package raft_test

import (
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
)

// A node cut off from the cluster keeps timing out. Without PreVote each
// timeout bumps its term, and on rejoin its inflated term deposes the leader.
func rejoinTerms(t *testing.T, classic bool) (before, after raft.Status, leaderBefore, leaderAfter raft.NodeID) {
	c := raftest.NewCluster(t, raftest.Options{Size: 5, Seed: 7, Classic: classic})
	l := c.WaitLeader()
	c.Run(time.Second)
	victim := raft.NodeID(1)
	if l == victim {
		victim = 2
	}
	c.Net.Isolate(victim)
	c.Run(5 * time.Second)
	before = c.Nodes[l].Status()
	c.Net.Heal()
	c.Run(3 * time.Second)
	return before, c.Nodes[l].Status(), l, c.Leader()
}

func TestPreVoteKeepsPartitionedNodeFromDeposingLeader(t *testing.T) {
	before, after, lb, la := rejoinTerms(t, false)
	if lb != la || after.Term != before.Term || after.Role != raft.Leader {
		t.Fatalf("rejoin disturbed the cluster: leader %d->%d, term %d->%d", lb, la, before.Term, after.Term)
	}
}

func TestWithoutPreVoteRejoiningNodeDisruptsLeader(t *testing.T) {
	before, after, _, _ := rejoinTerms(t, true)
	if after.Term <= before.Term {
		t.Fatalf("expected the classic protocol to be disrupted (term %d -> %d); the PreVote test proves nothing otherwise", before.Term, after.Term)
	}
}

func TestIsolatedNodeDoesNotInflateItsTerm(t *testing.T) {
	c := newCluster(t, 3, 8)
	l := c.WaitLeader()
	victim := raft.NodeID(1)
	if l == victim {
		victim = 2
	}
	term := c.Nodes[victim].Status().Term
	c.Net.Isolate(victim)
	c.Run(5 * time.Second)
	if got := c.Nodes[victim].Status().Term; got != term {
		t.Fatalf("isolated node's term went %d -> %d", term, got)
	}
}

func TestLeaderWithoutQuorumStepsDown(t *testing.T) {
	for _, classic := range []bool{false, true} {
		c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 9, Classic: classic})
		l := c.WaitLeader()
		c.Run(time.Second)
		c.Net.Isolate(l)
		c.Run(600 * time.Millisecond)
		role := c.Nodes[l].Status().Role
		if classic && role != raft.Leader {
			t.Fatalf("classic leader should not notice the partition, is %v", role)
		}
		if !classic && role == raft.Leader {
			t.Fatal("CheckQuorum leader kept leading without a majority")
		}
	}
}

// A write proposed to a minority-side leader must never be acknowledged, and
// after the heal the majority side's history wins.
func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 5, 10)
	old := c.WaitLeader()
	c.Run(time.Second)
	rest := []raft.NodeID{}
	for _, id := range c.IDs {
		if id != old {
			rest = append(rest, id)
		}
	}
	c.Net.Partition([]raft.NodeID{old}, rest)
	idx, _, ok := c.Nodes[old].Propose([]byte("lost"))
	if !ok {
		t.Fatal("leader refused")
	}
	c.Run(2 * time.Second)
	if c.Nodes[old].Status().CommitIndex >= idx {
		t.Fatal("minority leader committed")
	}
	c.Net.Heal()
	c.Run(2 * time.Second)
	for _, a := range c.Applied(old) {
		if a == "lost" {
			t.Fatal("uncommitted write survived the heal")
		}
	}
}
