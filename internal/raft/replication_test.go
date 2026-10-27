package raft_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
)

func newCluster(t *testing.T, size int, seed uint64) *raftest.Cluster {
	return raftest.NewCluster(t, raftest.Options{Size: size, Seed: seed})
}

// converged reports whether every listed node has applied exactly want.
func converged(c *raftest.Cluster, ids []raft.NodeID, want []string) bool {
	for _, id := range ids {
		if !slices.Equal(c.Applied(id), want) {
			return false
		}
	}
	return true
}

func TestReplicatesAndCommitsOnAllNodes(t *testing.T) {
	c := newCluster(t, 3, 1)
	var want []string
	for i := 0; i < 10; i++ {
		cmd := fmt.Sprintf("cmd-%d", i)
		c.Propose(cmd)
		want = append(want, cmd)
	}
	if !c.RunUntil(2*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("not converged: %v %v %v", c.Applied(1), c.Applied(2), c.Applied(3))
	}
}

func TestNoopsAreNotApplied(t *testing.T) {
	c := newCluster(t, 3, 2)
	c.WaitLeader()
	c.Run(time.Second)
	for _, id := range c.IDs {
		if len(c.Applied(id)) != 0 {
			t.Fatalf("node %d applied %v from an empty log", id, c.Applied(id))
		}
		if c.Nodes[id].Status().CommitIndex == 0 {
			t.Fatalf("node %d: leader no-op never committed", id)
		}
	}
}

func TestProposeOnFollowerFails(t *testing.T) {
	c := newCluster(t, 3, 3)
	l := c.WaitLeader()
	for _, id := range c.IDs {
		if id == l {
			continue
		}
		if _, _, ok := c.Nodes[id].Propose([]byte("x")); ok {
			t.Fatalf("follower %d accepted a proposal", id)
		}
	}
}

func TestCommitNeedsMajority(t *testing.T) {
	c := newCluster(t, 5, 4)
	l := c.WaitLeader()
	// Leave the leader with one follower: 2 of 5 is no quorum.
	var rest, mine []raft.NodeID
	for _, id := range c.IDs {
		if id == l || (len(mine) == 0 && id != l) {
			mine = append(mine, id)
		} else {
			rest = append(rest, id)
		}
	}
	c.Net.Partition(mine, rest)
	idx, _, _ := c.Nodes[l].Propose([]byte("lonely"))
	c.Run(100 * time.Millisecond) // too short for the others to elect anyone
	if got := c.Nodes[l].Status().CommitIndex; got >= idx {
		t.Fatalf("committed index %d without a majority", idx)
	}
	if len(c.Applied(l)) != 0 {
		t.Fatal("applied an uncommitted command")
	}
}

func TestFollowerCatchesUpAfterPartition(t *testing.T) {
	c := newCluster(t, 3, 5)
	l := c.WaitLeader()
	laggard := c.IDs[0]
	if laggard == l {
		laggard = c.IDs[1]
	}
	c.Net.Isolate(laggard)
	var want []string
	for i := 0; i < 200; i++ { // more than one maxBatch, so catch-up takes several rounds
		cmd := fmt.Sprintf("v%d", i)
		c.Nodes[l].Propose([]byte(cmd))
		want = append(want, cmd)
	}
	c.Run(500 * time.Millisecond)
	if len(c.Applied(laggard)) != 0 {
		t.Fatal("isolated node applied entries")
	}
	c.Net.Heal()
	if !c.RunUntil(3*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("laggard has %d of %d entries", len(c.Applied(laggard)), len(want))
	}
}

// The old leader holds entries nobody else has; after the majority moves on,
// those entries must be erased when it rejoins (conflict repair, §5.3).
func TestConflictingUncommittedEntriesAreOverwritten(t *testing.T) {
	c := newCluster(t, 3, 6)
	old := c.WaitLeader()
	c.Propose("committed")
	c.Run(500 * time.Millisecond)

	c.Net.Isolate(old)
	for i := 0; i < 5; i++ {
		c.Nodes[old].Propose([]byte(fmt.Sprintf("doomed-%d", i)))
	}
	var rest []raft.NodeID
	for _, id := range c.IDs {
		if id != old {
			rest = append(rest, id)
		}
	}
	if !c.RunUntil(3*time.Second, func() bool {
		l := c.Leader()
		return l != raft.NoNode && l != old
	}) {
		c.Fatalf("majority elected nobody")
	}
	fresh := c.Leader()
	c.Nodes[fresh].Propose([]byte("winner"))
	c.Run(500 * time.Millisecond)

	c.Net.Heal()
	want := []string{"committed", "winner"}
	if !c.RunUntil(3*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("applied: %v %v %v", c.Applied(1), c.Applied(2), c.Applied(3))
	}
	l := c.Nodes[old].Status().Log
	for _, e := range l {
		if string(e.Command) != "" && string(e.Command)[:1] == "d" {
			c.Fatalf("doomed entry survived: %+v", e)
		}
	}
}

// §5.4.1: a node missing committed entries must never win an election.
func TestElectionRestriction(t *testing.T) {
	c := newCluster(t, 3, 7)
	l := c.WaitLeader()
	stale := raft.NodeID(1)
	if stale == l {
		stale = 2
	}
	c.Net.Isolate(stale)
	c.Propose("a")
	c.Propose("b")
	c.Run(500 * time.Millisecond)

	// Now the leader dies and the stale node comes back.
	c.Net.Isolate(l)
	c.Net.Heal()
	c.Net.Isolate(l)
	var third raft.NodeID
	for _, id := range c.IDs {
		if id != l && id != stale {
			third = id
		}
	}
	for i := 0; i < 300; i++ { // 3 virtual seconds of repeated elections
		c.Run(10 * time.Millisecond)
		if c.Nodes[stale].Status().Role == raft.Leader {
			c.Fatalf("stale node %d won an election", stale)
		}
	}
	if c.Nodes[third].Status().Role != raft.Leader {
		c.Fatalf("up-to-date node %d never became leader", third)
	}
	c.Nodes[third].Propose([]byte("c"))
	want := []string{"a", "b", "c"}
	if !c.RunUntil(2*time.Second, func() bool { return converged(c, []raft.NodeID{third, stale}, want) }) {
		c.Fatalf("no convergence: %v %v", c.Applied(third), c.Applied(stale))
	}
}

// §5.4.2 / §8: entries from an earlier term become committed once the new
// leader's no-op commits.
func TestPriorTermEntryCommitsViaNoop(t *testing.T) {
	c := newCluster(t, 3, 8)
	l := c.WaitLeader()
	c.Run(500 * time.Millisecond)
	c.Net.SetDelay(10*time.Millisecond, 10*time.Millisecond)
	c.Nodes[l].Propose([]byte("orphan"))
	c.Clock.Advance(10 * time.Millisecond) // followers now hold it, acks are in flight
	c.Net.Isolate(l)                       // ...and the acks die
	c.Run(50 * time.Millisecond)
	if c.Nodes[l].Status().CommitIndex >= c.Nodes[l].Status().LastIndex {
		t.Fatal("setup failed: leader committed before losing contact")
	}
	if !c.RunUntil(3*time.Second, func() bool { x := c.Leader(); return x != raft.NoNode && x != l }) {
		c.Fatalf("no new leader")
	}
	fresh := c.Leader()
	c.Net.Heal()
	c.Net.SetDelay(0, 0)
	if !c.RunUntil(3*time.Second, func() bool { return converged(c, c.IDs, []string{"orphan"}) }) {
		c.Fatalf("orphan never committed: leader %d applied %v", fresh, c.Applied(fresh))
	}
}

// A stale, duplicated AppendEntries must not truncate entries delivered by a
// later one (Figure 2, step 3 wording).
func TestStaleAppendEntriesDoesNotTruncate(t *testing.T) {
	sent := &raftest.Sent{}
	n, err := raft.New(raft.Config{
		ID: 2, Peers: []raft.NodeID{1, 2, 3},
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Clock:             raftest.NewFakeClock(), Transport: sent, Rand: rand.New(rand.NewPCG(1, 1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	es := []raft.Entry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}, {Term: 1, Command: []byte("c")}}
	n.Handle(raft.Message{From: 1, To: 2, Body: raft.AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: es}})
	n.Handle(raft.Message{From: 1, To: 2, Body: raft.AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: es[:1]}})
	if got := n.Status().LastIndex; got != 3 {
		t.Fatalf("log length %d after stale RPC, want 3", got)
	}
	last := sent.Msgs[1].Body.(raft.AppendEntriesReply)
	if !last.Success || last.MatchIndex != 1 {
		t.Fatalf("reply %+v", last)
	}
}

func TestConflictHintSkipsBack(t *testing.T) {
	sent := &raftest.Sent{}
	n, _ := raft.New(raft.Config{
		ID: 2, Peers: []raft.NodeID{1, 2, 3},
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Clock:             raftest.NewFakeClock(), Transport: sent, Rand: rand.New(rand.NewPCG(1, 1)),
		Initial: raft.PersistentState{Term: 2, Log: []raft.Entry{{Term: 1}, {Term: 2}, {Term: 2}, {Term: 2}}},
	})
	n.Handle(raft.Message{From: 1, To: 2, Body: raft.AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 4, PrevLogTerm: 3}})
	n.Handle(raft.Message{From: 1, To: 2, Body: raft.AppendEntriesArgs{Term: 3, LeaderID: 1, PrevLogIndex: 9, PrevLogTerm: 3}})
	r0 := sent.Msgs[0].Body.(raft.AppendEntriesReply)
	r1 := sent.Msgs[1].Body.(raft.AppendEntriesReply)
	if r0.Success || r0.ConflictTerm != 2 || r0.ConflictIndex != 2 {
		t.Errorf("term conflict reply %+v, want term 2 first at 2", r0)
	}
	if r1.Success || r1.ConflictTerm != 0 || r1.ConflictIndex != 5 {
		t.Errorf("short log reply %+v, want index 5", r1)
	}
}

func TestSurvivesLossDuplicationAndDelay(t *testing.T) {
	for seed := uint64(1); seed <= 5; seed++ {
		c := newCluster(t, 5, seed)
		c.Net.SetLoss(0.2, 0.2)
		c.Net.SetDelay(0, 40*time.Millisecond)
		c.WaitLeader()
		var want []string
		for i := 0; i < 30; i++ {
			cmd := fmt.Sprintf("s%d-%d", seed, i)
			// Retry until some leader takes it; the log must then hold it exactly once.
			if c.RunUntil(5*time.Second, func() bool {
				l := c.Leader()
				if l == raft.NoNode {
					return false
				}
				_, _, ok := c.Nodes[l].Propose([]byte(cmd))
				return ok
			}) {
				want = append(want, cmd)
			}
			c.Run(30 * time.Millisecond)
		}
		c.Net.SetLoss(0, 0)
		c.Net.SetDelay(0, 0)
		if !c.RunUntil(10*time.Second, func() bool {
			a := c.Applied(c.IDs[0])
			return len(a) > 0 && converged(c, c.IDs, a)
		}) {
			c.Fatalf("replicas did not converge")
		}
		// Leader changes may lose unacknowledged proposals, but order is kept.
		got := c.Applied(c.IDs[0])
		prev := -1
		for _, g := range got {
			i := slices.Index(want, g)
			if i < 0 || i <= prev {
				c.Fatalf("applied %q out of order or unknown", g)
			}
			prev = i
		}
	}
}
