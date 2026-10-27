package raft_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
)

func TestRestartedNodeRecoversTermVoteAndLog(t *testing.T) {
	c := newCluster(t, 3, 21)
	c.WaitLeader()
	for i := 0; i < 5; i++ {
		c.Propose(fmt.Sprint("p", i))
	}
	c.Run(500 * time.Millisecond)
	id := raft.NodeID(1)
	before := c.Nodes[id].Status()
	c.Crash(id)
	c.Restart(id)
	after := c.Nodes[id].Status()
	if after.Term < before.Term || after.LastIndex != before.LastIndex {
		t.Fatalf("lost state: before term=%d last=%d, after term=%d last=%d", before.Term, before.LastIndex, after.Term, after.LastIndex)
	}
	if after.Role != raft.Follower || after.CommitIndex != 0 {
		t.Fatalf("commit index is volatile: %+v", after)
	}
	// It re-applies the log once the leader tells it the commit index.
	want := []string{"p0", "p1", "p2", "p3", "p4"}
	if !c.RunUntil(2*time.Second, func() bool { return slices.Equal(c.Applied(id), want) }) {
		c.Fatalf("replayed %v", c.Applied(id))
	}
}

// A vote must survive a crash: otherwise a restarted node could vote twice in
// one term and elect two leaders.
func TestVoteSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3, 22)
	c.Net.Isolate(1) // keep node 1 from interfering
	c.Crash(1)
	c.Restart(1)
	n := c.Nodes[1]
	n.Handle(raft.Message{From: 2, To: 1, Body: raft.RequestVoteArgs{Term: 9, CandidateID: 2}})
	c.Crash(1)
	c.Restart(1)
	if s := c.Nodes[1].Status(); s.Term != 9 || s.VotedFor != 2 {
		t.Fatalf("after restart term=%d votedFor=%d, want 9/2", s.Term, s.VotedFor)
	}
}

func TestWholeClusterRestartKeepsCommittedData(t *testing.T) {
	c := newCluster(t, 3, 23)
	var want []string
	for i := 0; i < 20; i++ {
		cmd := fmt.Sprint("d", i)
		c.Propose(cmd)
		want = append(want, cmd)
	}
	if !c.RunUntil(2*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("setup did not converge")
	}
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}
	if !c.RunUntil(5*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("after restart: %v", c.Applied(1))
	}
}

// Rolling crashes of the leader while writing must never lose acknowledged
// (committed and applied) commands.
func TestLeaderCrashLoop(t *testing.T) {
	c := newCluster(t, 5, 24)
	var want []string
	for round := 0; round < 6; round++ {
		l := c.WaitLeader()
		cmd := fmt.Sprint("r", round)
		idx := c.Propose(cmd)
		if !c.RunUntil(2*time.Second, func() bool { return c.Nodes[l].Status().LastApplied >= idx }) {
			c.Fatalf("round %d: leader %d never committed", round, l)
		}
		want = append(want, cmd)
		c.Crash(l)
		c.Run(600 * time.Millisecond)
		c.Restart(l)
	}
	if !c.RunUntil(5*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("want %v got %v", want, c.Applied(c.IDs[0]))
	}
}
