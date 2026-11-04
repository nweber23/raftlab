package raft_test

import (
	"slices"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
)

// readResult records the outcome of one ReadIndex call, plus what the state
// machine had applied at the moment the read was served.
type readResult struct {
	called  bool
	err     error
	applied []string
}

func startRead(c *raftest.Cluster, id raft.NodeID) (*readResult, error) {
	r := &readResult{}
	err := c.Nodes[id].ReadIndex(func(err error) {
		r.called, r.err = true, err
		if err == nil {
			r.applied = c.Applied(id)
		}
	})
	return r, err
}

func TestReadIndexSeesAcknowledgedWrites(t *testing.T) {
	c := newCluster(t, 3, 71)
	l := c.WaitLeader()
	c.Run(time.Second)
	idx := c.Propose("w1")
	if !c.RunUntil(time.Second, func() bool { return c.Nodes[l].Status().CommitIndex >= idx }) {
		c.Fatalf("write never committed")
	}
	r, err := startRead(c, l)
	if err != nil {
		c.Fatalf("ReadIndex: %v", err)
	}
	if !c.RunUntil(time.Second, func() bool { return r.called }) {
		c.Fatalf("read never completed")
	}
	if r.err != nil || !slices.Equal(r.applied, []string{"w1"}) {
		c.Fatalf("read saw %v (err %v), want [w1]", r.applied, r.err)
	}
}

func TestReadIndexRefusedOffLeaderAndBeforeFirstCommit(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 72})
	c.Net.SetDelay(50*time.Millisecond, 50*time.Millisecond)
	l := c.WaitLeader() // elected, but its no-op needs a 100ms round trip to commit
	if _, err := startRead(c, l); err != raft.ErrNotReady {
		c.Fatalf("fresh leader: err=%v, want ErrNotReady", err)
	}
	for _, id := range c.IDs {
		if id != l {
			if _, err := startRead(c, id); err != raft.ErrNotLeader {
				c.Fatalf("follower %d: err=%v, want ErrNotLeader", id, err)
			}
		}
	}
	c.Run(time.Second)
	if _, err := startRead(c, l); err != nil {
		c.Fatalf("after the no-op committed: %v", err)
	}
}

// The classic leader keeps believing it leads after being cut off. A read it
// accepts must neither return stale data nor return at all; once it learns of
// the new term the read fails.
func TestReadIndexOnDeposedLeaderNeverReturnsStaleData(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 5, Seed: 73, Classic: true})
	old := c.WaitLeader()
	c.Run(time.Second)
	c.Propose("v1")
	c.Run(500 * time.Millisecond)

	c.Net.Isolate(old)
	r, err := startRead(c, old)
	if err != nil {
		c.Fatalf("old leader refused: %v", err)
	}
	if !c.RunUntil(5*time.Second, func() bool { x := c.Leader(); return x != raft.NoNode && x != old }) {
		c.Fatalf("no new leader")
	}
	c.Propose("v2") // acknowledged on the majority side while old still thinks it leads
	c.Run(time.Second)
	if r.called {
		c.Fatalf("isolated leader completed a read: %+v", r)
	}
	if st := c.Nodes[old].Status(); st.Role != raft.Leader {
		c.Fatalf("setup: old leader already stepped down (%v)", st.Role)
	}
	c.Net.Heal()
	if !c.RunUntil(3*time.Second, func() bool { return r.called }) {
		c.Fatalf("pending read was never failed after the heal")
	}
	if r.err != raft.ErrNotLeader || r.applied != nil {
		c.Fatalf("got err=%v applied=%v, want ErrNotLeader", r.err, r.applied)
	}
}

func TestReadIndexSingleNode(t *testing.T) {
	c := newCluster(t, 1, 74)
	l := c.WaitLeader()
	c.Run(100 * time.Millisecond)
	idx := c.Propose("only")
	if !c.RunUntil(time.Second, func() bool { return c.Nodes[l].Status().LastApplied >= idx }) {
		c.Fatalf("write not applied")
	}
	r, err := startRead(c, l)
	if err != nil || !r.called || !slices.Equal(r.applied, []string{"only"}) {
		c.Fatalf("single-node read: err=%v %+v", err, r)
	}
}

func TestStoppingNodeFailsPendingReads(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 75})
	l := c.WaitLeader()
	c.Run(time.Second)
	c.Net.Isolate(l)
	r, err := startRead(c, l)
	if err != nil {
		t.Fatal(err)
	}
	c.Crash(l)
	if !r.called || r.err != raft.ErrNotLeader {
		t.Fatalf("pending read after crash: %+v", r)
	}
}
