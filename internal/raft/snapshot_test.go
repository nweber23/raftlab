package raft_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
	"github.com/nweber23/raftlab/internal/transport"
)

func snapCluster(t *testing.T, size int, seed uint64, threshold int) *raftest.Cluster {
	return raftest.NewCluster(t, raftest.Options{Size: size, Seed: seed, SnapshotThreshold: threshold})
}

func cmds(prefix string, n int) []string {
	var out []string
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprint(prefix, i))
	}
	return out
}

func TestLogIsCompacted(t *testing.T) {
	c := snapCluster(t, 3, 31, 10)
	want := cmds("k", 50)
	for _, w := range want {
		c.Propose(w)
		c.Run(20 * time.Millisecond)
	}
	if !c.RunUntil(2*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("no convergence")
	}
	for _, id := range c.IDs {
		s := c.Nodes[id].Status()
		if s.SnapIndex == 0 || len(s.Log) > 12 {
			t.Fatalf("node %d: snapIndex=%d, %d entries in memory; log not compacted", id, s.SnapIndex, len(s.Log))
		}
	}
}

// A follower that is missing entries the leader already compacted must be
// brought up to date with InstallSnapshot.
func TestInstallSnapshotCatchesUpLaggard(t *testing.T) {
	c := snapCluster(t, 3, 32, 10)
	l := c.WaitLeader()
	laggard := c.IDs[0]
	if laggard == l {
		laggard = c.IDs[1]
	}
	c.Net.Isolate(laggard)
	want := cmds("s", 60)
	for _, w := range want {
		c.Propose(w)
		c.Run(20 * time.Millisecond)
	}
	if c.Nodes[c.Leader()].Status().SnapIndex <= 1 {
		t.Fatal("setup: leader did not compact")
	}
	sawInstall := false
	c.Net.Observe(func(e transport.Event) {
		if e.Msg.Kind() == "InstallSnapshot" {
			sawInstall = true
		}
	})
	c.Net.Heal()
	if !c.RunUntil(3*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("laggard applied %d of %d", len(c.Applied(laggard)), len(want))
	}
	if !sawInstall {
		t.Fatal("catch-up did not use InstallSnapshot")
	}
}

func TestRestartFromSnapshot(t *testing.T) {
	c := snapCluster(t, 3, 33, 10)
	want := cmds("r", 40)
	for _, w := range want {
		c.Propose(w)
		c.Run(20 * time.Millisecond)
	}
	if !c.RunUntil(2*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("no convergence")
	}
	id := raft.NodeID(1)
	c.Crash(id)
	c.Restart(id)
	if c.Nodes[id].Status().SnapIndex == 0 {
		t.Fatal("snapshot not recovered from WAL")
	}
	if got := c.Applied(id); !slices.Equal(got, want[:len(got)]) || len(got) == 0 {
		t.Fatalf("restored state %v", got)
	}
	c.Propose("after")
	want = append(want, "after")
	if !c.RunUntil(3*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("after restart: %v", c.Applied(id))
	}
}

func TestSnapshotsUnderChurn(t *testing.T) {
	c := snapCluster(t, 5, 34, 5)
	c.Net.SetLoss(0.1, 0.1)
	c.Net.SetDelay(0, 20*time.Millisecond)
	for round := 0; round < 40; round++ {
		if l := c.Leader(); l != raft.NoNode {
			c.Nodes[l].Propose([]byte(fmt.Sprint("c", round)))
		}
		switch round % 8 {
		case 2:
			c.Net.Isolate(c.IDs[round%5])
		case 5:
			c.Net.Heal()
		case 7:
			c.Crash(c.IDs[(round+1)%5])
			c.Run(100 * time.Millisecond)
			c.Restart(c.IDs[(round+1)%5])
		}
		c.Run(80 * time.Millisecond)
	}
	c.Net.Heal()
	c.Net.SetLoss(0, 0)
	c.Net.SetDelay(0, 0)
	if !c.RunUntil(10*time.Second, func() bool {
		a := c.Applied(c.IDs[0])
		return len(a) > 0 && converged(c, c.IDs, a)
	}) {
		c.Fatalf("no convergence")
	}
}
