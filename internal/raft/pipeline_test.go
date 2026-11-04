package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
	"github.com/nweber23/raftlab/internal/transport"
)

// burst proposes n entries at once on a 3-node cluster with a 20ms one-way
// delay and returns how long (virtual) it took every node to apply them, and
// how many log entries the leader put on the wire in total.
func burst(t *testing.T, window, n int) (took time.Duration, entriesSent int) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 101, MaxInflight: window})
	l := c.WaitLeader()
	c.Run(time.Second)
	c.Net.SetDelay(20*time.Millisecond, 20*time.Millisecond)
	c.Net.Observe(func(e transport.Event) {
		if a, ok := e.Msg.Body.(raft.AppendEntriesArgs); ok {
			entriesSent += len(a.Entries)
		}
	})
	var want []string
	for i := 0; i < n; i++ {
		cmd := fmt.Sprint("e", i)
		want = append(want, cmd)
		if _, _, ok := c.Nodes[l].Propose([]byte(cmd)); !ok {
			c.Fatalf("leader refused")
		}
	}
	for ; took < time.Minute; took += raftest.Step {
		if converged(c, c.IDs, want) {
			return took, entriesSent
		}
		c.Run(raftest.Step)
	}
	c.Fatalf("window %d: not converged", window)
	return
}

func TestPipeliningIsFasterThanStopAndWait(t *testing.T) {
	slow, _ := burst(t, 1, 1000)
	fast, _ := burst(t, 8, 1000)
	t.Logf("1000 entries, 40ms round trip: stop-and-wait %v, window 8 %v", slow, fast)
	if fast*2 > slow {
		t.Fatalf("pipelining did not help: %v vs %v", fast, slow)
	}
}

// A proposal used to resend everything the follower had not yet acknowledged,
// so a burst cost O(n * batch) entries on the wire. Each entry should now go
// out about once per follower.
func TestBurstDoesNotResendUnacknowledgedEntries(t *testing.T) {
	const n, followers = 1000, 2
	_, sent := burst(t, 8, n)
	t.Logf("NEW: %d entries on the wire", sent)
	if limit := n * followers * 3 / 2; sent > limit {
		t.Fatalf("%d entries on the wire for %d proposals to %d followers (limit %d)", sent, n, followers, limit)
	}
}

// The leader must never run further ahead of a follower's acknowledgements than
// the window allows.
func TestWindowBoundsOutstandingEntries(t *testing.T) {
	const window = 2 // batches of 64
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 102, MaxInflight: window})
	l := c.WaitLeader()
	c.Run(time.Second)
	c.Net.SetDelay(20*time.Millisecond, 20*time.Millisecond)
	sentTo, ackedBy := map[raft.NodeID]uint64{}, map[raft.NodeID]uint64{}
	var worst uint64
	c.Net.Observe(func(e transport.Event) {
		switch b := e.Msg.Body.(type) {
		case raft.AppendEntriesArgs:
			if len(b.Entries) > 0 {
				sentTo[e.Msg.To] = max(sentTo[e.Msg.To], b.PrevLogIndex+uint64(len(b.Entries)))
			}
			worst = max(worst, sentTo[e.Msg.To]-min(sentTo[e.Msg.To], ackedBy[e.Msg.To]))
		case raft.AppendEntriesReply:
			if b.Success {
				ackedBy[e.Msg.From] = max(ackedBy[e.Msg.From], b.MatchIndex)
			}
		}
	})
	var want []string
	for i := 0; i < 600; i++ {
		want = append(want, fmt.Sprint("w", i))
		c.Nodes[l].Propose([]byte(want[i]))
	}
	if !c.RunUntil(30*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("not converged")
	}
	if limit := uint64(window*64 + 64); worst > limit {
		t.Fatalf("up to %d entries outstanding, window allows %d", worst, limit)
	}
	if worst < 64 {
		t.Fatalf("window never used (max outstanding %d); the test proves nothing", worst)
	}
}

// With loss, duplication and reordering, pipelined entries get lost or arrive
// out of order; the stall rewind and conflict repair must still converge.
func TestPipelinedReplicationSurvivesBadNetwork(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		c := raftest.NewCluster(t, raftest.Options{Size: 5, Seed: 110 + seed})
		c.WaitLeader()
		c.Run(time.Second)
		c.Net.SetLoss(0.2, 0.2)
		c.Net.SetDelay(0, 40*time.Millisecond)
		var want []string
		for i := 0; i < 400; i++ {
			want = append(want, fmt.Sprint("p", i))
			if l := c.Leader(); l != raft.NoNode {
				if _, _, ok := c.Nodes[l].Propose([]byte(want[len(want)-1])); !ok {
					want = want[:len(want)-1]
				}
			} else {
				want = want[:len(want)-1]
			}
			if i%25 == 0 {
				c.Run(raftest.Step)
			}
		}
		c.Net.SetLoss(0, 0)
		c.Net.SetDelay(0, 0)
		// Leaders may change under loss, dropping unreplicated proposals, so
		// compare the replicas with each other rather than with `want`.
		if !c.RunUntil(60*time.Second, func() bool {
			a := c.Applied(c.IDs[0])
			return len(a) > 0 && converged(c, c.IDs, a)
		}) {
			c.Fatalf("replicas never converged")
		}
	}
}

// A burst goes out optimistically and is then lost entirely. Nothing new is
// proposed and the leader keeps its seat, so the only thing that can notice is
// the stall check: after a couple of heartbeats without progress it rewinds
// and resends.
func TestLostPipelinedBatchesAreResent(t *testing.T) {
	c := raftest.NewCluster(t, raftest.Options{Size: 3, Seed: 103})
	l := c.WaitLeader()
	c.Run(time.Second)
	term := c.Nodes[l].Status().Term
	var want []string
	for i := 0; i < 300; i++ {
		want = append(want, fmt.Sprint("x", i))
		c.Nodes[l].Propose([]byte(want[i]))
	}
	c.Net.SetLoss(1, 0) // everything in flight, and every reply, is lost
	c.Run(60 * time.Millisecond)
	c.Net.SetLoss(0, 0)
	if !c.RunUntil(3*time.Second, func() bool { return converged(c, c.IDs, want) }) {
		c.Fatalf("lost batches were never resent: %d %d %d applied", len(c.Applied(1)), len(c.Applied(2)), len(c.Applied(3)))
	}
	if st := c.Nodes[l].Status(); st.Role != raft.Leader || st.Term != term {
		c.Fatalf("leader changed (%v, term %d -> %d); recovery came from somewhere else", st.Role, term, st.Term)
	}
}
