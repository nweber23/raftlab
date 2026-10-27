package transport_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
	"github.com/nweber23/raftlab/internal/transport"
)

type rig struct {
	clock *raftest.FakeClock
	net   *transport.Network
	got   map[raft.NodeID][]raft.Message
}

func newRig(ids ...raft.NodeID) *rig {
	r := &rig{clock: raftest.NewFakeClock(), got: map[raft.NodeID][]raft.Message{}}
	r.net = transport.NewNetwork(r.clock, rand.New(rand.NewPCG(1, 2)))
	for _, id := range ids {
		r.net.Register(id, func(m raft.Message) { r.got[id] = append(r.got[id], m) })
	}
	return r
}

func (r *rig) send(from, to raft.NodeID) {
	r.net.Send(raft.Message{From: from, To: to, Body: raft.AppendEntriesArgs{Term: 1}})
}

func TestDeliversInOrderWithNoFaults(t *testing.T) {
	r := newRig(1, 2)
	for i := 0; i < 3; i++ {
		r.net.Send(raft.Message{From: 1, To: 2, Body: raft.AppendEntriesArgs{Term: uint64(i)}})
	}
	if len(r.got[2]) != 0 {
		t.Fatal("Send must not deliver synchronously")
	}
	r.clock.Advance(0)
	for i, m := range r.got[2] {
		if m.Term() != uint64(i) {
			t.Fatalf("message %d has term %d", i, m.Term())
		}
	}
	if len(r.got[2]) != 3 {
		t.Fatalf("got %d messages", len(r.got[2]))
	}
}

func TestDelay(t *testing.T) {
	r := newRig(1, 2)
	r.net.SetDelay(100*time.Millisecond, 100*time.Millisecond)
	r.send(1, 2)
	r.clock.Advance(99 * time.Millisecond)
	if len(r.got[2]) != 0 {
		t.Fatal("delivered early")
	}
	r.clock.Advance(time.Millisecond)
	if len(r.got[2]) != 1 {
		t.Fatal("not delivered after delay")
	}
}

func TestDropAll(t *testing.T) {
	r := newRig(1, 2)
	r.net.SetLoss(1, 0)
	r.send(1, 2)
	r.clock.Advance(time.Second)
	if len(r.got[2]) != 0 {
		t.Fatal("message survived DropRate=1")
	}
}

func TestDuplicateAll(t *testing.T) {
	r := newRig(1, 2)
	r.net.SetLoss(0, 1)
	r.send(1, 2)
	r.clock.Advance(time.Second)
	if len(r.got[2]) != 2 {
		t.Fatalf("got %d copies, want 2", len(r.got[2]))
	}
}

func TestPartitionAndHeal(t *testing.T) {
	r := newRig(1, 2, 3)
	r.net.Partition([]raft.NodeID{1, 2}, []raft.NodeID{3})
	r.send(1, 2)
	r.send(1, 3)
	r.send(3, 1)
	r.clock.Advance(0)
	if len(r.got[2]) != 1 || len(r.got[3]) != 0 || len(r.got[1]) != 0 {
		t.Fatalf("partition leaked: %v", r.got)
	}
	r.net.Heal()
	r.send(1, 3)
	r.clock.Advance(0)
	if len(r.got[3]) != 1 {
		t.Fatal("not delivered after heal")
	}
}

func TestPartitionSwallowsInFlight(t *testing.T) {
	r := newRig(1, 2)
	r.net.SetDelay(50*time.Millisecond, 50*time.Millisecond)
	r.send(1, 2)
	r.net.Isolate(2)
	r.clock.Advance(time.Second)
	if len(r.got[2]) != 0 {
		t.Fatal("in-flight message crossed a new partition")
	}
}

func TestCrashDropsAndRestartReceives(t *testing.T) {
	r := newRig(1, 2)
	r.send(1, 2)
	r.net.Crash(2)
	r.clock.Advance(0)
	if len(r.got[2]) != 0 {
		t.Fatal("crashed node received a message")
	}
	r.net.Register(2, func(m raft.Message) { r.got[2] = append(r.got[2], m) })
	r.send(1, 2)
	r.clock.Advance(0)
	if len(r.got[2]) != 1 {
		t.Fatal("restarted node got nothing")
	}
}

func TestObserverSeesFate(t *testing.T) {
	r := newRig(1, 2)
	var evs []transport.Event
	r.net.Observe(func(e transport.Event) { evs = append(evs, e) })
	r.net.Isolate(2)
	r.send(1, 2)
	if len(evs) != 1 || !evs[0].Dropped || evs[0].Reason != "partition" {
		t.Fatalf("events: %+v", evs)
	}
}

func TestSameSeedSameFaults(t *testing.T) {
	run := func() int {
		r := newRig(1, 2)
		r.net.SetLoss(0.5, 0.2)
		for i := 0; i < 200; i++ {
			r.send(1, 2)
		}
		r.clock.Advance(time.Second)
		return len(r.got[2])
	}
	if a, b := run(), run(); a != b || a == 0 {
		t.Fatalf("runs differ or empty: %d vs %d", a, b)
	}
}
