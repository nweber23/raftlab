package raft_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
	"github.com/nweber23/raftlab/internal/transport"
)

// electionRig is a cluster over the manually-delivered Memory transport.
type electionRig struct {
	clock *raftest.FakeClock
	net   *transport.Memory
	nodes map[raft.NodeID]*raft.Node
}

func newElectionRig(t *testing.T, size int, seed uint64) *electionRig {
	t.Helper()
	r := &electionRig{clock: raftest.NewFakeClock(), net: transport.NewMemory(), nodes: map[raft.NodeID]*raft.Node{}}
	var peers []raft.NodeID
	for i := 1; i <= size; i++ {
		peers = append(peers, raft.NodeID(i))
	}
	for _, id := range peers {
		n, err := raft.New(raft.Config{
			ID: id, Peers: peers,
			ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval: 50 * time.Millisecond,
			Clock:             r.clock, Transport: r.net, Rand: rand.New(rand.NewPCG(seed, uint64(id))),
		})
		if err != nil {
			t.Fatal(err)
		}
		r.nodes[id] = n
		r.net.Register(id, n.Handle)
	}
	for _, n := range r.nodes {
		n.Start()
	}
	return r
}

// run advances virtual time in 10ms steps, delivering messages after each.
func (r *electionRig) run(d time.Duration) {
	for ; d > 0; d -= 10 * time.Millisecond {
		r.clock.Advance(10 * time.Millisecond)
		r.net.DeliverAll()
	}
}

func (r *electionRig) leaders() (ids []raft.NodeID) {
	for id, n := range r.nodes {
		if n.Status().Role == raft.Leader {
			ids = append(ids, id)
		}
	}
	return ids
}

func TestElectsSingleLeader(t *testing.T) {
	r := newElectionRig(t, 3, 1)
	r.run(time.Second)
	ls := r.leaders()
	if len(ls) != 1 {
		t.Fatalf("want 1 leader, got %v", ls)
	}
	lead := r.nodes[ls[0]].Status()
	for id, n := range r.nodes {
		s := n.Status()
		if s.Term != lead.Term || s.Leader != ls[0] {
			t.Errorf("node %d: term=%d leader=%d, want term=%d leader=%d", id, s.Term, s.Leader, lead.Term, ls[0])
		}
	}
}

func TestSingleNodeClusterElectsItself(t *testing.T) {
	r := newElectionRig(t, 1, 1)
	r.run(time.Second)
	if len(r.leaders()) != 1 {
		t.Fatal("single node did not become leader")
	}
}

func TestHeartbeatsSuppressElections(t *testing.T) {
	r := newElectionRig(t, 5, 2)
	r.run(time.Second)
	term := r.nodes[r.leaders()[0]].Status().Term
	r.run(10 * time.Second)
	if got := r.nodes[r.leaders()[0]].Status().Term; got != term {
		t.Fatalf("term moved from %d to %d with a healthy leader", term, got)
	}
}

func TestIsolatedLeaderIsReplacedAndStepsDown(t *testing.T) {
	r := newElectionRig(t, 3, 3)
	r.run(time.Second)
	old := r.leaders()[0]
	r.net.Isolate(old)
	r.run(time.Second)

	var fresh raft.NodeID
	for id, n := range r.nodes {
		if id != old && n.Status().Role == raft.Leader {
			fresh = id
		}
	}
	if fresh == raft.NoNode {
		t.Fatal("majority side elected no leader")
	}
	if r.nodes[fresh].Status().Term <= r.nodes[old].Status().Term {
		t.Fatal("new leader's term must exceed the isolated one's")
	}

	r.net.Rejoin(old)
	r.run(500 * time.Millisecond)
	if s := r.nodes[old].Status(); s.Role != raft.Follower || s.Leader != fresh {
		t.Fatalf("old leader after rejoin: role=%v leader=%d, want follower of %d", s.Role, s.Leader, fresh)
	}
	if len(r.leaders()) != 1 {
		t.Fatalf("leaders after heal: %v", r.leaders())
	}
}

func TestMinorityCannotElect(t *testing.T) {
	r := newElectionRig(t, 5, 4)
	r.net.Isolate(3)
	r.net.Isolate(4)
	r.net.Isolate(5)
	r.run(3 * time.Second)
	if ls := r.leaders(); len(ls) != 0 {
		t.Fatalf("leader elected without a quorum: %v", ls)
	}
}

func TestVoteGrantedOncePerTerm(t *testing.T) {
	r := newElectionRig(t, 3, 5)
	// Node 1 is never started; we play candidates 2 and 3 by hand.
	var replies []raft.RequestVoteReply
	r.net.Register(2, func(m raft.Message) { replies = append(replies, m.Body.(raft.RequestVoteReply)) })
	r.net.Register(3, func(m raft.Message) { replies = append(replies, m.Body.(raft.RequestVoteReply)) })
	for _, c := range []raft.NodeID{2, 3, 2} {
		r.nodes[1].Handle(raft.Message{From: c, To: 1, Body: raft.RequestVoteArgs{Term: 7, CandidateID: c}})
	}
	r.net.DeliverAll()
	want := []bool{true, false, true} // a repeat request from the same candidate is fine
	for i, w := range want {
		if replies[i].VoteGranted != w || replies[i].Term != 7 {
			t.Errorf("reply %d = %+v, want granted=%v term=7", i, replies[i], w)
		}
	}
}

func TestStaleTermRequestRejected(t *testing.T) {
	r := newElectionRig(t, 3, 6)
	var got raft.RequestVoteReply
	r.net.Register(2, func(m raft.Message) { got = m.Body.(raft.RequestVoteReply) })
	r.nodes[1].Handle(raft.Message{From: 2, To: 1, Body: raft.RequestVoteArgs{Term: 5, CandidateID: 2}})
	r.nodes[1].Handle(raft.Message{From: 2, To: 1, Body: raft.RequestVoteArgs{Term: 3, CandidateID: 2}})
	r.net.DeliverAll()
	if got.VoteGranted || got.Term != 5 {
		t.Fatalf("stale request answered %+v", got)
	}
}

func TestElectionIsDeterministicPerSeed(t *testing.T) {
	leaderOf := func(seed uint64) raft.NodeID {
		r := newElectionRig(t, 5, seed)
		r.run(time.Second)
		return r.leaders()[0]
	}
	for seed := uint64(1); seed <= 5; seed++ {
		if a, b := leaderOf(seed), leaderOf(seed); a != b {
			t.Fatalf("seed %d elected %d then %d", seed, a, b)
		}
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	good := raft.Config{
		ID: 1, Peers: []raft.NodeID{1, 2, 3},
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Clock:             raftest.NewFakeClock(), Transport: transport.NewMemory(), Rand: rand.New(rand.NewPCG(1, 1)),
	}
	if _, err := raft.New(good); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*raft.Config){
		"zero id":          func(c *raft.Config) { c.ID = 0 },
		"not in peers":     func(c *raft.Config) { c.ID = 9 },
		"heartbeat >= min": func(c *raft.Config) { c.HeartbeatInterval = 150 * time.Millisecond },
		"nil clock":        func(c *raft.Config) { c.Clock = nil },
	} {
		c := good
		mut(&c)
		if _, err := raft.New(c); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

// staleClock records every callback and lets a test run one by hand even
// after its timer was stopped, as a real clock can when the callback has
// already been dispatched by the time Stop is called.
type staleClock struct{ fns []func() }

type noStop struct{}

func (noStop) Stop() bool { return true }

func (c *staleClock) AfterFunc(_ time.Duration, f func()) raft.Timer {
	c.fns = append(c.fns, f)
	return noStop{}
}

func TestStaleElectionTimerIsIgnored(t *testing.T) {
	clk := &staleClock{}
	sent := &raftest.Sent{}
	n, err := raft.New(raft.Config{
		ID: 2, Peers: []raft.NodeID{1, 2, 3},
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Clock:             clk, Transport: sent, Rand: rand.New(rand.NewPCG(1, 1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	stale := clk.fns[0]
	// A heartbeat arrives and re-arms the timer; then the old callback runs.
	n.Handle(raft.Message{From: 1, To: 2, Body: raft.AppendEntriesArgs{Term: 1, LeaderID: 1}})
	stale()
	if s := n.Status(); s.Role != raft.Follower || s.Term != 1 {
		t.Fatalf("stale timer changed state: role=%v term=%d", s.Role, s.Term)
	}
	clk.fns[len(clk.fns)-1]() // the current timer does fire
	if s := n.Status(); s.Role != raft.Candidate || s.Term != 2 {
		t.Fatalf("live timer ignored: role=%v term=%d", s.Role, s.Term)
	}
}
