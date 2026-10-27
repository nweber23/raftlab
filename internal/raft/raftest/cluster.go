package raftest

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/storage"
	"github.com/nweber23/raftlab/internal/transport"
)

// Step is the granularity of Cluster.Run: virtual time advances this much
// between invariant checks.
const Step = 10 * time.Millisecond

// Applied is one command seen by a Recorder.
type Applied struct {
	Index uint64
	Cmd   string
}

// Recorder is a StateMachine that remembers everything applied to it.
type Recorder struct {
	mu   sync.Mutex
	list []Applied
}

// Apply implements raft.StateMachine.
func (r *Recorder) Apply(index uint64, cmd []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = append(r.list, Applied{index, string(cmd)})
}

// Snapshot implements raft.StateMachine.
func (r *Recorder) Snapshot() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, _ := json.Marshal(r.list)
	return b
}

// Restore implements raft.StateMachine.
func (r *Recorder) Restore(data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = nil
	if err := json.Unmarshal(data, &r.list); err != nil {
		panic(err)
	}
}

// Applied returns a copy of the applied commands in order.
func (r *Recorder) Applied() []Applied {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.list)
}

// Options configures NewCluster.
type Options struct {
	Size int
	Seed uint64
	// SnapshotThreshold is passed to every node (0 disables compaction).
	SnapshotThreshold int
	// Machine builds the state machine for a node; nil gives each node a Recorder.
	Machine func(id raft.NodeID) raft.StateMachine
	// OnBoot, if set, runs for every node (including restarts) just before it starts.
	OnBoot func(id raft.NodeID, n *raft.Node, sm raft.StateMachine)
}

// Cluster is a deterministic in-process Raft cluster: fake clock, faulty
// network, one goroutine. Everything is a pure function of Options.Seed.
type Cluster struct {
	T     testing.TB
	Opts  Options
	Clock *FakeClock
	Net   *transport.Network
	IDs   []raft.NodeID
	Nodes map[raft.NodeID]*raft.Node
	SMs   map[raft.NodeID]raft.StateMachine
	WALs  map[raft.NodeID]*storage.WAL

	dir      string
	restarts map[raft.NodeID]uint64

	leaderOfTerm map[uint64]raft.NodeID // election safety bookkeeping
}

// NewCluster builds and starts a cluster. Failures report the seed.
func NewCluster(t testing.TB, o Options) *Cluster {
	t.Helper()
	c := &Cluster{
		T: t, Opts: o, Clock: NewFakeClock(),
		Nodes: map[raft.NodeID]*raft.Node{}, SMs: map[raft.NodeID]raft.StateMachine{},
		WALs: map[raft.NodeID]*storage.WAL{}, dir: t.TempDir(), restarts: map[raft.NodeID]uint64{},
		leaderOfTerm: map[uint64]raft.NodeID{},
	}
	c.Net = transport.NewNetwork(c.Clock, rand.New(rand.NewPCG(o.Seed, 0xfeed)))
	for i := 1; i <= o.Size; i++ {
		c.IDs = append(c.IDs, raft.NodeID(i))
	}
	for _, id := range c.IDs {
		c.boot(id)
	}
	return c
}

// boot creates and starts node id from scratch.
func (c *Cluster) boot(id raft.NodeID) {
	var sm raft.StateMachine = &Recorder{}
	if c.Opts.Machine != nil {
		sm = c.Opts.Machine(id)
	}
	// Crashes are modeled at process level, so fsync would only slow tests down.
	wal, initial, err := storage.Open(filepath.Join(c.dir, fmt.Sprint(id)), storage.Options{NoSync: true})
	if err != nil {
		c.T.Fatalf("seed=%d: %v", c.Opts.Seed, err)
	}
	c.WALs[id] = wal
	n, err := raft.New(raft.Config{
		ID: id, Peers: c.IDs, Initial: initial, Storage: wal,
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Clock:             c.Clock, Transport: c.Net, Machine: sm, SnapshotThreshold: c.Opts.SnapshotThreshold,
		Rand: rand.New(rand.NewPCG(c.Opts.Seed+c.restarts[id], uint64(id))),
	})
	if err != nil {
		c.T.Fatalf("seed=%d: %v", c.Opts.Seed, err)
	}
	c.Nodes[id], c.SMs[id] = n, sm
	c.Net.Register(id, n.Handle)
	if c.Opts.OnBoot != nil {
		c.Opts.OnBoot(id, n, sm)
	}
	n.Start()
}

// Crash kills node id: its timers stop, the network drops its traffic and its
// volatile state is gone. Only what it wrote to its WAL survives.
func (c *Cluster) Crash(id raft.NodeID) {
	c.Nodes[id].Stop()
	c.Net.Crash(id)
	c.WALs[id].Close()
}

// Restart boots a fresh node for id from its WAL.
func (c *Cluster) Restart(id raft.NodeID) {
	c.restarts[id]++
	c.boot(id)
}

// Fatalf fails the test and always names the seed.
func (c *Cluster) Fatalf(format string, args ...any) {
	c.T.Helper()
	c.T.Fatalf("seed=%d: %s", c.Opts.Seed, fmt.Sprintf(format, args...))
}

// Run advances virtual time by d, checking safety invariants every Step.
func (c *Cluster) Run(d time.Duration) {
	c.T.Helper()
	for ; d > 0; d -= Step {
		c.Clock.Advance(Step)
		c.CheckInvariants()
	}
}

// RunUntil runs until cond holds or max virtual time passes; it reports success.
func (c *Cluster) RunUntil(max time.Duration, cond func() bool) bool {
	c.T.Helper()
	for ; max > 0; max -= Step {
		if cond() {
			return true
		}
		c.Clock.Advance(Step)
		c.CheckInvariants()
	}
	return cond()
}

// Leader returns the leader with the highest term among nodes the network
// still considers up, or NoNode.
func (c *Cluster) Leader() raft.NodeID {
	var best raft.NodeID
	var bestTerm uint64
	for _, id := range c.IDs {
		if c.Net.Faults().DownNodes[id] {
			continue
		}
		if s := c.Nodes[id].Status(); s.Role == raft.Leader && s.Term >= bestTerm {
			best, bestTerm = id, s.Term
		}
	}
	return best
}

// WaitLeader runs until some leader exists and returns it.
func (c *Cluster) WaitLeader() raft.NodeID {
	c.T.Helper()
	if !c.RunUntil(5*time.Second, func() bool { return c.Leader() != raft.NoNode }) {
		c.Fatalf("no leader within 5s")
	}
	return c.Leader()
}

// Propose submits cmd to the current leader and returns its log index.
func (c *Cluster) Propose(cmd string) uint64 {
	c.T.Helper()
	l := c.WaitLeader()
	idx, _, ok := c.Nodes[l].Propose([]byte(cmd))
	if !ok {
		c.Fatalf("leader %d refused %q", l, cmd)
	}
	return idx
}

// Applied returns the commands applied on node id (Recorder machines only).
func (c *Cluster) Applied(id raft.NodeID) []string {
	var out []string
	for _, a := range c.SMs[id].(*Recorder).Applied() {
		out = append(out, a.Cmd)
	}
	return out
}

// CheckInvariants verifies the Raft safety properties that must hold at every
// instant: at most one leader per term, and committed log prefixes agree.
func (c *Cluster) CheckInvariants() {
	c.T.Helper()
	statuses := map[raft.NodeID]raft.Status{}
	for _, id := range c.IDs {
		s := c.Nodes[id].Status()
		statuses[id] = s
		if s.Role == raft.Leader {
			if prev, ok := c.leaderOfTerm[s.Term]; ok && prev != id {
				c.Fatalf("election safety: nodes %d and %d both lead term %d", prev, id, s.Term)
			}
			c.leaderOfTerm[s.Term] = id
		}
	}
	for _, a := range c.IDs {
		for _, b := range c.IDs {
			if a >= b {
				continue
			}
			sa, sb := statuses[a], statuses[b]
			hi := min(sa.CommitIndex, sb.CommitIndex)
			for i := max(sa.FirstIndex(), sb.FirstIndex()); i <= hi; i++ {
				if !equalEntry(sa.EntryAt(i), sb.EntryAt(i)) {
					c.Fatalf("state machine safety: nodes %d and %d disagree on committed index %d", a, b, i)
				}
			}
		}
	}
}

func equalEntry(a, b raft.Entry) bool {
	return a.Term == b.Term && string(a.Command) == string(b.Command)
}

// Sent is a Transport that just remembers what was sent; for driving a
// single node by hand.
type Sent struct {
	mu   sync.Mutex
	Msgs []raft.Message
}

// Send implements raft.Transport.
func (s *Sent) Send(m raft.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Msgs = append(s.Msgs, m)
}
