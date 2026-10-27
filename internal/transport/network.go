package transport

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
)

// Event describes what the network decided to do with one message. It is
// emitted at send time; the visualizer animates it.
type Event struct {
	Msg     raft.Message
	Delay   time.Duration
	Dropped bool
	Reason  string // "random", "partition", "down"; set when Dropped
	Dup     bool   // this event is the injected duplicate of an earlier send
}

// Faults is the network's current fault configuration.
type Faults struct {
	DropRate    float64 // probability a message is silently lost
	DupRate     float64 // probability a message is delivered twice
	MinDelay    time.Duration
	MaxDelay    time.Duration
	PartitionOf map[raft.NodeID]int // partition group per node; absent means group 0
	DownNodes   map[raft.NodeID]bool
}

// Network is an in-process transport with fault injection. Delivery is
// scheduled on the supplied Clock, so with a fake clock a run is a pure
// function of the seed. Send never calls a handler synchronously.
type Network struct {
	mu       sync.Mutex
	clock    raft.Clock
	rng      *rand.Rand
	handlers map[raft.NodeID]func(raft.Message)
	faults   Faults
	observe  func(Event)
}

// NewNetwork returns a fault-free network. rng must not be shared.
func NewNetwork(clock raft.Clock, rng *rand.Rand) *Network {
	return &Network{
		clock: clock, rng: rng,
		handlers: make(map[raft.NodeID]func(raft.Message)),
		faults:   Faults{PartitionOf: map[raft.NodeID]int{}, DownNodes: map[raft.NodeID]bool{}},
	}
}

// Observe installs a callback invoked (without locks held) for every send.
func (n *Network) Observe(f func(Event)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.observe = f
}

// Register installs the receive handler for id and marks it up.
func (n *Network) Register(id raft.NodeID, h func(raft.Message)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	delete(n.faults.DownNodes, id)
}

// Crash unregisters id: everything addressed to it is dropped, including
// messages already in flight. Messages it already sent are still delivered.
func (n *Network) Crash(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
	n.faults.DownNodes[id] = true
}

// SetLoss sets the random drop and duplicate probabilities.
func (n *Network) SetLoss(drop, dup float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.faults.DropRate, n.faults.DupRate = drop, dup
}

// SetDelay makes every message take a uniform random time in [lo, hi].
func (n *Network) SetDelay(lo, hi time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.faults.MinDelay, n.faults.MaxDelay = lo, hi
}

// Partition splits the cluster: nodes can talk only within their own group.
// Nodes not listed form one extra group together.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.faults.PartitionOf = map[raft.NodeID]int{}
	for i, g := range groups {
		for _, id := range g {
			n.faults.PartitionOf[id] = i + 1
		}
	}
}

// Heal removes all partitions.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.faults.PartitionOf = map[raft.NodeID]int{}
}

// Isolate cuts id off from everyone else.
func (n *Network) Isolate(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.faults.PartitionOf[id] = -int(id) // unique negative group
}

// Faults returns a copy of the current fault configuration.
func (n *Network) Faults() Faults {
	n.mu.Lock()
	defer n.mu.Unlock()
	f := n.faults
	f.PartitionOf = make(map[raft.NodeID]int, len(n.faults.PartitionOf))
	for k, v := range n.faults.PartitionOf {
		f.PartitionOf[k] = v
	}
	f.DownNodes = make(map[raft.NodeID]bool, len(n.faults.DownNodes))
	for k, v := range n.faults.DownNodes {
		f.DownNodes[k] = v
	}
	return f
}

func (n *Network) partitioned(a, b raft.NodeID) bool {
	return n.faults.PartitionOf[a] != n.faults.PartitionOf[b]
}

func (n *Network) delay() time.Duration {
	f := n.faults
	if f.MaxDelay <= f.MinDelay {
		return f.MinDelay
	}
	return f.MinDelay + n.rng.N(f.MaxDelay-f.MinDelay+1)
}

// Send implements raft.Transport.
func (n *Network) Send(m raft.Message) {
	n.mu.Lock()
	var events []Event
	copies := 1
	if n.rng.Float64() < n.faults.DupRate {
		copies = 2
	}
	for i := 0; i < copies; i++ {
		ev := Event{Msg: m, Dup: i > 0}
		switch {
		case n.faults.DownNodes[m.To]:
			ev.Dropped, ev.Reason = true, "down"
		case n.partitioned(m.From, m.To):
			ev.Dropped, ev.Reason = true, "partition"
		case n.rng.Float64() < n.faults.DropRate:
			ev.Dropped, ev.Reason = true, "random"
		default:
			ev.Delay = n.delay()
		}
		events = append(events, ev)
	}
	obs := n.observe
	n.mu.Unlock()

	for _, ev := range events {
		if obs != nil {
			obs(ev)
		}
		if !ev.Dropped {
			n.clock.AfterFunc(ev.Delay, func() { n.deliver(m) })
		}
	}
}

// deliver re-checks faults at arrival time, so a partition or crash that
// happens while a message is in flight swallows it.
func (n *Network) deliver(m raft.Message) {
	n.mu.Lock()
	h := n.handlers[m.To]
	drop := h == nil || n.partitioned(m.From, m.To)
	n.mu.Unlock()
	if !drop {
		h(m)
	}
}
