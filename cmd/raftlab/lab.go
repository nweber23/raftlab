package main

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/nweber23/raftlab/internal/clock"
	"github.com/nweber23/raftlab/internal/kv"
	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/storage"
	"github.com/nweber23/raftlab/internal/transport"
)

// Lab is an in-process cluster on the wall clock, with a fault-injecting
// network between its nodes. It is what the visualizer looks at and pokes.
type Lab struct {
	dir string
	ids []raft.NodeID
	net *transport.Network

	mu      sync.Mutex
	members map[raft.NodeID]*member // absent while a node is crashed
	boots   uint64                  // varies each node's timeout randomness across restarts
}

type member struct {
	node  *raft.Node
	store *kv.Store
	wal   *storage.WAL
	api   http.Handler
}

// NewLab boots size nodes whose WALs live under dir.
func NewLab(size int, dir string) (*Lab, error) {
	l := &Lab{
		dir: dir, members: map[raft.NodeID]*member{},
		net: transport.NewNetwork(clock.System{}, rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))),
	}
	for i := 1; i <= size; i++ {
		l.ids = append(l.ids, raft.NodeID(i))
	}
	for _, id := range l.ids {
		if err := l.Restart(id); err != nil {
			l.Close()
			return nil, err
		}
	}
	return l, nil
}

// Restart boots node id from its WAL. It is a no-op if the node is up.
func (l *Lab) Restart(id raft.NodeID) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.members[id] != nil {
		return nil
	}
	if !l.known(id) {
		return fmt.Errorf("no such node %d", id)
	}
	wal, initial, err := storage.Open(filepath.Join(l.dir, fmt.Sprint(id)), storage.Options{})
	if err != nil {
		return err
	}
	l.boots++
	store := kv.New()
	node, err := raft.New(raft.Config{
		ID: id, Peers: l.ids, Initial: initial, Storage: wal, Machine: store,
		Clock: clock.System{}, Transport: l.net, SnapshotThreshold: 40,
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Rand:              rand.New(rand.NewPCG(uint64(time.Now().UnixNano())+l.boots, uint64(id))),
	})
	if err != nil {
		_ = wal.Close()
		return err
	}
	store.Attach(node)
	api := (&kv.Server{
		Store: store, Node: node, Timeout: 3 * time.Second,
		LeaderURL: func(n raft.NodeID) string { return fmt.Sprintf("/node/%d", n) },
	}).Handler()
	l.members[id] = &member{node, store, wal, api}
	l.net.Register(id, node.Handle)
	node.Start()
	return nil
}

func (l *Lab) known(id raft.NodeID) bool {
	for _, x := range l.ids {
		if x == id {
			return true
		}
	}
	return false
}

// Crash kills node id; its WAL stays on disk. No-op if already down.
func (l *Lab) Crash(id raft.NodeID) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.known(id) {
		return fmt.Errorf("no such node %d", id)
	}
	m := l.members[id]
	if m == nil {
		return nil
	}
	m.node.Stop()
	l.net.Crash(id)
	delete(l.members, id)
	return m.wal.Close()
}

// Close stops every node.
func (l *Lab) Close() {
	for _, id := range l.ids {
		_ = l.Crash(id)
	}
}

// NodeHandler serves node id's key-value API, or 503 while it is crashed.
func (l *Lab) NodeHandler(id raft.NodeID) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		m := l.members[id]
		l.mu.Unlock()
		if m == nil {
			http.Error(w, "node is down", http.StatusServiceUnavailable)
			return
		}
		m.api.ServeHTTP(w, r)
	})
}

// Partition splits the cluster into the given groups; unlisted nodes form one more group.
func (l *Lab) Partition(groups [][]raft.NodeID) { l.net.Partition(groups...) }

// Heal removes all partitions.
func (l *Lab) Heal() { l.net.Heal() }

// SetNetwork changes latency (a uniform range) and loss.
func (l *Lab) SetNetwork(minDelay, maxDelay time.Duration, drop, dup float64) {
	l.net.SetDelay(minDelay, maxDelay)
	l.net.SetLoss(drop, dup)
}

// Wire types for the visualizer.

// LogEntry is one log slot as shown in the UI.
type LogEntry struct {
	Index uint64 `json:"i"`
	Term  uint64 `json:"term"`
	Cmd   string `json:"cmd"` // empty for the leader's no-op
}

// NodeState is one node in a State frame.
type NodeState struct {
	ID        raft.NodeID       `json:"id"`
	Up        bool              `json:"up"`
	Group     int               `json:"group"` // partition group; 0 is the default
	Role      string            `json:"role"`
	Term      uint64            `json:"term"`
	VotedFor  raft.NodeID       `json:"votedFor"`
	Leader    raft.NodeID       `json:"leader"`
	Commit    uint64            `json:"commit"`
	Applied   uint64            `json:"applied"`
	SnapIndex uint64            `json:"snapIndex"`
	LastIndex uint64            `json:"lastIndex"`
	Log       []LogEntry        `json:"log"`
	KV        map[string]string `json:"kv"`
}

// State is a full snapshot of the lab.
type State struct {
	Type    string      `json:"type"` // "state"
	Nodes   []NodeState `json:"nodes"`
	DelayLo int64       `json:"delayMinMs"`
	DelayHi int64       `json:"delayMaxMs"`
	Drop    float64     `json:"dropRate"`
	Dup     float64     `json:"dupRate"`
}

// State captures the current state of every node and the network.
func (l *Lab) State() State {
	f := l.net.Faults()
	st := State{
		Type: "state", DelayLo: f.MinDelay.Milliseconds(), DelayHi: f.MaxDelay.Milliseconds(),
		Drop: f.DropRate, Dup: f.DupRate,
	}
	l.mu.Lock()
	members := make(map[raft.NodeID]*member, len(l.members))
	for id, m := range l.members {
		members[id] = m
	}
	l.mu.Unlock()
	for _, id := range l.ids {
		ns := NodeState{ID: id, Group: f.PartitionOf[id], Log: []LogEntry{}, KV: map[string]string{}}
		if m := members[id]; m != nil {
			s := m.node.Status()
			ns.Up, ns.Role, ns.Term, ns.VotedFor, ns.Leader = true, s.Role.String(), s.Term, s.VotedFor, s.Leader
			ns.Commit, ns.Applied, ns.SnapIndex, ns.LastIndex = s.CommitIndex, s.LastApplied, s.SnapIndex, s.LastIndex
			for i, e := range s.Log {
				ns.Log = append(ns.Log, LogEntry{Index: s.SnapIndex + uint64(i) + 1, Term: e.Term, Cmd: string(e.Command)})
			}
			ns.KV = m.store.Data()
		}
		st.Nodes = append(st.Nodes, ns)
	}
	return st
}

// MsgEvent is one network send, for the animation.
type MsgEvent struct {
	Type    string      `json:"type"` // "msg"
	From    raft.NodeID `json:"from"`
	To      raft.NodeID `json:"to"`
	Kind    string      `json:"kind"`
	Term    uint64      `json:"term"`
	Detail  string      `json:"detail"`
	DelayMs int64       `json:"delayMs"`
	Dropped bool        `json:"dropped"`
	Reason  string      `json:"reason,omitempty"`
	Dup     bool        `json:"dup,omitempty"`
}

func describe(e transport.Event) MsgEvent {
	m := e.Msg
	ev := MsgEvent{
		Type: "msg", From: m.From, To: m.To, Kind: m.Kind(), Term: m.Term(),
		DelayMs: e.Delay.Milliseconds(), Dropped: e.Dropped, Reason: e.Reason, Dup: e.Dup,
	}
	switch b := m.Body.(type) {
	case raft.RequestVoteArgs:
		ev.Detail = fmt.Sprintf("last log %d@%d", b.LastLogIndex, b.LastLogTerm)
	case raft.RequestVoteReply:
		ev.Detail = fmt.Sprintf("granted=%v", b.VoteGranted)
	case raft.AppendEntriesArgs:
		ev.Detail = fmt.Sprintf("prev=%d entries=%d commit=%d", b.PrevLogIndex, len(b.Entries), b.LeaderCommit)
	case raft.AppendEntriesReply:
		ev.Detail = fmt.Sprintf("success=%v match=%d", b.Success, b.MatchIndex)
	case raft.InstallSnapshotArgs:
		ev.Detail = fmt.Sprintf("through index %d", b.LastIncludedIndex)
	case raft.InstallSnapshotReply:
		ev.Detail = fmt.Sprintf("match=%d", b.MatchIndex)
	}
	return ev
}
