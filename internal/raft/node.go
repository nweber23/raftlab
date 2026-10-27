package raft

import (
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// Config holds everything a Node needs. Nothing in it is a global.
type Config struct {
	ID    NodeID
	Peers []NodeID // every cluster member, including ID

	ElectionTimeoutMin time.Duration // paper §5.2 suggests 150ms
	ElectionTimeoutMax time.Duration // paper §5.2 suggests 300ms
	HeartbeatInterval  time.Duration // must be well below ElectionTimeoutMin (§5.6)

	Clock     Clock
	Transport Transport
	Rand      *rand.Rand // seeded by the caller; the core never touches global rand

	Initial PersistentState // zero value on first boot
}

// Node is one Raft server. All state is guarded by mu. Handlers (the lower
// case methods) assume mu is held; only the exported methods and timer
// callbacks take it.
type Node struct {
	cfg Config
	mu  sync.Mutex

	// Persistent state (Figure 2). Not yet written anywhere; see the
	// persistence part.
	currentTerm uint64
	votedFor    NodeID
	log         []Entry // log[0] is entry index 1

	// Volatile state.
	role   Role
	leader NodeID
	votes  map[NodeID]bool // candidate only: who granted us a vote this term

	electionTimer  Timer
	heartbeatTimer Timer
	stopped        bool
}

// New validates cfg and returns a stopped-until-Start follower.
func New(cfg Config) (*Node, error) {
	switch {
	case cfg.ID == NoNode:
		return nil, errors.New("raft: ID must be non-zero")
	case cfg.Clock == nil || cfg.Transport == nil || cfg.Rand == nil:
		return nil, errors.New("raft: Clock, Transport and Rand are required")
	case cfg.ElectionTimeoutMin <= 0 || cfg.ElectionTimeoutMax < cfg.ElectionTimeoutMin:
		return nil, errors.New("raft: invalid election timeout range")
	case cfg.HeartbeatInterval <= 0 || cfg.HeartbeatInterval >= cfg.ElectionTimeoutMin:
		return nil, errors.New("raft: HeartbeatInterval must be in (0, ElectionTimeoutMin)")
	}
	self := false
	for _, p := range cfg.Peers {
		self = self || p == cfg.ID
	}
	if !self {
		return nil, errors.New("raft: Peers must include ID")
	}
	return &Node{
		cfg:         cfg,
		currentTerm: cfg.Initial.Term,
		votedFor:    cfg.Initial.VotedFor,
		log:         append([]Entry(nil), cfg.Initial.Log...),
	}, nil
}

// Start arms the election timer. Until Start is called the node is inert.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.resetElectionTimer()
}

// Stop disarms both timers and makes the node ignore all further messages.
// It models a crash; Part 4 builds restart on top of it.
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopped = true
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
}

// Status returns a snapshot of the node.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.cfg.ID, Role: n.role, Term: n.currentTerm, VotedFor: n.votedFor,
		Leader: n.leader, LogLen: uint64(len(n.log)),
	}
}

// Handle delivers one incoming message to the node.
func (n *Node) Handle(m Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	switch b := m.Body.(type) {
	case RequestVoteArgs:
		n.handleRequestVote(m.From, b)
	case RequestVoteReply:
		n.handleRequestVoteReply(m.From, b)
	case AppendEntriesArgs:
		n.handleAppendEntries(m.From, b)
	case AppendEntriesReply:
		n.handleAppendEntriesReply(m.From, b)
	}
}

// --- helpers (provided) ---------------------------------------------------

func (n *Node) send(to NodeID, body any) {
	n.cfg.Transport.Send(Message{From: n.cfg.ID, To: to, Body: body})
}

// lastLog returns the index and term of the last log entry, or (0, 0) for an
// empty log.
func (n *Node) lastLog() (index, term uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	return uint64(len(n.log)), n.log[len(n.log)-1].Term
}

// randomElectionTimeout picks uniformly from [Min, Max].
func (n *Node) randomElectionTimeout() time.Duration {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	return n.cfg.ElectionTimeoutMin + n.cfg.Rand.N(span+1)
}

func (n *Node) resetElectionTimer() {
	d := n.randomElectionTimeout()
	if n.electionTimer == nil {
		n.electionTimer = n.cfg.Clock.AfterFunc(d, n.onElectionTimeout)
		return
	}
	n.electionTimer.Reset(d)
}

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

// --- election ---------------------------------------------------------------

// onElectionTimeout is the election timer callback. It runs on the clock's
// goroutine, so it must take mu itself. A timer that fired just before a Reset
// can still call in, hence the role check.
func (n *Node) onElectionTimeout() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role == Leader {
		return
	}
	n.startElection()
}

// onHeartbeatTimer is the heartbeat timer callback; it also takes mu itself.
func (n *Node) onHeartbeatTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role != Leader {
		return
	}
	n.broadcastHeartbeat()
}

// becomeFollower adopts term (if newer) and steps down. Figure 2, "All Servers".
func (n *Node) becomeFollower(term uint64) {
	wasLeader := n.role == Leader
	if term > n.currentTerm {
		n.currentTerm, n.votedFor, n.leader = term, NoNode, NoNode
		n.persistState()
	}
	n.role, n.votes = Follower, nil
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	if wasLeader {
		n.resetElectionTimer() // becomeLeader stopped it
	}
}

// startElection implements the Candidates rules of Figure 2.
func (n *Node) startElection() {
	n.currentTerm++
	n.role, n.leader = Candidate, NoNode
	n.votedFor = n.cfg.ID
	n.persistState()
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.resetElectionTimer()
	if len(n.votes) >= n.majority() { // single-node cluster
		n.becomeLeader()
		return
	}
	li, lt := n.lastLog()
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.send(p, RequestVoteArgs{Term: n.currentTerm, CandidateID: n.cfg.ID, LastLogIndex: li, LastLogTerm: lt})
		}
	}
}

// becomeLeader switches to leader and sends the first heartbeats.
func (n *Node) becomeLeader() {
	n.role, n.leader, n.votes = Leader, n.cfg.ID, nil
	n.electionTimer.Stop()
	n.broadcastHeartbeat()
}

// broadcastHeartbeat sends an empty AppendEntries to every peer and re-arms
// the heartbeat timer.
func (n *Node) broadcastHeartbeat() {
	li, lt := n.lastLog()
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.send(p, AppendEntriesArgs{Term: n.currentTerm, LeaderID: n.cfg.ID, PrevLogIndex: li, PrevLogTerm: lt})
		}
	}
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = n.cfg.Clock.AfterFunc(n.cfg.HeartbeatInterval, n.onHeartbeatTimer)
		return
	}
	n.heartbeatTimer.Reset(n.cfg.HeartbeatInterval)
}

// persistState writes currentTerm and votedFor to stable storage before the
// node answers any RPC. Wired up in the persistence step.
func (n *Node) persistState() {}

func (n *Node) handleRequestVote(from NodeID, a RequestVoteArgs) {
	if a.Term > n.currentTerm {
		n.becomeFollower(a.Term)
	}
	reply := RequestVoteReply{Term: n.currentTerm}
	if a.Term == n.currentTerm && (n.votedFor == NoNode || n.votedFor == a.CandidateID) {
		// §5.4.1 election restriction: the candidate's log must be at least
		// as up-to-date as ours.
		li, lt := n.lastLog()
		if a.LastLogTerm > lt || (a.LastLogTerm == lt && a.LastLogIndex >= li) {
			n.votedFor = a.CandidateID
			n.persistState()
			n.resetElectionTimer()
			reply.VoteGranted = true
		}
	}
	n.send(from, reply)
}

func (n *Node) handleRequestVoteReply(from NodeID, r RequestVoteReply) {
	if r.Term > n.currentTerm {
		n.becomeFollower(r.Term)
		return
	}
	if n.role != Candidate || r.Term != n.currentTerm || !r.VoteGranted {
		return
	}
	n.votes[from] = true
	if len(n.votes) >= n.majority() {
		n.becomeLeader()
	}
}

func (n *Node) handleAppendEntries(from NodeID, a AppendEntriesArgs) {
	if a.Term < n.currentTerm {
		n.send(from, AppendEntriesReply{Term: n.currentTerm})
		return
	}
	// A current-term AppendEntries also demotes a candidate (§5.2).
	n.becomeFollower(a.Term)
	n.leader = a.LeaderID
	n.resetElectionTimer()
	n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: true})
}

func (n *Node) handleAppendEntriesReply(from NodeID, r AppendEntriesReply) {
	if r.Term > n.currentTerm {
		n.becomeFollower(r.Term)
	}
}
