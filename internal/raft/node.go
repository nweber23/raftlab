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
	Machine   StateMachine // nil means commands are committed but go nowhere
	Rand      *rand.Rand   // seeded by the caller; the core never touches global rand

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
	role        Role
	leader      NodeID
	votes       map[NodeID]bool // candidate only: who granted us a vote this term
	commitIndex uint64
	lastApplied uint64

	// Leader only, reinitialized on election (Figure 2).
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

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
	if cfg.Machine == nil {
		cfg.Machine = noopMachine{}
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
		Leader: n.leader, CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastIndex: n.lastIndex(), Log: append([]Entry(nil), n.log...),
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

// lastIndex is the index of the last log entry, 0 for an empty log.
func (n *Node) lastIndex() uint64 { return uint64(len(n.log)) }

// termAt returns the term of the entry at index i; i == 0 is the empty-log
// sentinel with term 0. The caller guarantees i <= lastIndex.
func (n *Node) termAt(i uint64) uint64 {
	if i == 0 {
		return 0
	}
	return n.log[i-1].Term
}

// lastLog returns the index and term of the last log entry, or (0, 0) for an
// empty log.
func (n *Node) lastLog() (index, term uint64) {
	return n.lastIndex(), n.termAt(n.lastIndex())
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

// becomeLeader switches to leader, appends a no-op so entries from earlier
// terms can commit (§5.4.2, §8), and sends the first AppendEntries.
func (n *Node) becomeLeader() {
	n.role, n.leader, n.votes = Leader, n.cfg.ID, nil
	n.electionTimer.Stop()
	n.nextIndex = make(map[NodeID]uint64, len(n.cfg.Peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndex() + 1
	}
	n.appendLocal(nil)
	n.broadcastHeartbeat()
}

// appendLocal appends one entry in the current term to the leader's log.
func (n *Node) appendLocal(cmd []byte) uint64 {
	n.log = append(n.log, Entry{Term: n.currentTerm, Command: cmd})
	n.persistAppend(n.lastIndex()-1, n.log[n.lastIndex()-1:])
	n.matchIndex[n.cfg.ID] = n.lastIndex()
	n.advanceCommit() // a single-node cluster commits immediately
	return n.lastIndex()
}

// Propose appends command to the log if this node is the leader. It returns
// the entry's index and term; whether it commits is learned through the
// StateMachine. ok is false on a non-leader.
func (n *Node) Propose(command []byte) (index, term uint64, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role != Leader || len(command) == 0 {
		return 0, 0, false
	}
	index = n.appendLocal(command)
	n.broadcastHeartbeat()
	return index, n.currentTerm, true
}

const maxBatch = 64 // entries per AppendEntries

// broadcastHeartbeat replicates to every peer (an empty AppendEntries when
// the peer is caught up) and re-arms the heartbeat timer.
func (n *Node) broadcastHeartbeat() {
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppend(p)
		}
	}
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = n.cfg.Clock.AfterFunc(n.cfg.HeartbeatInterval, n.onHeartbeatTimer)
		return
	}
	n.heartbeatTimer.Reset(n.cfg.HeartbeatInterval)
}

// sendAppend sends peer p the entries from nextIndex[p] on.
// ponytail: nothing tracks what is already in flight, so a burst of proposals
// resends overlapping batches; harmless because followers skip entries they
// already hold. Add pipelining state if bandwidth matters.
func (n *Node) sendAppend(p NodeID) {
	next := n.nextIndex[p]
	prev := next - 1
	hi := min(n.lastIndex(), prev+maxBatch)
	n.send(p, AppendEntriesArgs{
		Term: n.currentTerm, LeaderID: n.cfg.ID,
		PrevLogIndex: prev, PrevLogTerm: n.termAt(prev),
		Entries:      append([]Entry(nil), n.log[prev:hi]...),
		LeaderCommit: n.commitIndex,
	})
}

// advanceCommit moves commitIndex to the highest index stored on a majority
// whose entry is from the current term (§5.4.2), then applies.
func (n *Node) advanceCommit() {
	for i := n.lastIndex(); i > n.commitIndex; i-- {
		if n.termAt(i) != n.currentTerm {
			break // older terms commit only indirectly
		}
		count := 0
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= i {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = i
			n.applyCommitted()
			return
		}
	}
}

// applyCommitted feeds newly committed entries to the state machine. Empty
// commands are leader no-ops and are skipped.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if e := n.log[n.lastApplied-1]; len(e.Command) > 0 {
			n.cfg.Machine.Apply(n.lastApplied, e.Command)
		}
	}
}

// persistAppend makes "log truncated to after, then entries appended" durable.
// Wired up in the persistence step.
func (n *Node) persistAppend(after uint64, entries []Entry) {}

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

	// Consistency check (Figure 2, step 2).
	if a.PrevLogIndex > n.lastIndex() {
		n.send(from, AppendEntriesReply{Term: n.currentTerm, ConflictIndex: n.lastIndex() + 1})
		return
	}
	if t := n.termAt(a.PrevLogIndex); t != a.PrevLogTerm {
		first := a.PrevLogIndex
		for first > 1 && n.termAt(first-1) == t {
			first--
		}
		n.send(from, AppendEntriesReply{Term: n.currentTerm, ConflictIndex: first, ConflictTerm: t})
		return
	}

	// Steps 3-4: truncate only at a real conflict, so a stale or duplicated
	// RPC can never erase entries that a newer one already delivered.
	for i, e := range a.Entries {
		idx := a.PrevLogIndex + 1 + uint64(i)
		if idx <= n.lastIndex() {
			if n.termAt(idx) == e.Term {
				continue
			}
			n.log = n.log[:idx-1]
		}
		n.log = append(n.log, a.Entries[i:]...)
		n.persistAppend(idx-1, a.Entries[i:])
		break
	}

	match := a.PrevLogIndex + uint64(len(a.Entries))
	if a.LeaderCommit > n.commitIndex {
		n.commitIndex = min(a.LeaderCommit, match)
		n.applyCommitted()
	}
	n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: true, MatchIndex: match})
}

func (n *Node) handleAppendEntriesReply(from NodeID, r AppendEntriesReply) {
	if r.Term > n.currentTerm {
		n.becomeFollower(r.Term)
		return
	}
	if n.role != Leader || r.Term != n.currentTerm {
		return
	}
	if r.Success {
		n.matchIndex[from] = max(n.matchIndex[from], r.MatchIndex)
		n.nextIndex[from] = max(n.nextIndex[from], n.matchIndex[from]+1)
		n.advanceCommit()
		if n.nextIndex[from] <= n.lastIndex() {
			n.sendAppend(from) // more to ship
		}
		return
	}
	// Conflict repair: jump back to where the follower's log can diverge.
	next := r.ConflictIndex
	if r.ConflictTerm != 0 {
		// If we have entries of the follower's conflicting term, retry just
		// after our last one; otherwise drop the whole term.
		for i := n.lastIndex(); i >= 1; i-- {
			if n.termAt(i) == r.ConflictTerm {
				next = i + 1
				break
			}
			if n.termAt(i) < r.ConflictTerm {
				break
			}
		}
	}
	next = max(next, n.matchIndex[from]+1, 1)
	if next < n.nextIndex[from] { // otherwise a stale reply: ignore, no resend
		n.nextIndex[from] = next
		n.sendAppend(from)
	}
}
