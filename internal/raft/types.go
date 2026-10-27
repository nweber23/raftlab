// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", 2014). Figure 2 of
// the paper is the specification; section numbers in comments refer to it.
//
// The package knows nothing about sockets or wall-clock time: all I/O goes
// through Transport and all time through Clock.
package raft

// NodeID identifies a cluster member. Zero means "nobody" (the paper's null).
type NodeID uint32

// NoNode is the null NodeID, used for "has not voted" and "no known leader".
const NoNode NodeID = 0

// Role is the state of a server (paper Figure 4).
type Role uint8

// The three roles of a Raft server (§5.1).
const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Entry is one log entry. Its index is its position in the log, starting at 1.
type Entry struct {
	Term    uint64
	Command []byte
}

// RequestVoteArgs is the RequestVote RPC request (Figure 2).
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RequestVote RPC response (Figure 2).
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC request (Figure 2).
// With no Entries it is a heartbeat (§5.2).
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesReply is the AppendEntries RPC response (Figure 2).
type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// Message is one datagram between two nodes. Body is one of the four
// Args/Reply types above, passed by value.
type Message struct {
	From, To NodeID
	Body     any
}

// PersistentState is what Figure 2 says must survive a restart.
type PersistentState struct {
	Term     uint64
	VotedFor NodeID
	Log      []Entry
}

// Status is a point-in-time snapshot of a node, for tests and the visualizer.
type Status struct {
	ID       NodeID
	Role     Role
	Term     uint64
	VotedFor NodeID
	Leader   NodeID // NoNode if unknown
	LogLen   uint64
}

// Kind names the RPC carried by m, for logs and the visualizer.
func (m Message) Kind() string {
	switch m.Body.(type) {
	case RequestVoteArgs:
		return "RequestVote"
	case RequestVoteReply:
		return "RequestVoteReply"
	case AppendEntriesArgs:
		return "AppendEntries"
	case AppendEntriesReply:
		return "AppendEntriesReply"
	}
	return "Unknown"
}

// Term returns the term carried by m's body, or 0 if it has none.
func (m Message) Term() uint64 {
	switch b := m.Body.(type) {
	case RequestVoteArgs:
		return b.Term
	case RequestVoteReply:
		return b.Term
	case AppendEntriesArgs:
		return b.Term
	case AppendEntriesReply:
		return b.Term
	}
	return 0
}
