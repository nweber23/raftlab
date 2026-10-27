// Package kv is a linearizable key-value store replicated with Raft. Every
// operation, reads included, goes through the log. Clients tag writes with a
// (client, seq) pair so a retried request is applied at most once.
package kv

import (
	"encoding/json"
	"errors"
	"maps"
	"sync"

	"github.com/nweber23/raftlab/internal/raft"
)

// Operation kinds.
const (
	OpSet    = "set"
	OpGet    = "get"
	OpDelete = "delete"
)

// ErrNotLeader means the node cannot take proposals; see Server for redirects.
var ErrNotLeader = errors.New("kv: not the leader")

// Op is one client command, as stored in the Raft log (JSON, so the log is
// readable in the visualizer).
type Op struct {
	Kind   string `json:"op"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Client string `json:"client"`
	Seq    uint64 `json:"seq"` // per-client, strictly increasing
}

// Result is what an applied Op returns to its caller.
type Result struct {
	Value string
	Found bool
	Err   string
}

type session struct {
	Seq    uint64
	Result Result
}

type waitKey struct {
	client string
	seq    uint64
}

// Pending is an in-flight Submit. Done closes when the op has been applied.
type Pending struct {
	done chan struct{}
	res  Result
}

// Done is closed once Result is valid.
func (p *Pending) Done() <-chan struct{} { return p.done }

// Result returns the outcome; valid after Done is closed.
func (p *Pending) Result() Result { return p.res }

// Store is the state machine plus the glue to submit operations to Raft.
type Store struct {
	mu       sync.Mutex
	data     map[string]string
	sessions map[string]session // last applied request per client
	waiters  map[waitKey]*Pending
	node     *raft.Node
}

var _ raft.StateMachine = (*Store)(nil)

// New returns an empty store. Call Attach before Submit.
func New() *Store {
	return &Store{
		data: map[string]string{}, sessions: map[string]session{}, waiters: map[waitKey]*Pending{},
	}
}

// Attach connects the store to its Raft node. The two refer to each other,
// so one has to be built first.
func (s *Store) Attach(n *raft.Node) { s.node = n }

// Data returns a copy of the current contents.
func (s *Store) Data() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.data)
}

// Submit proposes op, which must carry a Client and Seq. It may be called
// again with the same pair after a timeout; the op still executes once. It
// returns ErrNotLeader on a follower.
func (s *Store) Submit(op Op) (*Pending, error) {
	if op.Client == "" {
		return nil, errors.New("kv: op needs a client ID")
	}
	k := waitKey{op.Client, op.Seq}
	s.mu.Lock()
	if sess, ok := s.sessions[op.Client]; ok && op.Seq <= sess.Seq {
		s.mu.Unlock()
		return finished(sessionResult(sess, op.Seq)), nil
	}
	p := s.waiters[k]
	if p == nil {
		p = &Pending{done: make(chan struct{})}
		s.waiters[k] = p
	}
	s.mu.Unlock()

	// Always propose, even if a waiter exists: the earlier proposal may have
	// died with a deposed leader. A second copy in the log is skipped by the
	// session check in Apply.
	cmd, _ := json.Marshal(op)
	if _, _, ok := s.node.Propose(cmd); !ok {
		s.mu.Lock()
		delete(s.waiters, k)
		s.mu.Unlock()
		return nil, ErrNotLeader
	}
	return p, nil
}

func finished(r Result) *Pending {
	p := &Pending{done: make(chan struct{}), res: r}
	close(p.done)
	return p
}

func sessionResult(sess session, seq uint64) Result {
	if seq == sess.Seq {
		return sess.Result
	}
	return Result{Err: "stale request: a newer one from this client was already applied"}
}

// Apply implements raft.StateMachine.
func (s *Store) Apply(_ uint64, cmd []byte) {
	var op Op
	if err := json.Unmarshal(cmd, &op); err != nil {
		return // not ours; every replica skips it identically
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var res Result
	if sess, dup := s.sessions[op.Client]; dup && op.Seq <= sess.Seq {
		res = sessionResult(sess, op.Seq) // duplicate: answer, do not re-execute
	} else {
		switch op.Kind {
		case OpSet:
			s.data[op.Key] = op.Value
		case OpDelete:
			_, res.Found = s.data[op.Key]
			delete(s.data, op.Key)
		case OpGet:
			res.Value, res.Found = s.data[op.Key]
		default:
			res.Err = "unknown op " + op.Kind
		}
		s.sessions[op.Client] = session{Seq: op.Seq, Result: res}
	}
	if p, ok := s.waiters[waitKey{op.Client, op.Seq}]; ok {
		delete(s.waiters, waitKey{op.Client, op.Seq})
		p.res = res
		close(p.done)
	}
}

type snapshotImage struct {
	Data     map[string]string
	Sessions map[string]session
}

// Snapshot implements raft.StateMachine.
// ponytail: sessions grow with the number of distinct clients and are never
// expired; add client leases if that ever matters.
func (s *Store) Snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(snapshotImage{s.data, s.sessions})
	return b
}

// Restore implements raft.StateMachine.
func (s *Store) Restore(data []byte) {
	var img snapshotImage
	if err := json.Unmarshal(data, &img); err != nil {
		panic("kv: corrupt snapshot: " + err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.sessions = img.Data, img.Sessions
	if s.data == nil {
		s.data = map[string]string{}
	}
	if s.sessions == nil {
		s.sessions = map[string]session{}
	}
	// Ops covered by the snapshot never reach Apply; release their waiters.
	for k, p := range s.waiters {
		if sess, ok := s.sessions[k.client]; ok && k.seq <= sess.Seq {
			delete(s.waiters, k)
			p.res = sessionResult(sess, k.seq)
			close(p.done)
		}
	}
}
