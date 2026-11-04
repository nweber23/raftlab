// Package kv is a linearizable key-value store replicated with Raft. Writes go
// through the log; clients tag them with a (client, seq) pair so a retried
// request is applied at most once. Reads use Raft's ReadIndex and skip the log
// (Submit with OpGet still works and is also linearizable).
//
// Sessions: a client may pick its own ID, or ask the cluster for one with
// OpRegister (the ID is derived from the log index, so it is unique and the
// same on every replica). With SessionTTL set, a session that has not been
// used for that long is forgotten, and with RequireSessions an unknown or
// expired ID is rejected instead of silently starting a new session, which
// would otherwise let a very late duplicate of an old request run again.
// "Time" is the timestamp the proposing node stamped into the log entry, so
// every replica expires the same sessions at the same log position.
package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
)

// Operation kinds.
const (
	OpSet    = "set"
	OpGet    = "get"
	OpDelete = "delete"
	// OpRegister creates a session and returns its ID in Result.Value.
	OpRegister = "register"
	// OpKeepAlive does nothing except refresh the caller's session.
	OpKeepAlive = "keepalive"
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
	// Time is stamped by Submit (unix nanoseconds, the proposer's clock). It is
	// the only clock the state machine ever reads.
	Time int64 `json:"t,omitempty"`
	// Anon marks an untracked request: it has no session and is never deduplicated.
	Anon bool `json:"anon,omitempty"`
}

// Result is what an applied Op returns to its caller.
type Result struct {
	Value   string
	Found   bool
	Err     string
	Expired bool // the client's session is unknown or has expired; register again
}

type session struct {
	Seq    uint64
	Result Result
	Last   int64 // Time of the newest op seen from this client
}

// SessionOptions is the session configuration of a Store, for command-line wiring.
type SessionOptions struct {
	TTL     time.Duration
	Require bool
}

// ErrSessionExpired is the Result.Err text for Result.Expired.
const ErrSessionExpired = "session unknown or expired: register a new one"

type waitKey struct {
	client string
	seq    uint64
}

// Pending is an in-flight Submit. Done closes when the op has been applied.
type Pending struct {
	done chan struct{}
	res  Result
	err  error // set instead of res when leadership was lost before a read completed
}

// Done is closed once Result is valid.
func (p *Pending) Done() <-chan struct{} { return p.done }

// Result returns the outcome; valid after Done is closed.
func (p *Pending) Result() Result { return p.res }

// Err is non-nil, once Done is closed, if a read lost its leader before it
// could complete; the caller should retry elsewhere.
func (p *Pending) Err() error { return p.err }

// Store is the state machine plus the glue to submit operations to Raft.
type Store struct {
	mu       sync.Mutex
	data     map[string]string
	sessions map[string]session // last applied request per client
	waiters  map[waitKey]*Pending
	node     *raft.Node

	// Set these before Attach.
	SessionTTL      time.Duration    // 0: sessions never expire
	RequireSessions bool             // reject ops from unknown or expired client IDs
	Now             func() time.Time // stamps ops; nil means time.Now

	tick, sweptAt int64 // newest op Time applied, and when sessions were last swept
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
	if op.Time == 0 {
		now := time.Now
		if s.Now != nil {
			now = s.Now
		}
		op.Time = now().UnixNano()
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

// Read looks key up without writing to the log, using Raft's ReadIndex: the
// leader confirms it is still the leader, waits for its state machine to catch
// up, then reads locally. It returns raft.ErrNotLeader on a follower and
// raft.ErrNotReady for a brand-new leader; both are retryable.
func (s *Store) Read(key string) (*Pending, error) {
	p := &Pending{done: make(chan struct{})}
	err := s.node.ReadIndex(func(err error) { // inside the node's lock, like Apply
		if err != nil {
			p.err = err
		} else {
			s.mu.Lock()
			p.res.Value, p.res.Found = s.data[key]
			s.mu.Unlock()
		}
		close(p.done)
	})
	if err != nil {
		return nil, err
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
func (s *Store) Apply(index uint64, cmd []byte) {
	var op Op
	if err := json.Unmarshal(cmd, &op); err != nil {
		return // not ours; every replica skips it identically
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(op.Time)

	var res Result
	switch {
	case op.Kind == OpRegister:
		res.Value = fmt.Sprintf("s%d", index) // the log index is unique and identical everywhere
		s.sessions[res.Value] = session{Last: op.Time}
	case op.Anon:
		res = s.execute(op)
	default:
		sess, ok := s.sessions[op.Client]
		if ok && s.expired(sess, op.Time) {
			delete(s.sessions, op.Client)
			ok = false
		}
		switch {
		case !ok && s.RequireSessions:
			res = Result{Err: ErrSessionExpired, Expired: true}
		case ok && op.Seq <= sess.Seq:
			res = sessionResult(sess, op.Seq) // duplicate: answer, do not re-execute
			sess.Last = max(sess.Last, op.Time)
			s.sessions[op.Client] = sess
		default:
			res = s.execute(op)
			s.sessions[op.Client] = session{Seq: op.Seq, Result: res, Last: max(sess.Last, op.Time)}
		}
	}
	if p, ok := s.waiters[waitKey{op.Client, op.Seq}]; ok {
		delete(s.waiters, waitKey{op.Client, op.Seq})
		p.res = res
		close(p.done)
	}
}

func (s *Store) execute(op Op) (res Result) {
	switch op.Kind {
	case OpSet:
		s.data[op.Key] = op.Value
	case OpDelete:
		_, res.Found = s.data[op.Key]
		delete(s.data, op.Key)
	case OpGet:
		res.Value, res.Found = s.data[op.Key]
	case OpKeepAlive:
	default:
		res.Err = "unknown op " + op.Kind
	}
	return res
}

func (s *Store) expired(sess session, now int64) bool {
	return s.SessionTTL > 0 && now-sess.Last > int64(s.SessionTTL)
}

// advance moves the state machine's clock to t (never backwards) and
// periodically forgets expired sessions. It reads nothing but log contents.
func (s *Store) advance(t int64) {
	s.tick = max(s.tick, t)
	if s.SessionTTL <= 0 || s.tick-s.sweptAt < int64(s.SessionTTL)/2 {
		return
	}
	s.sweptAt = s.tick
	for id, sess := range s.sessions {
		if s.expired(sess, s.tick) {
			delete(s.sessions, id)
		}
	}
}

// SessionCount reports how many client sessions are currently remembered.
func (s *Store) SessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

// HasSession reports whether id is a remembered session (it may still be
// expired by the next op).
func (s *Store) HasSession(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.sessions[id]
	return ok
}

type snapshotImage struct {
	Data     map[string]string
	Sessions map[string]session
	Tick     int64
	SweptAt  int64
}

// Snapshot implements raft.StateMachine.
// ponytail: with SessionTTL = 0 sessions are kept forever; an expired session's
// map entry also survives until the next sweep (every TTL/2 of log time).
func (s *Store) Snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(snapshotImage{s.data, s.sessions, s.tick, s.sweptAt})
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
	s.data, s.sessions, s.tick, s.sweptAt = img.Data, img.Sessions, img.Tick, img.SweptAt
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
