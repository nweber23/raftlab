package raft

import (
	"encoding/json"
	"errors"
	"slices"
)

// Membership is a cluster membership. Old is empty except during a membership
// change, when the cluster runs the joint configuration C(old,new): every
// decision needs a majority of Voters and a majority of Old (§6, Raft thesis
// §4.3), so no single side can act alone while servers are being swapped.
type Membership struct {
	Voters []NodeID `json:"voters"`
	Old    []NodeID `json:"old,omitempty"`
}

// Joint reports whether c is a joint configuration.
func (c Membership) Joint() bool { return len(c.Old) > 0 }

// Has reports whether id votes under c (in either half of a joint config).
func (c Membership) Has(id NodeID) bool {
	return slices.Contains(c.Voters, id) || slices.Contains(c.Old, id)
}

// Quorum reports whether the nodes for which ok is true form a majority of
// Voters and, in a joint configuration, of Old as well. A config with no
// voters has no quorum.
func (c Membership) Quorum(ok func(NodeID) bool) bool {
	return majorityOf(c.Voters, ok) && (!c.Joint() || majorityOf(c.Old, ok))
}

func majorityOf(set []NodeID, ok func(NodeID) bool) bool {
	n := 0
	for _, id := range set {
		if ok(id) {
			n++
		}
	}
	return len(set) > 0 && n >= len(set)/2+1
}

// members returns every node in c, sorted, so iteration order is deterministic.
func (c Membership) members() []NodeID {
	all := slices.Concat(c.Voters, c.Old)
	slices.Sort(all)
	return slices.Compact(all)
}

func normalize(ids []NodeID) []NodeID {
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}

// Membership change errors.
var (
	ErrNotLeader     = errors.New("raft: not the leader")
	ErrConfigPending = errors.New("raft: another membership change is in progress")
	ErrNotReady      = errors.New("raft: leader has not committed an entry in its term yet")
	ErrBadConfig     = errors.New("raft: configuration needs at least one voter, none of them zero")
)

// ChangeMembership starts replacing the voter set with voters, using joint
// consensus: the leader appends C(old,new), and once that commits, appends C(new)
// by itself. The returned index is that of the first entry; the change is
// complete when Status shows a non-joint Membership equal to voters with
// ConfigIndex <= CommitIndex. A leader that is not in voters steps down once
// C(new) commits.
//
// New servers should be started first (with no Peers). A server that is not
// yet in the configuration first receives the log as a non-voting learner, and
// C(old,new) is appended only once every such server has caught up to the
// commit index, so a far-behind newcomer cannot stall commits. In that case the
// returned index is 0 and the change is learned of through Status as before.
// Calling again while still catching up replaces the target, which is how a
// change stuck on a dead newcomer is abandoned.
func (n *Node) ChangeMembership(voters []NodeID) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role != Leader {
		return 0, ErrNotLeader
	}
	voters = normalize(voters)
	if len(voters) == 0 || voters[0] == NoNode {
		return 0, ErrBadConfig
	}
	// One change at a time, and only once this term has committed something:
	// otherwise the leader might act on a configuration it cannot yet prove
	// is the committed one (thesis §4.1).
	if n.conf.Joint() || n.confIndex > n.commitIndex {
		return 0, ErrConfigPending
	}
	if n.termAt(n.commitIndex) != n.currentTerm {
		return 0, ErrNotReady
	}
	n.target, n.learners = voters, map[NodeID]bool{}
	for _, id := range voters {
		if !n.conf.Has(id) {
			n.learners[id] = true
		}
	}
	n.syncProgress()
	if idx := n.promote(); idx != 0 {
		return idx, nil
	}
	n.replicate()
	return 0, nil
}

// promote appends C(old,new) once every learner has caught up to the commit
// index, and returns its index (0 if it is still waiting).
func (n *Node) promote() uint64 {
	if n.target == nil {
		return 0
	}
	for id := range n.learners {
		if n.matchIndex[id] < n.commitIndex {
			return 0
		}
	}
	idx := n.appendConfig(Membership{Voters: n.target, Old: n.conf.Voters})
	n.target, n.learners = nil, nil
	n.replicate()
	return idx
}

func encodeConfig(c Membership) []byte {
	b, _ := json.Marshal(c) // cannot fail: plain slices of integers
	return b
}

func decodeConfig(b []byte) Membership {
	var c Membership
	if err := json.Unmarshal(b, &c); err != nil {
		panic("raft: corrupt config entry: " + err.Error())
	}
	return c
}

// peers lists every other node this one must talk to under its current config.
func (n *Node) peers() []NodeID {
	all := n.conf.members()
	for id := range n.learners {
		all = append(all, id)
	}
	for id := range n.departing {
		all = append(all, id)
	}
	slices.Sort(all)
	return slices.DeleteFunc(all, func(id NodeID) bool { return id == n.cfg.ID })
}

// appendConfig appends a config entry to the leader's log and adopts it at once.
func (n *Node) appendConfig(c Membership) uint64 {
	n.log = append(n.log, Entry{Term: n.currentTerm, Kind: EntryConfig, Command: encodeConfig(c)})
	n.persistAppend(n.lastIndex()-1, n.entries(n.lastIndex()-1, n.lastIndex()))
	n.matchIndex[n.cfg.ID] = n.lastIndex()
	n.conf, n.confIndex = c, n.lastIndex()
	n.syncProgress()
	n.advanceCommit()
	return n.confIndex
}

// configAt returns the membership in effect at log index idx.
func (n *Node) configAt(idx uint64) (Membership, uint64) {
	for i := idx; i > n.snapIndex; i-- {
		if e := n.log[i-n.snapIndex-1]; e.Kind == EntryConfig {
			return decodeConfig(e.Command), i
		}
	}
	return n.snapConf, 0
}

// refreshConfig recomputes the active configuration after the log changed
// underneath it (truncation, snapshot install, restart).
func (n *Node) refreshConfig() {
	n.conf, n.confIndex = n.configAt(n.lastIndex())
	n.syncProgress()
}

// syncProgress gives a leader replication state for peers that just joined.
func (n *Node) syncProgress() {
	if n.role != Leader {
		return
	}
	for _, p := range n.peers() {
		if _, ok := n.nextIndex[p]; !ok {
			n.nextIndex[p] = n.lastIndex() + 1
		}
	}
}

// afterCommit drives a membership change forward once its entry commits: the
// leader follows C(old,new) with C(new), and leaves when C(new) excludes it.
func (n *Node) afterCommit() {
	if n.role != Leader || n.confIndex > n.commitIndex {
		return
	}
	switch {
	case n.conf.Joint():
		// Servers leaving only have C(old,new), which still lists them. Keep
		// sending to them until they hold C(new) and know they are gone.
		// ponytail: a leader that removes itself, or a crashed removed server,
		// is not covered; PreVote and the lease still keep those out.
		gone := n.conf.Old
		n.appendConfig(Membership{Voters: n.conf.Voters})
		n.departing = map[NodeID]bool{}
		for _, id := range gone {
			if !n.conf.Has(id) && id != n.cfg.ID {
				n.departing[id] = true
			}
		}
		n.replicate()
	case !n.conf.Has(n.cfg.ID):
		n.becomeFollower(n.currentTerm)
	}
}

// dropDeparted stops sending to removed servers that now hold C(new).
func (n *Node) dropDeparted() {
	for id := range n.departing {
		if n.matchIndex[id] >= n.confIndex {
			delete(n.departing, id)
		}
	}
}
