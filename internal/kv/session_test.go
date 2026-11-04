package kv_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/kv"
	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
)

const ttl = 10 * time.Second

// sessionCluster is a KV cluster whose stores stamp ops with the cluster's
// virtual time and require registered, expiring sessions.
func sessionCluster(t *testing.T, seed uint64) (*raftest.Cluster, map[raft.NodeID]*kv.Store) {
	t.Helper()
	stores := map[raft.NodeID]*kv.Store{}
	var c *raftest.Cluster
	base := time.Unix(1_700_000_000, 0)
	c = raftest.NewCluster(t, raftest.Options{
		Size: 3, Seed: seed, SnapshotThreshold: 15,
		Machine: func(raft.NodeID) raft.StateMachine {
			s := kv.New()
			s.SessionTTL, s.RequireSessions = ttl, true
			s.Now = func() time.Time { return base.Add(c.Clock.Now()) }
			return s
		},
		OnBoot: func(id raft.NodeID, n *raft.Node, sm raft.StateMachine) {
			sm.(*kv.Store).Attach(n)
			stores[id] = sm.(*kv.Store)
		},
	})
	return c, stores
}

type sess struct {
	c      *raftest.Cluster
	stores map[raft.NodeID]*kv.Store
	id     string
	seq    uint64
}

func (s *sess) do(kind, key, val string) kv.Result {
	s.c.T.Helper()
	s.seq++
	r, err := do(s.c, s.stores, s.c.WaitLeader(), kv.Op{Kind: kind, Key: key, Value: val, Client: s.id, Seq: s.seq})
	if err != nil {
		s.c.Fatalf("%s %s: %v", kind, key, err)
	}
	return r
}

func register(c *raftest.Cluster, stores map[raft.NodeID]*kv.Store, tag string) *sess {
	c.T.Helper()
	r, err := do(c, stores, c.WaitLeader(), kv.Op{Kind: kv.OpRegister, Client: "anon-" + tag, Seq: 1, Anon: true})
	if err != nil || r.Value == "" {
		c.Fatalf("register: %+v %v", r, err)
	}
	return &sess{c: c, stores: stores, id: r.Value}
}

func TestRegisteredIDsAreUniqueAndReplicated(t *testing.T) {
	c, st := sessionCluster(t, 121)
	a, b := register(c, st, "a"), register(c, st, "b")
	if a.id == b.id {
		t.Fatalf("two clients got the same ID %q", a.id)
	}
	c.Run(time.Second)
	for id, s := range st {
		if !s.HasSession(a.id) || !s.HasSession(b.id) {
			t.Fatalf("node %d does not know both sessions", id)
		}
	}
	if r := a.do(kv.OpSet, "k", "v"); r.Err != "" {
		t.Fatalf("write with a registered ID: %+v", r)
	}
}

func TestUnknownClientIDIsRejected(t *testing.T) {
	c, st := sessionCluster(t, 122)
	r, err := do(c, st, c.WaitLeader(), kv.Op{Kind: kv.OpSet, Key: "k", Value: "v", Client: "made-up", Seq: 1})
	if err != nil || !r.Expired {
		t.Fatalf("want an expired-session result, got %+v err=%v", r, err)
	}
	c.Run(time.Second)
	for _, s := range st {
		if len(s.Data()) != 0 {
			t.Fatal("a rejected op changed state")
		}
	}
}

// Expiry is decided by the timestamps in the log, so it needs later traffic to
// move the state machine's clock; every replica then agrees on who is gone.
func TestIdleSessionExpiresEverywhere(t *testing.T) {
	c, st := sessionCluster(t, 123)
	idle, busy := register(c, st, "idle"), register(c, st, "busy")
	idle.do(kv.OpSet, "k", "1")
	for i := 0; i < 4; i++ { // busy keeps its session alive while idle falls silent
		c.Run(ttl / 2)
		busy.do(kv.OpKeepAlive, "", "")
	}
	if r := idle.do(kv.OpSet, "k", "2"); !r.Expired {
		t.Fatalf("idle session survived %v of silence: %+v", 2*ttl, r)
	}
	if r := busy.do(kv.OpSet, "k", "3"); r.Err != "" {
		t.Fatalf("active session was expired: %+v", r)
	}
	c.Run(time.Second)
	for id, s := range st {
		if s.Data()["k"] != "3" || s.HasSession(idle.id) || !s.HasSession(busy.id) {
			t.Fatalf("node %d disagrees: k=%q idle=%v busy=%v", id, s.Data()["k"], s.HasSession(idle.id), s.HasSession(busy.id))
		}
	}
}

// The reason sessions expire only with RequireSessions: a retry of an old
// request arriving after expiry must be refused, not executed a second time.
func TestLateDuplicateAfterExpiryIsNotReapplied(t *testing.T) {
	c, st := sessionCluster(t, 124)
	a, b := register(c, st, "a"), register(c, st, "b")
	a.do(kv.OpSet, "k", "first")
	b.do(kv.OpSet, "k", "second")
	c.Run(2 * ttl)
	b.do(kv.OpKeepAlive, "", "") // moves the clock; a is now expired
	r, err := do(c, st, c.WaitLeader(), kv.Op{Kind: kv.OpSet, Key: "k", Value: "first", Client: a.id, Seq: 1})
	if err != nil || !r.Expired {
		t.Fatalf("late duplicate: %+v %v", r, err)
	}
	if st[c.WaitLeader()].Data()["k"] != "second" {
		t.Fatal("the duplicate overwrote newer data")
	}
}

func TestSessionsSurviveSnapshotAndRestart(t *testing.T) {
	c, st := sessionCluster(t, 125)
	a, b := register(c, st, "a"), register(c, st, "b")
	for i := 0; i < 40; i++ { // several snapshots
		a.do(kv.OpSet, fmt.Sprint("k", i%5), fmt.Sprint(i))
	}
	c.Run(time.Second)
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}
	c.WaitLeader()
	c.Run(2 * time.Second)
	if r := a.do(kv.OpSet, "after", "1"); r.Err != "" {
		t.Fatalf("session lost across restart: %+v", r)
	}
	c.Run(2 * ttl)
	a.do(kv.OpKeepAlive, "", "")
	if r := b.do(kv.OpSet, "x", "y"); !r.Expired { // b stayed silent through the restart and the wait, so its restored session is stale
		t.Fatalf("a long-idle session survived the restart: %+v", r)
	}
}

func TestAnonymousOpsLeaveNoSession(t *testing.T) {
	c, st := newKVCluster(t, 3, 126)
	l := c.WaitLeader()
	for i := 0; i < 30; i++ {
		if _, err := do(c, st, l, kv.Op{Kind: kv.OpSet, Key: "k", Value: "v", Client: fmt.Sprint("anon-", i), Seq: 1, Anon: true}); err != nil {
			t.Fatal(err)
		}
	}
	c.Run(time.Second)
	for id, s := range st {
		if s.SessionCount() != 0 || s.Data()["k"] != "v" {
			t.Fatalf("node %d: %d sessions, k=%q", id, s.SessionCount(), s.Data()["k"])
		}
	}
}

func TestHTTPSessionFlow(t *testing.T) {
	urls, nodes := httpCluster(t, func(s *kv.Store) { s.SessionTTL, s.RequireSessions = time.Minute, true })
	l := waitHTTPLeader(t, nodes)
	base := urls[l]

	if resp, _ := request(t, "PUT", base+"/kv/k", "v", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous write on a session-only cluster: %d", resp.StatusCode)
	}
	resp, id := request(t, "POST", base+"/session", "", nil)
	if resp.StatusCode != 200 || !strings.HasPrefix(id, "s") {
		t.Fatalf("register: %d %q", resp.StatusCode, id)
	}
	hdr := map[string]string{"X-Client-ID": id, "X-Request-ID": "1"}
	if resp, body := request(t, "PUT", base+"/kv/k", "v", hdr); resp.StatusCode != 204 {
		t.Fatalf("write: %d %s", resp.StatusCode, body)
	}
	hdr["X-Request-ID"] = "2"
	if resp, _ := request(t, "POST", base+"/keepalive", "", hdr); resp.StatusCode != 204 {
		t.Fatalf("keepalive: %d", resp.StatusCode)
	}
	bogus := map[string]string{"X-Client-ID": "s999999", "X-Request-ID": "1"}
	if resp, body := request(t, "PUT", base+"/kv/k", "x", bogus); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown session: %d %s", resp.StatusCode, body)
	}
	if resp, body := request(t, "GET", base+"/kv/k", "", nil); resp.StatusCode != 200 || body != "v" {
		t.Fatalf("reads need no session: %d %q", resp.StatusCode, body)
	}
}
