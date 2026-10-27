package kv_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/kv"
	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
)

func newKVCluster(t testing.TB, size int, seed uint64) (*raftest.Cluster, map[raft.NodeID]*kv.Store) {
	stores := map[raft.NodeID]*kv.Store{}
	c := raftest.NewCluster(t, raftest.Options{
		Size: size, Seed: seed, SnapshotThreshold: 20,
		Machine: func(raft.NodeID) raft.StateMachine { return kv.New() },
		OnBoot: func(id raft.NodeID, n *raft.Node, sm raft.StateMachine) {
			s := sm.(*kv.Store)
			s.Attach(n)
			stores[id] = s
		},
	})
	return c, stores
}

// do submits op to node id and runs the cluster until it completes.
func do(c *raftest.Cluster, stores map[raft.NodeID]*kv.Store, id raft.NodeID, op kv.Op) (kv.Result, error) {
	p, err := stores[id].Submit(op)
	if err != nil {
		return kv.Result{}, err
	}
	if !c.RunUntil(3*time.Second, func() bool {
		select {
		case <-p.Done():
			return true
		default:
			return false
		}
	}) {
		return kv.Result{}, fmt.Errorf("timeout")
	}
	return p.Result(), nil
}

func TestSetGetDeleteThroughRaft(t *testing.T) {
	c, st := newKVCluster(t, 3, 41)
	l := c.WaitLeader()
	must := func(op kv.Op) kv.Result {
		t.Helper()
		r, err := do(c, st, l, op)
		if err != nil {
			c.Fatalf("%+v: %v", op, err)
		}
		return r
	}
	must(kv.Op{Kind: kv.OpSet, Key: "k", Value: "v", Client: "c", Seq: 1})
	if r := must(kv.Op{Kind: kv.OpGet, Key: "k", Client: "c", Seq: 2}); r.Value != "v" || !r.Found {
		t.Fatalf("get: %+v", r)
	}
	must(kv.Op{Kind: kv.OpDelete, Key: "k", Client: "c", Seq: 3})
	if r := must(kv.Op{Kind: kv.OpGet, Key: "k", Client: "c", Seq: 4}); r.Found {
		t.Fatalf("deleted key still found: %+v", r)
	}
	c.Run(500 * time.Millisecond)
	for id, s := range st { // replicated everywhere
		if len(s.Data()) != 0 {
			t.Fatalf("node %d: %v", id, s.Data())
		}
	}
}

func TestFollowerRejectsSubmit(t *testing.T) {
	c, st := newKVCluster(t, 3, 42)
	l := c.WaitLeader()
	for id, s := range st {
		if id == l {
			continue
		}
		if _, err := s.Submit(kv.Op{Kind: kv.OpSet, Key: "k", Client: "c", Seq: 1}); err != kv.ErrNotLeader {
			t.Fatalf("follower %d: err=%v", id, err)
		}
	}
}

// The client times out and retries the same request on the new leader; the
// first attempt may or may not have survived. Either way: applied once.
func TestRetryAcrossFailoverAppliesOnce(t *testing.T) {
	c, st := newKVCluster(t, 3, 43)
	old := c.WaitLeader()
	c.Net.Isolate(old)
	p, err := st[old].Submit(kv.Op{Kind: kv.OpSet, Key: "k", Value: "v1", Client: "c", Seq: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !c.RunUntil(3*time.Second, func() bool { x := c.Leader(); return x != raft.NoNode && x != old }) {
		c.Fatalf("no failover")
	}
	select {
	case <-p.Done():
		c.Fatalf("isolated leader acknowledged a write")
	default:
	}
	fresh := c.Leader()
	if _, err := do(c, st, fresh, kv.Op{Kind: kv.OpSet, Key: "k", Value: "v1", Client: "c", Seq: 1}); err != nil {
		c.Fatalf("retry: %v", err)
	}
	if _, err := do(c, st, fresh, kv.Op{Kind: kv.OpSet, Key: "k", Value: "v2", Client: "c", Seq: 2}); err != nil {
		c.Fatalf("second write: %v", err)
	}
	c.Net.Heal() // the old leader's copy of seq 1 may now resurface; it must not undo seq 2
	c.Run(2 * time.Second)
	for id, s := range st {
		if s.Data()["k"] != "v2" {
			c.Fatalf("node %d has k=%q", id, s.Data()["k"])
		}
	}
	select {
	case <-p.Done(): // old waiter is released once the op is applied anywhere on that node
	default:
		c.Fatalf("original waiter never released")
	}
}

func TestStaleRequestIsRejected(t *testing.T) {
	c, st := newKVCluster(t, 3, 44)
	l := c.WaitLeader()
	do(c, st, l, kv.Op{Kind: kv.OpSet, Key: "k", Value: "a", Client: "c", Seq: 2})
	r, err := do(c, st, l, kv.Op{Kind: kv.OpSet, Key: "k", Value: "b", Client: "c", Seq: 1})
	if err != nil || r.Err == "" {
		t.Fatalf("stale seq: res=%+v err=%v", r, err)
	}
	if st[l].Data()["k"] != "a" {
		t.Fatal("stale request changed state")
	}
}

func TestStoreRebuiltAfterRestart(t *testing.T) {
	c, st := newKVCluster(t, 3, 45)
	l := c.WaitLeader()
	for i := 1; i <= 60; i++ { // crosses several snapshot thresholds
		if _, err := do(c, st, l, kv.Op{Kind: kv.OpSet, Key: fmt.Sprint("k", i%7), Value: fmt.Sprint(i), Client: "c", Seq: uint64(i)}); err != nil {
			c.Fatalf("write %d: %v", i, err)
		}
	}
	c.Run(time.Second)
	want := st[l].Data()
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}
	c.WaitLeader()
	if !c.RunUntil(5*time.Second, func() bool {
		for _, id := range c.IDs {
			d := st[id].Data()
			if len(d) != len(want) {
				return false
			}
			for k, v := range want {
				if d[k] != v {
					return false
				}
			}
		}
		return true
	}) {
		c.Fatalf("want %v, got %v %v %v", want, st[1].Data(), st[2].Data(), st[3].Data())
	}
}
