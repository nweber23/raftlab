package kv_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/kv"
	"github.com/nweber23/raftlab/internal/linz"
	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
)

// The chaos test crashes nodes, partitions the network and perturbs delivery
// while clients read and write shared keys, then heals everything, checks that
// the replicas converge, and checks the complete recorded history for
// linearizability. The cluster runs on a fake
// clock, so a seed reproduces a run exactly:
//
//	CHAOS_SEED=17 go test -race -run Chaos -v ./internal/kv
//
// CHAOS_SEEDS=N runs seeds 1..N (default 5).
func TestChaos(t *testing.T) {
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		runChaos(t, seed)
		return
	}
	n := 5
	if s := os.Getenv("CHAOS_SEEDS"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	for seed := uint64(1); seed <= uint64(n); seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) { runChaos(t, seed) })
	}
}

// chaosClient is a sequential client. Clients share a few keys, so reads can
// observe anyone's writes; whether the result is plausible is decided by the
// linearizability checker, not by the client.
type chaosClient struct {
	n       int
	id      string
	seq     uint64
	vals    int // values written so far, to keep every write unique
	op      *kv.Op
	viaRead bool // a get served by ReadIndex instead of the log
	call    int  // step at which the current op was invoked
	pending *kv.Pending
	sentAt  int
	done    int
	stopNew bool
}

var chaosKeys = []string{"a", "b"}

func runChaos(t *testing.T, seed uint64) {
	const (
		nodes      = 5
		chaosSteps = 4000 // 40s of virtual time
		retryAfter = 100  // steps (1s) before a client re-sends
	)
	// Odd seeds run the classic protocol, where a deposed leader lingers and
	// stale reads are a real possibility; even seeds run with leases.
	c, stores := newKVClusterOpts(t, nodes, seed, seed%2 == 1)
	rng := rand.New(rand.NewPCG(seed, 0xc4a05))
	clients := make([]*chaosClient, 4)
	for i := range clients {
		clients[i] = &chaosClient{n: i, id: fmt.Sprint("client", i)}
	}
	var history []linz.KVOp
	down := func(id raft.NodeID) bool { return c.Net.Faults().DownNodes[id] }
	pickUp := func() (raft.NodeID, bool) {
		var up []raft.NodeID
		for _, id := range c.IDs {
			if !down(id) {
				up = append(up, id)
			}
		}
		if len(up) == 0 {
			return 0, false
		}
		return up[rng.IntN(len(up))], true
	}
	act := func(cl *chaosClient) {
		cl.act(c, stores, rng, pickUp, stepCounter, retryAfter, &history)
	}

	step := func(chaos bool) {
		if chaos {
			switch r := rng.IntN(600); {
			case r < 4: // crash
				if id, ok := pickUp(); ok {
					c.Crash(id)
				}
			case r < 10: // restart someone
				for _, id := range c.IDs {
					if down(id) {
						c.Restart(id)
						break
					}
				}
			case r < 13: // partition into 1-2 isolated nodes and the rest
				ids := append([]raft.NodeID(nil), c.IDs...)
				rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
				k := 1 + rng.IntN(2)
				c.Net.Partition(ids[:k], ids[k:])
			case r < 17:
				c.Net.Heal()
			case r < 20:
				c.Net.SetLoss(rng.Float64()*0.3, rng.Float64()*0.3)
				c.Net.SetDelay(0, time.Duration(rng.IntN(60))*time.Millisecond)
			}
		}
		for _, cl := range clients {
			act(cl)
		}
		c.Run(raftest.Step)
	}

	for i := 0; i < chaosSteps; i++ {
		stepCounter++
		step(true)
	}

	// Heal: everything up, network clean. Clients keep going meanwhile.
	for _, id := range c.IDs {
		if down(id) {
			c.Restart(id)
		}
	}
	c.Net.Heal()
	c.Net.SetLoss(0, 0)
	c.Net.SetDelay(0, 0)
	for i := 0; i < 500; i++ {
		stepCounter++
		step(false)
	}
	for _, cl := range clients { // let in-flight requests finish, then stop
		cl.stopNew = true
	}
	if !c.RunUntil(10*time.Second, func() bool {
		for _, cl := range clients {
			if cl.op != nil {
				stepCounter++
				act(cl)
				if cl.op != nil {
					return false
				}
			}
		}
		return true
	}) {
		c.Fatalf("clients still blocked after healing")
	}

	// Convergence: identical stores and commit points on every node.
	if !c.RunUntil(10*time.Second, func() bool { return replicasEqual(c, stores) }) {
		c.Fatalf("replicas did not converge: %v | %v | %v", stores[1].Data(), stores[2].Data(), stores[3].Data())
	}

	// Linearizability: whatever the crashes, partitions and retries did, the
	// operations clients saw must be explainable by one total order that
	// respects real time. This covers durability (an acknowledged write is
	// never lost), freshness (no stale read, log or ReadIndex) and
	// exactly-once execution of retried writes.
	if len(history) == 0 {
		c.Fatalf("no operation ever completed; the test proved nothing")
	}
	var writes, reads int
	for _, o := range history {
		if o.Kind == linz.KVGet {
			reads++
		} else {
			writes++
		}
	}
	if err := linz.CheckKV(history); err != nil {
		c.Fatalf("history of %d operations is not linearizable: %v", len(history), err)
	}

	// And the cluster is alive.
	l := c.WaitLeader()
	if _, err := do(c, stores, l, kv.Op{Kind: kv.OpSet, Key: "final", Value: "ok", Client: "final", Seq: 1}); err != nil {
		c.Fatalf("cluster unavailable after healing: %v", err)
	}
	t.Logf("seed %d: %d linearizable operations (%d writes, %d reads)", seed, len(history), writes, reads)
}

// stepCounter is the virtual time in steps; chaos runs are single-goroutine
// and sequential, so a package variable is enough.
// ponytail: global, so chaos subtests must not use t.Parallel.
var stepCounter int

func (cl *chaosClient) act(c *raftest.Cluster, stores map[raft.NodeID]*kv.Store, rng *rand.Rand,
	pickUp func() (raft.NodeID, bool), now, retryAfter int, history *[]linz.KVOp) {
	if cl.op == nil {
		if cl.stopNew {
			return
		}
		cl.seq++
		cl.call, cl.viaRead = now, false
		key := chaosKeys[rng.IntN(len(chaosKeys))]
		switch r := rng.IntN(10); {
		case r < 5:
			cl.vals++
			cl.op = &kv.Op{Kind: kv.OpSet, Key: key, Value: fmt.Sprintf("%s#%d", cl.id, cl.vals), Client: cl.id, Seq: cl.seq}
		case r < 8:
			cl.op = &kv.Op{Kind: kv.OpGet, Key: key, Client: cl.id, Seq: cl.seq}
			cl.viaRead = rng.IntN(2) == 0
		default:
			cl.op = &kv.Op{Kind: kv.OpDelete, Key: key, Client: cl.id, Seq: cl.seq}
		}
		cl.pending = nil
	}
	if cl.pending != nil {
		select {
		case <-cl.pending.Done():
			if cl.pending.Err() != nil { // the reading leader lost its seat: ask again
				cl.pending = nil
			} else {
				cl.finish(c, now, history)
				return
			}
		default:
			if now-cl.sentAt < retryAfter {
				return
			}
		}
	}
	// (Re)send to a random live node; followers just refuse.
	if id, ok := pickUp(); ok {
		var p *kv.Pending
		var err error
		if cl.viaRead {
			p, err = stores[id].Read(cl.op.Key)
		} else {
			p, err = stores[id].Submit(*cl.op)
		}
		if err == nil {
			cl.pending, cl.sentAt = p, now
		}
	}
}

func (cl *chaosClient) finish(c *raftest.Cluster, now int, history *[]linz.KVOp) {
	res := cl.pending.Result()
	if res.Err != "" {
		c.Fatalf("%s: unexpected error %q for %+v", cl.id, res.Err, *cl.op)
	}
	*history = append(*history, linz.KVOp{
		Client: cl.n, Call: int64(cl.call), Return: int64(now),
		Kind: cl.op.Kind, Key: cl.op.Key, Value: cl.op.Value, Found: res.Found, Got: res.Value,
	})
	cl.done++
	cl.op, cl.pending = nil, nil
}

func replicasEqual(c *raftest.Cluster, stores map[raft.NodeID]*kv.Store) bool {
	var ref raft.Status
	for i, id := range c.IDs {
		s := c.Nodes[id].Status()
		if s.LastApplied != s.CommitIndex {
			return false
		}
		if i == 0 {
			ref = s
		} else if s.CommitIndex != ref.CommitIndex {
			return false
		}
		a, b := stores[id].Data(), stores[c.IDs[0]].Data()
		if len(a) != len(b) {
			return false
		}
		for k, v := range a {
			if b[k] != v {
				return false
			}
		}
	}
	return true
}
