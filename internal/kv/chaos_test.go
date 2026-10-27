package kv_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/kv"
	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/raft/raftest"
)

// The chaos test crashes nodes, partitions the network and perturbs delivery
// while clients write, then heals everything and checks that the replicas
// converge and no acknowledged write was lost. The cluster runs on a fake
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

// chaosClient is a sequential client with a private key it only ever
// increments, so every read has exactly one correct answer.
type chaosClient struct {
	id        string
	key       string
	seq       uint64
	written   int // value of the newest Set it has started
	acked     int // value of the newest Set that was acknowledged
	op        *kv.Op
	pending   *kv.Pending
	sentAt    int
	ackedSets int
	stopNew   bool
}

func runChaos(t *testing.T, seed uint64) {
	const (
		nodes      = 5
		chaosSteps = 4000 // 40s of virtual time
		retryAfter = 100  // steps (1s) before a client re-sends
	)
	c, stores := newKVCluster(t, nodes, seed)
	rng := rand.New(rand.NewPCG(seed, 0xc4a05))
	clients := make([]*chaosClient, 4)
	for i := range clients {
		clients[i] = &chaosClient{id: fmt.Sprint("client", i), key: fmt.Sprint("key", i)}
	}
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
			cl.act(c, stores, rng, pickUp, chaosStepNow(c), retryAfter)
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
				cl.act(c, stores, rng, pickUp, chaosStepNow(c), retryAfter)
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

	// Durability: every acknowledged write is still there (the client is
	// sequential, so the value is the last ack or the one write in flight).
	data := stores[c.IDs[0]].Data()
	total := 0
	for _, cl := range clients {
		got, err := strconv.Atoi(data[cl.key])
		if cl.acked > 0 && (err != nil || got < cl.acked || got > cl.written) {
			c.Fatalf("%s: store has %q, acked %d, attempted %d: acknowledged write lost", cl.id, data[cl.key], cl.acked, cl.written)
		}
		total += cl.ackedSets
	}
	if total == 0 {
		c.Fatalf("no write was ever acknowledged; the test proved nothing")
	}

	// And the cluster is alive.
	l := c.WaitLeader()
	if _, err := do(c, stores, l, kv.Op{Kind: kv.OpSet, Key: "final", Value: "ok", Client: "final", Seq: 1}); err != nil {
		c.Fatalf("cluster unavailable after healing: %v", err)
	}
	t.Logf("seed %d: %d acknowledged writes", seed, total)
}

// stepCounter is the virtual time in steps; chaos runs are single-goroutine
// and sequential, so a package variable is enough.
// ponytail: global, so chaos subtests must not use t.Parallel.
var stepCounter int

func chaosStepNow(*raftest.Cluster) int { return stepCounter }

func (cl *chaosClient) act(c *raftest.Cluster, stores map[raft.NodeID]*kv.Store, rng *rand.Rand,
	pickUp func() (raft.NodeID, bool), now, retryAfter int) {
	if cl.op == nil {
		if cl.stopNew {
			return
		}
		cl.seq++
		if cl.written > 0 && rng.IntN(3) == 0 {
			cl.op = &kv.Op{Kind: kv.OpGet, Key: cl.key, Client: cl.id, Seq: cl.seq}
		} else {
			cl.written++
			cl.op = &kv.Op{Kind: kv.OpSet, Key: cl.key, Value: strconv.Itoa(cl.written), Client: cl.id, Seq: cl.seq}
		}
		cl.pending = nil
	}
	if cl.pending != nil {
		select {
		case <-cl.pending.Done():
			cl.finish(c)
			return
		default:
		}
		if now-cl.sentAt < retryAfter {
			return
		}
	}
	// (Re)send to a random live node; followers just refuse.
	if id, ok := pickUp(); ok {
		if p, err := stores[id].Submit(*cl.op); err == nil {
			cl.pending, cl.sentAt = p, now
		}
	}
}

func (cl *chaosClient) finish(c *raftest.Cluster) {
	res := cl.pending.Result()
	if res.Err != "" {
		c.Fatalf("%s: unexpected error %q for %+v", cl.id, res.Err, *cl.op)
	}
	switch cl.op.Kind {
	case kv.OpSet:
		cl.acked = cl.written
		cl.ackedSets++
	case kv.OpGet:
		// Sequential client: the only possible answer is its last acked write.
		if cl.acked > 0 && (!res.Found || res.Value != strconv.Itoa(cl.acked)) {
			c.Fatalf("%s: read %q (found=%v), want %d: stale or lost write", cl.id, res.Value, res.Found, cl.acked)
		}
	}
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
