package transport

import (
	"bytes"
	"crypto/tls"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/clock"
	"github.com/nweber23/raftlab/internal/pki"
	"github.com/nweber23/raftlab/internal/raft"
)

// mtlsNode is one TLS server running the peer endpoint for a node.
type mtlsNode struct {
	id   raft.NodeID
	srv  *httptest.Server
	got  chan raft.Message
	cert tls.Certificate
}

func newMTLSNodes(t *testing.T, ca *pki.CA, ids ...raft.NodeID) map[raft.NodeID]*mtlsNode {
	t.Helper()
	nodes := map[raft.NodeID]*mtlsNode{}
	for _, id := range ids {
		cert, err := ca.IssuePair(id, "127.0.0.1")
		if err != nil {
			t.Fatal(err)
		}
		n := &mtlsNode{id: id, got: make(chan raft.Message, 16), cert: cert}
		h := Handler(id, ids, "", func(m raft.Message) { n.got <- m }, RequirePeerCert(pki.NodeName))
		mux := http.NewServeMux()
		mux.Handle("POST /raft", h)
		n.srv = httptest.NewUnstartedServer(mux)
		n.srv.TLS = &tls.Config{
			MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
			ClientCAs: ca.Pool(), ClientAuth: tls.VerifyClientCertIfGiven,
		}
		n.srv.StartTLS()
		t.Cleanup(n.srv.Close)
		nodes[id] = n
	}
	return nodes
}

// post sends m to node `to` as whoever owns client (nil certs = anonymous).
func post(t *testing.T, ca *pki.CA, to *mtlsNode, client []tls.Certificate, m raft.Message) (int, error) {
	t.Helper()
	body, err := Encode(m)
	if err != nil {
		t.Fatal(err)
	}
	c := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: ca.Pool(), Certificates: client},
	}}
	resp, err := c.Post(to.srv.URL+"/raft", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

func vote(from, to raft.NodeID) raft.Message {
	return raft.Message{From: from, To: to, Body: raft.RequestVoteArgs{Term: 1, CandidateID: from}}
}

func TestMTLSAcceptsOnlyTheCertifiedSender(t *testing.T) {
	ca, err := pki.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	nodes := newMTLSNodes(t, ca, 1, 2, 3)

	if code, err := post(t, ca, nodes[1], []tls.Certificate{nodes[2].cert}, vote(2, 1)); err != nil || code != http.StatusNoContent {
		t.Fatalf("genuine peer: %d %v", code, err)
	}
	select {
	case m := <-nodes[1].got:
		if m.From != 2 {
			t.Fatalf("delivered %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("message not delivered")
	}

	// Node 3 holds a valid certificate but claims to be node 2.
	if code, _ := post(t, ca, nodes[1], []tls.Certificate{nodes[3].cert}, vote(2, 1)); code != http.StatusForbidden {
		t.Fatalf("impersonation: %d, want 403", code)
	}
	// No certificate at all.
	if code, _ := post(t, ca, nodes[1], nil, vote(2, 1)); code != http.StatusUnauthorized {
		t.Fatalf("anonymous: %d, want 401", code)
	}
	// A certificate with the right name from some other CA never gets past the handshake.
	other, _ := pki.NewCA()
	rogue, err := other.IssuePair(2, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if code, err := post(t, ca, nodes[1], []tls.Certificate{rogue}, vote(2, 1)); err == nil {
		t.Fatalf("foreign CA accepted: %d", code)
	}
	select {
	case m := <-nodes[1].got:
		t.Fatalf("a rejected message was delivered: %+v", m)
	default:
	}
}

// Three real nodes over HTTPS with mutual TLS elect a leader and commit.
func TestRaftClusterRunsOverMTLS(t *testing.T) {
	ca, _ := pki.NewCA()
	ids := []raft.NodeID{1, 2, 3}
	nodes := newMTLSNodes(t, ca, ids...)
	var mu sync.Mutex
	applied := map[raft.NodeID][]string{}
	raftNodes := map[raft.NodeID]*raft.Node{}
	for _, id := range ids {
		others := map[raft.NodeID]string{}
		for _, o := range ids {
			if o != id {
				others[o] = nodes[o].srv.URL
			}
		}
		tr := NewHTTP(others, "", WithTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: ca.Pool(), Certificates: []tls.Certificate{nodes[id].cert}}))
		t.Cleanup(tr.Close)
		n, err := raft.New(raft.Config{
			ID: id, Peers: ids, Clock: clock.System{}, Transport: tr, PreVote: true, CheckQuorum: true,
			Machine:            recorder{id, &mu, applied},
			ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval: 50 * time.Millisecond, Rand: rand.New(rand.NewPCG(5, uint64(id))),
		})
		if err != nil {
			t.Fatal(err)
		}
		raftNodes[id] = n
		// Route incoming messages from the test servers' channels into the node.
		go func(src *mtlsNode) {
			for m := range src.got {
				n.Handle(m)
			}
		}(nodes[id])
		n.Start()
		t.Cleanup(n.Stop)
	}
	deadline := time.Now().Add(8 * time.Second)
	proposed := false
	for time.Now().Before(deadline) {
		for _, n := range raftNodes {
			if !proposed {
				if _, _, ok := n.Propose([]byte("over-tls")); ok {
					proposed = true
				}
			}
		}
		mu.Lock()
		done := len(applied[1]) == 1 && len(applied[2]) == 1 && len(applied[3]) == 1
		mu.Unlock()
		if done {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("no commit over mTLS: proposed=%v applied=%v", proposed, applied)
}

type recorder struct {
	id      raft.NodeID
	mu      *sync.Mutex
	applied map[raft.NodeID][]string
}

func (r recorder) Apply(_ uint64, c []byte) {
	r.mu.Lock()
	r.applied[r.id] = append(r.applied[r.id], string(c))
	r.mu.Unlock()
}
func (recorder) Snapshot() []byte { return nil }
func (recorder) Restore([]byte)   {}
