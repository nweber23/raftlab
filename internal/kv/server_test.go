package kv_test

import (
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/clock"
	"github.com/nweber23/raftlab/internal/kv"
	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/transport"
)

// httpCluster runs three real-clock nodes with an httptest server each.
func httpCluster(t *testing.T) (urls map[raft.NodeID]string, nodes map[raft.NodeID]*raft.Node) {
	t.Helper()
	net := transport.NewNetwork(clock.System{}, rand.New(rand.NewPCG(1, 1)))
	ids := []raft.NodeID{1, 2, 3}
	urls, nodes = map[raft.NodeID]string{}, map[raft.NodeID]*raft.Node{}
	type pair struct {
		n *raft.Node
		s *kv.Store
	}
	built := map[raft.NodeID]pair{}
	for _, id := range ids {
		s := kv.New()
		n, err := raft.New(raft.Config{
			ID: id, Peers: ids, Clock: clock.System{}, Transport: net, Machine: s,
			ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval: 50 * time.Millisecond, Rand: rand.New(rand.NewPCG(7, uint64(id))),
		})
		if err != nil {
			t.Fatal(err)
		}
		s.Attach(n)
		net.Register(id, n.Handle)
		built[id], nodes[id] = pair{n, s}, n
		srv := httptest.NewServer((&kv.Server{Store: s, Node: n, Timeout: 2 * time.Second,
			LeaderURL: func(l raft.NodeID) string { return urls[l] }}).Handler())
		t.Cleanup(srv.Close)
		urls[id] = srv.URL
	}
	for _, p := range built {
		p.n.Start()
		t.Cleanup(p.n.Stop)
	}
	return urls, nodes
}

func request(t *testing.T, method, url, body string, hdr map[string]string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func waitHTTPLeader(t *testing.T, nodes map[raft.NodeID]*raft.Node) raft.NodeID {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		for id, n := range nodes {
			if n.Status().Role == raft.Leader {
				return id
			}
		}
	}
	t.Fatal("no leader")
	return 0
}

func TestHTTPPutGetDeleteWithLeaderRedirect(t *testing.T) {
	urls, nodes := httpCluster(t)
	leader := waitHTTPLeader(t, nodes)
	var follower raft.NodeID
	for id := range urls {
		if id != leader {
			follower = id
		}
	}
	// Wait until the follower knows who leads, otherwise it answers 503.
	for i := 0; i < 100 && nodes[follower].Status().Leader == raft.NoNode; i++ {
		time.Sleep(20 * time.Millisecond)
	}

	// PUT through a follower: the client library follows the 307.
	resp, _ := request(t, "PUT", urls[follower]+"/kv/hello", "world", map[string]string{"X-Client-ID": "t", "X-Request-ID": "1"})
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("put via follower: %d", resp.StatusCode)
	}
	if got := resp.Request.URL.String(); !strings.HasPrefix(got, urls[leader]) {
		t.Fatalf("request ended at %s, want leader %s", got, urls[leader])
	}
	if resp, body := request(t, "GET", urls[leader]+"/kv/hello", "", nil); resp.StatusCode != 200 || body != "world" {
		t.Fatalf("get: %d %q", resp.StatusCode, body)
	}
	if resp, _ := request(t, "GET", urls[leader]+"/kv/nope", "", nil); resp.StatusCode != 404 {
		t.Fatalf("missing key: %d", resp.StatusCode)
	}
	if resp, _ := request(t, "DELETE", urls[leader]+"/kv/hello", "", map[string]string{"X-Client-ID": "t", "X-Request-ID": "2"}); resp.StatusCode != 204 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}
	if resp, _ := request(t, "GET", urls[leader]+"/kv/hello", "", nil); resp.StatusCode != 404 {
		t.Fatalf("after delete: %d", resp.StatusCode)
	}
}

func TestHTTPDuplicateRequestIDIsIdempotent(t *testing.T) {
	urls, nodes := httpCluster(t)
	l := waitHTTPLeader(t, nodes)
	h := map[string]string{"X-Client-ID": "t", "X-Request-ID": "1"}
	request(t, "PUT", urls[l]+"/kv/k", "first", h)
	h2 := map[string]string{"X-Client-ID": "t", "X-Request-ID": "2"}
	request(t, "PUT", urls[l]+"/kv/k", "second", h2)
	// A late retry of request 1 gets a conflict and changes nothing.
	if resp, _ := request(t, "PUT", urls[l]+"/kv/k", "first", h); resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale retry: %d", resp.StatusCode)
	}
	if _, body := request(t, "GET", urls[l]+"/kv/k", "", nil); body != "second" {
		t.Fatalf("k = %q", body)
	}
}

func TestHTTPBadRequestID(t *testing.T) {
	urls, nodes := httpCluster(t)
	l := waitHTTPLeader(t, nodes)
	if resp, _ := request(t, "PUT", urls[l]+"/kv/k", "v", map[string]string{"X-Client-ID": "t", "X-Request-ID": "abc"}); resp.StatusCode != 400 {
		t.Fatalf("got %d", resp.StatusCode)
	}
	if resp, _ := request(t, "PUT", urls[l]+"/kv/k", "v", map[string]string{"X-Client-ID": "t"}); resp.StatusCode != 400 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestHTTPNoLeaderIs503(t *testing.T) {
	s := kv.New()
	n, err := raft.New(raft.Config{
		ID: 1, Peers: []raft.NodeID{1, 2, 3}, Clock: clock.System{}, Transport: transport.NewNetwork(clock.System{}, rand.New(rand.NewPCG(1, 1))),
		ElectionTimeoutMin: time.Second, ElectionTimeoutMax: 2 * time.Second, HeartbeatInterval: 100 * time.Millisecond,
		Rand: rand.New(rand.NewPCG(1, 1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Attach(n) // never started: no leader known
	srv := httptest.NewServer((&kv.Server{Store: s, Node: n, LeaderURL: func(raft.NodeID) string { return "" }}).Handler())
	defer srv.Close()
	resp, _ := request(t, "PUT", srv.URL+"/kv/k", "v", map[string]string{"X-Client-ID": "t", "X-Request-ID": "1"})
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("got %d, Retry-After=%q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}
