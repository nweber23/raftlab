package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/nweber23/raftlab/internal/raft"
)

func newTestLab(t *testing.T) *Lab {
	t.Helper()
	lab, err := NewLab(3, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lab.Close)
	return lab
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func leaderOf(lab *Lab, skip raft.NodeID) raft.NodeID {
	for _, n := range lab.State().Nodes {
		if n.Up && n.Role == "leader" && n.ID != skip {
			return n.ID
		}
	}
	return raft.NoNode
}

func TestLabFailoverAndRestart(t *testing.T) {
	lab := newTestLab(t)
	waitFor(t, "a leader", func() bool { return leaderOf(lab, 0) != 0 })
	old := leaderOf(lab, 0)
	if err := lab.Crash(old); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a new leader", func() bool { return leaderOf(lab, old) != 0 })
	if lab.State().Nodes[old-1].Up {
		t.Fatal("crashed node reported up")
	}
	if err := lab.Restart(old); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the restarted node to follow", func() bool {
		n := lab.State().Nodes[old-1]
		return n.Up && n.Role == "follower" && n.Leader != 0
	})
	if lab.Crash(99) == nil || lab.Restart(99) == nil {
		t.Fatal("unknown node accepted")
	}
}

func TestServerAPIAndKVRedirect(t *testing.T) {
	lab := newTestLab(t)
	srv := httptest.NewServer(NewServer(lab).Handler())
	defer srv.Close()
	waitFor(t, "a leader everyone knows", func() bool {
		for _, n := range lab.State().Nodes {
			if n.Leader == 0 {
				return false
			}
		}
		return true
	})
	l := leaderOf(lab, 0)
	follower := l%3 + 1

	req, _ := http.NewRequest("PUT", srv.URL+"/node/"+string(rune('0'+follower))+"/kv/k", strings.NewReader("v"))
	req.Header.Set("X-Client-ID", "t")
	req.Header.Set("X-Request-ID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || !strings.Contains(resp.Request.URL.Path, "/node/"+string(rune('0'+l))+"/") {
		t.Fatalf("status %d, final url %s; want 204 at leader %d", resp.StatusCode, resp.Request.URL, l)
	}
	waitFor(t, "the write to replicate", func() bool {
		for _, n := range lab.State().Nodes {
			if n.KV["k"] != "v" {
				return false
			}
		}
		return true
	})

	post := func(path, body string) int {
		r, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		return r.StatusCode
	}
	if c := post("/api/crash?id=2", ""); c != 204 {
		t.Fatalf("crash: %d", c)
	}
	if lab.State().Nodes[1].Up {
		t.Fatal("node 2 still up")
	}
	if c := post("/api/restart?id=2", ""); c != 204 {
		t.Fatalf("restart: %d", c)
	}
	if c := post("/api/partition", `{"groups":[[1]]}`); c != 204 || lab.State().Nodes[0].Group == 0 {
		t.Fatalf("partition: %d, group %d", c, lab.State().Nodes[0].Group)
	}
	if c := post("/api/heal", ""); c != 204 || lab.State().Nodes[0].Group != 0 {
		t.Fatalf("heal: %d", c)
	}
	if c := post("/api/network", `{"minDelayMs":10,"maxDelayMs":30,"dropRate":0.1,"dupRate":0.2}`); c != 204 {
		t.Fatalf("network: %d", c)
	}
	if s := lab.State(); s.DelayLo != 10 || s.DelayHi != 30 || s.Drop != 0.1 || s.Dup != 0.2 {
		t.Fatalf("network state %+v", s)
	}
	for _, bad := range []string{`{"minDelayMs":50,"maxDelayMs":10}`, `{"dropRate":2}`, `nope`} {
		if c := post("/api/network", bad); c != 400 {
			t.Errorf("network %s: %d", bad, c)
		}
	}
	if c := post("/api/crash?id=abc", ""); c != 400 {
		t.Errorf("bad id: %d", c)
	}
}

func TestWebSocketStreamsStateAndMessages(t *testing.T) {
	lab := newTestLab(t)
	s := NewServer(lab)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go s.Run(ctx, 50*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.CloseNow() }()

	var gotState, gotMsg bool
	for !gotState || !gotMsg {
		_, b, err := c.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v (state=%v msg=%v)", err, gotState, gotMsg)
		}
		var m struct {
			Type  string
			Nodes []NodeState
			Kind  string
		}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		switch m.Type {
		case "state":
			gotState = len(m.Nodes) == 3
		case "msg":
			gotMsg = m.Kind != ""
		}
	}
}

func TestStaticFilesServed(t *testing.T) {
	srv := httptest.NewServer(NewServer(newTestLab(t)).Handler())
	defer srv.Close()
	for _, p := range []string{"/", "/app.js", "/style.css"} {
		r, err := http.Get(srv.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if r.StatusCode != 200 || len(b) == 0 {
			t.Errorf("%s: %d, %d bytes", p, r.StatusCode, len(b))
		}
	}
}

func TestCrossOriginWritesRefused(t *testing.T) {
	lab := newTestLab(t)
	srv := httptest.NewServer(NewServer(lab).Handler())
	defer srv.Close()
	do := func(origin string) int {
		req, _ := http.NewRequest("POST", srv.URL+"/api/crash?id=1", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Body.Close()
		return r.StatusCode
	}
	if c := do("https://evil.example"); c != http.StatusForbidden {
		t.Fatalf("cross-origin: %d", c)
	}
	if !lab.State().Nodes[0].Up {
		t.Fatal("cross-origin request crashed a node")
	}
	if c := do(srv.URL); c != http.StatusNoContent {
		t.Fatalf("same-origin: %d", c)
	}
	r, _ := http.Post(srv.URL+"/api/network", "application/json", strings.NewReader(`{"minDelayMs":0,"maxDelayMs":9223372036854}`))
	_ = r.Body.Close()
	if r.StatusCode != http.StatusBadRequest {
		t.Fatalf("huge delay accepted: %d", r.StatusCode)
	}
}
