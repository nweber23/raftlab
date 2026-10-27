package transport_test

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/transport"
)

func TestCodecRoundTrip(t *testing.T) {
	for _, body := range []any{
		raft.RequestVoteArgs{Term: 3, CandidateID: 2, LastLogIndex: 9, LastLogTerm: 2},
		raft.RequestVoteReply{Term: 3, VoteGranted: true},
		raft.AppendEntriesArgs{Term: 4, LeaderID: 1, PrevLogIndex: 5, PrevLogTerm: 3, LeaderCommit: 4,
			Entries: []raft.Entry{{Term: 4, Command: []byte("x\x00y")}}},
		raft.AppendEntriesReply{Term: 4, Success: true, MatchIndex: 6},
		raft.InstallSnapshotArgs{Term: 5, LeaderID: 1, LastIncludedIndex: 7, LastIncludedTerm: 4, Data: []byte("img")},
		raft.InstallSnapshotReply{Term: 5, MatchIndex: 7},
	} {
		in := raft.Message{From: 1, To: 2, Body: body}
		b, err := transport.Encode(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := transport.Decode(b)
		if err != nil || !reflect.DeepEqual(in, out) {
			t.Fatalf("%T: got %+v, err %v", body, out, err)
		}
	}
	if _, err := transport.Decode([]byte(`{"kind":"Nope","body":{}}`)); err == nil {
		t.Fatal("unknown kind accepted")
	}
}

func TestHTTPDeliversBetweenNodes(t *testing.T) {
	var mu sync.Mutex
	var got []raft.Message
	srv := httptest.NewServer(transport.Handler(func(m raft.Message) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, m)
	}))
	defer srv.Close()
	h := transport.NewHTTP(map[raft.NodeID]string{2: srv.URL})
	defer h.Close()
	for i := 0; i < 5; i++ {
		h.Send(raft.Message{From: 1, To: 2, Body: raft.AppendEntriesArgs{Term: uint64(i)}})
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 5 {
		t.Fatalf("delivered %d of 5", len(got))
	}
	for i, m := range got {
		if m.Term() != uint64(i) {
			t.Fatalf("out of order: %d has term %d", i, m.Term())
		}
	}
}

func TestHTTPSendToDeadPeerNeverBlocks(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nobody listens any more
	h := transport.NewHTTP(map[raft.NodeID]string{2: url})
	defer h.Close()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 5000; i++ {
			h.Send(raft.Message{From: 1, To: 2, Body: raft.AppendEntriesArgs{}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on a dead peer")
	}
}

func TestHandlerRejectsGarbage(t *testing.T) {
	rec := httptest.NewRecorder()
	transport.Handler(func(raft.Message) { t.Fatal("delivered garbage") }).
		ServeHTTP(rec, httptest.NewRequest("POST", "/raft", nopBody("not json")))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rec.Code)
	}
}

func nopBody(s string) *strings.Reader { return strings.NewReader(s) }
