package transport

import (
	"bytes"
	"crypto/subtle"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
)

const httpQueue = 256 // messages buffered per peer before new ones are dropped

// HTTP sends Raft messages to peers as POST /raft requests. Each peer has a
// queue and one worker, so a slow or dead peer cannot block the node and
// messages to one peer stay in order. Raft tolerates the drops.
type HTTP struct {
	peers  map[raft.NodeID]string // base URLs, e.g. http://127.0.0.1:7001
	queues map[raft.NodeID]chan raft.Message
	client *http.Client
	token  string
	done   chan struct{}
	wg     sync.WaitGroup
}

var _ raft.Transport = (*HTTP)(nil)

// NewHTTP starts the per-peer workers. peers maps every other node to its
// base URL. A non-empty token is sent as a bearer token with every message.
func NewHTTP(peers map[raft.NodeID]string, token string) *HTTP {
	h := &HTTP{
		token: token, peers: peers, queues: map[raft.NodeID]chan raft.Message{},
		client: &http.Client{Timeout: time.Second}, done: make(chan struct{}),
	}
	for id := range peers {
		q := make(chan raft.Message, httpQueue)
		h.queues[id] = q
		h.wg.Add(1)
		go h.worker(id, q)
	}
	return h
}

// Send implements raft.Transport. It never blocks.
func (h *HTTP) Send(m raft.Message) {
	if q, ok := h.queues[m.To]; ok {
		select {
		case q <- m:
		default: // queue full: drop
		}
	}
}

func (h *HTTP) worker(id raft.NodeID, q chan raft.Message) {
	defer h.wg.Done()
	for {
		select {
		case <-h.done:
			return
		case m := <-q:
			body, err := Encode(m)
			if err != nil {
				continue
			}
			req, err := http.NewRequest(http.MethodPost, h.peers[id]+"/raft", bytes.NewReader(body))
			if err != nil {
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			if h.token != "" {
				req.Header.Set("Authorization", "Bearer "+h.token)
			}
			resp, err := h.client.Do(req)
			if err != nil {
				continue // peer down; the next heartbeat tries again
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}
}

// Close stops the workers.
func (h *HTTP) Close() {
	close(h.done)
	h.wg.Wait()
}

// Handler serves POST /raft for node self, passing each decoded message to
// deliver. It drops messages not addressed to self or not from one of peers.
// If token is non-empty, requests must present it as a bearer token.
//
// This is a shared-secret check, not transport security: without TLS the
// token and messages travel in clear text.
func Handler(self raft.NodeID, peers []raft.NodeID, token string, deliver func(raft.Message)) http.Handler {
	known := make(map[raft.NodeID]bool, len(peers))
	for _, p := range peers {
		known[p] = p != self
	}
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
		if err == nil {
			var m raft.Message
			if m, err = Decode(b); err == nil {
				if m.To != self || !known[m.From] {
					http.Error(w, "unknown sender or wrong recipient", http.StatusForbidden)
					return
				}
				deliver(m)
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
	})
}
