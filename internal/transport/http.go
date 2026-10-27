package transport

import (
	"bytes"
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
	done   chan struct{}
	wg     sync.WaitGroup
}

var _ raft.Transport = (*HTTP)(nil)

// NewHTTP starts the per-peer workers. peers maps every other node to its
// base URL.
func NewHTTP(peers map[raft.NodeID]string) *HTTP {
	h := &HTTP{
		peers: peers, queues: map[raft.NodeID]chan raft.Message{},
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
			resp, err := h.client.Post(h.peers[id]+"/raft", "application/json", bytes.NewReader(body))
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

// Handler serves POST /raft, passing each decoded message to deliver.
func Handler(deliver func(raft.Message)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
		if err == nil {
			var m raft.Message
			if m, err = Decode(b); err == nil {
				deliver(m)
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
	})
}
