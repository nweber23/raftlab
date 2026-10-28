package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/transport"
	"github.com/nweber23/raftlab/web"
)

// hub fans JSON frames out to every connected browser. A client that cannot
// keep up loses frames rather than stalling the cluster.
type hub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func (h *hub) subscribe() chan []byte {
	ch := make(chan []byte, 512)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients == nil {
		h.clients = map[chan []byte]struct{}{}
	}
	h.clients[ch] = struct{}{}
	return ch
}

func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, ch)
}

func (h *hub) broadcast(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- b:
		default:
		}
	}
}

// Server is the visualizer's HTTP surface: static UI, WebSocket feed, control
// API, and each node's key-value API under /node/{id}/.
type Server struct {
	lab *Lab
	hub hub
}

// NewServer wires lab's message events into the hub and returns the handler.
// Call Run to start publishing state frames.
func NewServer(lab *Lab) *Server {
	s := &Server{lab: lab}
	lab.net.Observe(func(e transport.Event) { s.hub.broadcast(describe(e)) })
	return s
}

// Run publishes a state frame every interval until ctx ends.
func (s *Server) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.hub.broadcast(s.lab.State())
		}
	}
}

// Handler returns the full HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServerFS(web.FS))
	mux.HandleFunc("GET /ws", s.ws)
	mux.HandleFunc("POST /api/crash", s.nodeAction(s.lab.Crash))
	mux.HandleFunc("POST /api/restart", s.nodeAction(s.lab.Restart))
	mux.HandleFunc("POST /api/partition", s.partition)
	mux.HandleFunc("POST /api/heal", func(w http.ResponseWriter, _ *http.Request) {
		s.lab.Heal()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /api/network", s.network)
	mux.HandleFunc("/node/{id}/", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseUint(r.PathValue("id"), 10, 32)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		http.StripPrefix("/node/"+r.PathValue("id"), s.lab.NodeHandler(raft.NodeID(id))).ServeHTTP(w, r)
	})
	return sameOriginWrites(mux)
}

// sameOriginWrites rejects state-changing requests that a browser marks as
// coming from another origin, so a web page elsewhere cannot make the user's
// browser crash nodes or write keys on localhost. Non-browser clients send no
// Origin header and pass.
func sameOriginWrites(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if o := r.Header.Get("Origin"); o != "" {
				if u, err := url.Parse(o); err != nil || u.Host != r.Host {
					http.Error(w, "cross-origin request refused", http.StatusForbidden)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = c.CloseNow() }()
	ctx := c.CloseRead(r.Context()) // we never read; this just notices the client leaving
	ch := s.hub.subscribe()
	defer s.hub.unsubscribe(ch)

	first, _ := json.Marshal(s.lab.State())
	if c.Write(ctx, websocket.MessageText, first) != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-ch:
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.Write(wctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (s *Server) nodeAction(f func(raft.NodeID) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseUint(r.URL.Query().Get("id"), 10, 32)
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		if err := f(raft.NodeID(id)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) partition(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Groups [][]raft.NodeID `json:"groups"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.lab.Partition(req.Groups)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) network(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MinDelayMs int     `json:"minDelayMs"`
		MaxDelayMs int     `json:"maxDelayMs"`
		DropRate   float64 `json:"dropRate"`
		DupRate    float64 `json:"dupRate"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	const maxDelayMs = 60_000
	if req.MinDelayMs < 0 || req.MaxDelayMs < req.MinDelayMs || req.MaxDelayMs > maxDelayMs || req.DropRate < 0 || req.DropRate > 1 || req.DupRate < 0 || req.DupRate > 1 {
		http.Error(w, "values out of range", http.StatusBadRequest)
		return
	}
	s.lab.SetNetwork(time.Duration(req.MinDelayMs)*time.Millisecond, time.Duration(req.MaxDelayMs)*time.Millisecond, req.DropRate, req.DupRate)
	w.WriteHeader(http.StatusNoContent)
}
