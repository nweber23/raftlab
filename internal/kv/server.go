package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
)

// Server exposes a Store over HTTP:
//
//	PUT    /kv/{key}   body = value
//	GET    /kv/{key}
//	DELETE /kv/{key}
//	GET    /status
//
// Writes carry X-Client-ID and X-Request-ID (a per-client increasing number);
// a retry with the same pair is applied once. Requests to a follower get a
// 307 redirect to the leader, or 503 if there is none.
type Server struct {
	Store *Store
	Node  *raft.Node
	// LeaderURL maps a node ID to the base URL clients should use for it;
	// "" means unknown.
	LeaderURL func(raft.NodeID) string
	// Timeout bounds how long a request waits for its commit.
	Timeout time.Duration
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.serveKV(OpGet))
	mux.HandleFunc("PUT /kv/{key}", s.serveKV(OpSet))
	mux.HandleFunc("DELETE /kv/{key}", s.serveKV(OpDelete))
	mux.HandleFunc("GET /status", s.status)
	return mux
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	st := s.Node.Status()
	st.Log = nil
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"id": st.ID, "role": st.Role.String(), "term": st.Term,
		"leader": st.Leader, "commitIndex": st.CommitIndex, "lastApplied": st.LastApplied, "lastIndex": st.LastIndex})
}

func (s *Server) serveKV(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		op := Op{Kind: kind, Key: r.PathValue("key"), Client: r.Header.Get("X-Client-ID")}
		if kind == OpSet {
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
			if err != nil {
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			op.Value = string(body)
		}
		if op.Client == "" { // untracked request: unique ID, so no dedup across retries
			op.Client, op.Seq = fmt.Sprintf("anon-%x", rand.Uint64()), 1
		} else if h := r.Header.Get("X-Request-ID"); h != "" {
			seq, err := strconv.ParseUint(h, 10, 64)
			if err != nil {
				http.Error(w, "bad X-Request-ID", http.StatusBadRequest)
				return
			}
			op.Seq = seq
		} else {
			http.Error(w, "X-Client-ID needs X-Request-ID", http.StatusBadRequest)
			return
		}

		p, err := s.Store.Submit(op)
		if errors.Is(err, ErrNotLeader) {
			s.redirect(w, r)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		wait := s.Timeout
		if wait == 0 {
			wait = 3 * time.Second
		}
		timeout := time.NewTimer(wait)
		defer timeout.Stop()
		select {
		case <-p.Done():
		case <-timeout.C:
			http.Error(w, "timed out waiting for commit; retry with the same request ID", http.StatusGatewayTimeout)
			return
		case <-r.Context().Done():
			return
		}
		res := p.Result()
		switch {
		case res.Err != "":
			http.Error(w, res.Err, http.StatusConflict)
		case kind == OpGet && !res.Found:
			http.Error(w, "not found", http.StatusNotFound)
		case kind == OpGet:
			io.WriteString(w, res.Value)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

func (s *Server) redirect(w http.ResponseWriter, r *http.Request) {
	st := s.Node.Status()
	l := st.Leader
	if base := s.LeaderURL(l); l != raft.NoNode && l != st.ID && base != "" {
		w.Header().Set("X-Raft-Leader", strconv.Itoa(int(l)))
		http.Redirect(w, r, base+r.URL.Path, http.StatusTemporaryRedirect)
		return
	}
	w.Header().Set("Retry-After", "1")
	http.Error(w, "no leader known", http.StatusServiceUnavailable)
}
