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
//	POST   /session    register: the body of the reply is a new client ID
//	POST   /keepalive  refresh the session named by X-Client-ID
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
	mux.HandleFunc("POST /session", s.serveKV(OpRegister))
	mux.HandleFunc("POST /keepalive", s.serveKV(OpKeepAlive))
	mux.HandleFunc("GET /status", s.status)
	return mux
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	st := s.Node.Status()
	st.Log = nil
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"id": st.ID, "role": st.Role.String(), "term": st.Term,
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
		if kind == OpRegister || kind == OpGet || (op.Client == "" && !s.Store.RequireSessions) {
			// untracked request: no session, no dedup across retries. The waiter
			// still needs a unique key.
			op.Anon, op.Client, op.Seq = true, fmt.Sprintf("anon-%x", rand.Uint64()), 1
		} else if op.Client == "" {
			http.Error(w, "this cluster requires a session: POST /session, then send X-Client-ID", http.StatusUnauthorized)
			return
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

		var p *Pending
		var err error
		if kind == OpGet {
			p, err = s.Store.Read(op.Key)
		} else {
			p, err = s.Store.Submit(op)
		}
		if errors.Is(err, ErrNotLeader) || errors.Is(err, raft.ErrNotLeader) {
			s.redirect(w, r)
			return
		}
		if errors.Is(err, raft.ErrNotReady) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
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
		if p.Err() != nil { // the read's leader lost its lease; the client can retry
			s.redirect(w, r)
			return
		}
		res := p.Result()
		switch {
		case res.Expired:
			http.Error(w, res.Err, http.StatusUnauthorized)
		case res.Err != "":
			http.Error(w, res.Err, http.StatusConflict)
		case kind == OpRegister:
			_, _ = io.WriteString(w, res.Value)
		case kind == OpGet && !res.Found:
			http.Error(w, "not found", http.StatusNotFound)
		case kind == OpGet:
			_, _ = io.WriteString(w, res.Value)
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
