package transport

import (
	"bytes"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
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

// HTTPOption customizes NewHTTP.
type HTTPOption func(*HTTP)

// WithTLS makes the transport dial peers with cfg: set Certificates to present
// this node's identity (for mutual TLS) and RootCAs to verify the peers.
func WithTLS(cfg *tls.Config) HTTPOption {
	return func(h *HTTP) {
		h.client.Transport = &http.Transport{TLSClientConfig: cfg}
	}
}

// NewHTTP starts the per-peer workers. peers maps every other node to its
// base URL. A non-empty token is sent as a bearer token with every message.
func NewHTTP(peers map[raft.NodeID]string, token string, opts ...HTTPOption) *HTTP {
	h := &HTTP{
		token: token, peers: peers, queues: map[raft.NodeID]chan raft.Message{},
		client: &http.Client{Timeout: time.Second}, done: make(chan struct{}),
	}
	for _, o := range opts {
		o(h)
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

// HandlerOption customizes Handler.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	peerName func(raft.NodeID) string
}

// RequirePeerCert makes Handler accept a message only over a TLS connection
// whose client certificate was verified and whose Common Name equals
// name(m.From): a node may only speak as itself. The server's tls.Config must
// request client certificates (tls.VerifyClientCertIfGiven or stricter).
func RequirePeerCert(name func(raft.NodeID) string) HandlerOption {
	return func(c *handlerConfig) { c.peerName = name }
}

// Handler serves POST /raft for node self, passing each decoded message to
// deliver. It drops messages not addressed to self or not from one of peers.
// If token is non-empty, requests must present it as a bearer token.
//
// The token is a shared secret, not transport security: without TLS it and the
// messages travel in clear text. With RequirePeerCert the sender is
// authenticated by its certificate, which is the real defense.
func Handler(self raft.NodeID, peers []raft.NodeID, token string, deliver func(raft.Message), opts ...HandlerOption) http.Handler {
	var cfg handlerConfig
	for _, o := range opts {
		o(&cfg)
	}
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
		var certs []*x509.Certificate
		if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 {
			certs = r.TLS.VerifiedChains[0]
		}
		if cfg.peerName != nil && len(certs) == 0 {
			http.Error(w, "a verified client certificate is required", http.StatusUnauthorized)
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
				if cfg.peerName != nil && certs[0].Subject.CommonName != cfg.peerName(m.From) {
					http.Error(w, "certificate does not belong to the claimed sender", http.StatusForbidden)
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
