package main

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/nweber23/raftlab/internal/pki"
	"github.com/nweber23/raftlab/internal/raft"
)

func TestTLSFilesFromRaftcertWork(t *testing.T) {
	dir := t.TempDir()
	if err := pki.WriteCluster(dir, []raft.NodeID{1, 2}, []string{"127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	if err := pki.WriteCluster(dir, []raft.NodeID{1}, nil); err == nil {
		t.Fatal("existing certificates were overwritten")
	}
	files := func(id string) tlsFiles {
		return tlsFiles{ca: filepath.Join(dir, "ca.pem"), cert: filepath.Join(dir, "node-"+id+".pem"), key: filepath.Join(dir, "node-"+id+"-key.pem")}
	}
	serverCfg, _, err := files("1").configs()
	if err != nil {
		t.Fatal(err)
	}
	_, clientCfg, err := files("2").configs()
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || r.TLS.VerifiedChains[0][0].Subject.CommonName != pki.NodeName(2) {
			http.Error(w, "no verified peer", http.StatusUnauthorized)
		}
	}))
	srv.TLS = serverCfg
	srv.StartTLS()
	defer srv.Close()
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: clientCfg}}
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	// A client with no certificate may still connect (the KV API is public)...
	plain := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: clientCfg.RootCAs}}}
	if resp, err := plain.Get(srv.URL); err != nil {
		t.Fatal(err)
	} else {
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized { // ...but is not a verified peer.
			t.Fatalf("anonymous client: %d", resp.StatusCode)
		}
	}
	for _, bad := range []tlsFiles{{ca: "x"}, {ca: "x", cert: "y", key: "z"}, {}} {
		if _, _, err := bad.configs(); err == nil {
			t.Fatalf("%+v accepted", bad)
		}
	}
}
