// Command raftlab runs a five-node Raft cluster inside one process and serves
// a live visualizer for it, with controls to crash nodes, cut the network and
// slow it down.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address; the control API is unauthenticated, keep it on loopback")
	size := flag.Int("nodes", 5, "cluster size")
	data := flag.String("data", "", "WAL directory (default: a temp dir removed on exit)")
	flag.Parse()
	if err := run(*addr, *size, *data); err != nil {
		log.Fatal(err)
	}
}

func run(addr string, size int, data string) error {
	if data == "" {
		dir, err := os.MkdirTemp("", "raftlab-")
		if err != nil {
			return err
		}
		defer func() { _ = os.RemoveAll(dir) }()
		data = dir
	}
	lab, err := NewLab(size, data)
	if err != nil {
		return err
	}
	defer lab.Close()
	srv := NewServer(lab)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go srv.Run(ctx, 100*time.Millisecond)

	hs := &http.Server{Addr: addr, Handler: srv.Handler()}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = hs.Shutdown(shutdown)
	}()
	if host, _, err := net.SplitHostPort(addr); err != nil || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
		log.Printf("warning: %s is not loopback; anyone who can reach it can crash nodes and rewrite data", addr)
	}
	log.Printf("raftlab: %d nodes, visualizer on http://%s", size, addr)
	if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
