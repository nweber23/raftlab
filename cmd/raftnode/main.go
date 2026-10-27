// Command raftnode runs one member of a raftlab cluster: Raft, a durable
// write-ahead log, and an HTTP server that serves both the key-value API for
// clients and the Raft RPCs for peers.
//
//	raftnode -id 1 -peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 -data data
//
// If $RAFTLAB_TOKEN is set, peers must present it on /raft. It is read from
// the environment only, so it never appears in the process list.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nweber23/raftlab/internal/clock"
	"github.com/nweber23/raftlab/internal/kv"
	"github.com/nweber23/raftlab/internal/raft"
	"github.com/nweber23/raftlab/internal/storage"
	"github.com/nweber23/raftlab/internal/transport"
)

func main() {
	id := flag.Uint("id", 0, "this node's ID (must appear in -peers)")
	peerList := flag.String("peers", "", "comma-separated id=host:port for every node, this one included")
	data := flag.String("data", "data", "directory for the write-ahead log (a subdirectory per node ID)")
	snap := flag.Int("snapshot-threshold", 1000, "compact the log after this many applied entries (0 = never)")
	flag.Parse()

	if err := run(raft.NodeID(*id), *peerList, filepath.Join(*data, fmt.Sprint(*id)), os.Getenv("RAFTLAB_TOKEN"), *snap); err != nil {
		log.Fatal(err)
	}
}

func parsePeers(s string) (map[raft.NodeID]string, error) {
	peers := map[raft.NodeID]string{}
	for _, item := range strings.Split(s, ",") {
		k, addr, ok := strings.Cut(strings.TrimSpace(item), "=")
		n, err := strconv.ParseUint(k, 10, 32)
		if !ok || err != nil || n == 0 || addr == "" {
			return nil, fmt.Errorf("bad peer %q, want id=host:port", item)
		}
		peers[raft.NodeID(n)] = addr
	}
	return peers, nil
}

func run(id raft.NodeID, peerList, dir, token string, snapThreshold int) error {
	addrs, err := parsePeers(peerList)
	if err != nil {
		return err
	}
	self, ok := addrs[id]
	if !ok {
		return errors.New("-id must appear in -peers")
	}
	var ids []raft.NodeID
	urls := map[raft.NodeID]string{}
	others := map[raft.NodeID]string{}
	for p, a := range addrs {
		ids = append(ids, p)
		urls[p] = "http://" + a
		if p != id {
			others[p] = urls[p]
		}
	}

	wal, initial, err := storage.Open(dir, storage.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = wal.Close() }()
	tr := transport.NewHTTP(others, token)
	defer tr.Close()
	store := kv.New()
	node, err := raft.New(raft.Config{
		ID: id, Peers: ids, Initial: initial, Storage: wal, Machine: store,
		Clock: clock.System{}, Transport: tr, SnapshotThreshold: snapThreshold,
		ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Rand:              rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(id))),
	})
	if err != nil {
		return err
	}
	store.Attach(node)

	mux := http.NewServeMux()
	mux.Handle("POST /raft", transport.Handler(id, ids, token, node.Handle))
	api := &kv.Server{Store: store, Node: node, LeaderURL: func(l raft.NodeID) string { return urls[l] }}
	mux.Handle("/", api.Handler())
	srv := &http.Server{Addr: self, Handler: mux}

	node.Start()
	defer node.Stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Printf("node %d listening on %s, data in %s", id, self, dir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	}
}
