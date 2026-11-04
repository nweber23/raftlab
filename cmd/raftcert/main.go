// Command raftcert writes a throwaway CA and one certificate per node, for
// running raftnode with mutual TLS:
//
//	raftcert -dir certs -nodes 1,2,3 -hosts 127.0.0.1,localhost
//	raftnode -id 1 -peers ... -tls-ca certs/ca.pem -tls-cert certs/node-1.pem -tls-key certs/node-1-key.pem
//
// The CA key is discarded after issuing; to add a node later, make a new set.
package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/nweber23/raftlab/internal/pki"
	"github.com/nweber23/raftlab/internal/raft"
)

func main() {
	dir := flag.String("dir", "certs", "output directory")
	nodes := flag.String("nodes", "1,2,3", "comma-separated node IDs")
	hosts := flag.String("hosts", "127.0.0.1,localhost", "host names and IPs the certificates are valid for")
	flag.Parse()
	var ids []raft.NodeID
	for _, s := range strings.Split(*nodes, ",") {
		n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 32)
		if err != nil || n == 0 {
			log.Fatalf("bad node ID %q", s)
		}
		ids = append(ids, raft.NodeID(n))
	}
	if err := pki.WriteCluster(*dir, ids, strings.Split(*hosts, ",")); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote ca.pem and %d node certificates to %s\n", len(ids), *dir)
}
