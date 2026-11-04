# raftlab

A from-scratch implementation of the [Raft](https://raft.github.io/raft.pdf) consensus algorithm in Go, with a replicated key-value store and a live visualizer where you can kill nodes, cut the network and watch the cluster recover.

- **Complete Raft**: leader election, log replication, conflict repair, the election restriction, persistence, snapshots and `InstallSnapshot`.
- **The extensions production systems add**: PreVote and CheckQuorum, live membership changes by joint consensus, ReadIndex reads that skip the log, chunked snapshot streaming, and pipelined replication with flow control.
- **Checked, not just tested.** A seeded chaos test crashes nodes and cuts the network while clients hammer shared keys, then proves the recorded history linearizable with a checker written for this repo.
- **No Raft libraries.** The core uses only the standard library; the one dependency is a WebSocket library for the visualizer.
- **Deterministic by construction.** The core never touches sockets or the wall clock, so the test suite runs whole clusters on a fake clock, and every chaos run is reproducible from its seed.

## Quick start

```sh
make run          # visualizer on http://localhost:8080
make test         # go test -race ./...
make test-chaos   # 100 seeds of the chaos test
make lint
```

Port 8080 busy? `make run ADDR=127.0.0.1:9000`.

`make run` starts five nodes inside one process. Open the page and try the [demo scenarios](#demo-scenarios).

### A real cluster

`cmd/raftnode` is one cluster member per process. It serves the key-value API and the Raft RPCs on one port.

```sh
P=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
go build -o bin/ ./cmd/...
for i in 1 2 3; do bin/raftnode -id $i -peers $P -data data & done

# Write through any node; followers redirect to the leader (curl -L follows).
curl -L -X PUT -H 'X-Client-ID: me' -H 'X-Request-ID: 1' -d hello 127.0.0.1:7002/kv/greeting
curl -L 127.0.0.1:7003/kv/greeting
curl 127.0.0.1:7001/status
```

| Endpoint | |
|---|---|
| `PUT /kv/{key}` | body is the value; `204` on commit |
| `GET /kv/{key}` | `200` value or `404`; linearizable, served with ReadIndex (no log write) |
| `DELETE /kv/{key}` | `204` |
| `POST /session` | `200`, the body is a new server-issued client ID |
| `POST /keepalive` | `204`; refreshes the session named by `X-Client-ID` |
| `GET /status` | role, term, leader, commit index |

Writes should carry `X-Client-ID` plus a per-client increasing `X-Request-ID`. If a request times out (`504`), retry it with the same pair: it is applied at most once. A request to a follower gets a `307` to the leader, or `503` with `Retry-After` while there is no leader. Requests without a client ID are untracked: no session is kept for them and they are never deduplicated.

**Sessions.** `-session-ttl 5m` forgets clients idle that long, and `-require-sessions` makes the cluster refuse writes from any ID it did not issue (`401`; register again). Together they close the gap where a very late duplicate of an old request runs a second time after its session was forgotten. Expiry is decided from timestamps written into the log, so every replica forgets the same sessions at the same point; send `POST /keepalive` from clients that only read.

```sh
ID=$(curl -s -X POST 127.0.0.1:7001/session)
curl -L -X PUT -H "X-Client-ID: $ID" -H 'X-Request-ID: 1' -d hello 127.0.0.1:7001/kv/greeting
```

### Securing the cluster

Set `RAFTLAB_TOKEN` in every node's environment to require a shared secret on the peer RPC endpoint. For real protection use mutual TLS: `raftcert` writes a throwaway CA and a certificate per node, and `raftnode` then serves HTTPS and accepts a peer message only if the sender's verified certificate is named for the node the message claims to come from.

```sh
bin/raftcert -dir certs -nodes 1,2,3 -hosts 127.0.0.1,localhost
for i in 1 2 3; do bin/raftnode -id $i -peers $P -data data \
  -tls-ca certs/ca.pem -tls-cert certs/node-$i.pem -tls-key certs/node-$i-key.pem & done
curl --cacert certs/ca.pem -L https://127.0.0.1:7002/kv/greeting   # the client API needs no client certificate
```

See the [security notes](docs/design-decisions.md#security-model) for what this does and does not cover.

### Changing the cluster

Membership changes are a core feature (`Node.ChangeMembership`) and drive the visualizer's **Scale** control, which adds and removes servers live. The standalone `raftnode` binary has a fixed address list, so it does not expose them yet (see the roadmap).

## Architecture

```
cmd/raftnode   one node: HTTP KV API + peer RPCs + WAL
cmd/raftlab    five nodes in-process + fault-injecting network + visualizer
internal/raft        the algorithm; knows only the Transport, Clock, Storage and StateMachine interfaces
internal/raft/raftest   fake clock and a deterministic cluster harness for tests
internal/transport   in-memory network with faults, manual in-memory transport, HTTP transport (optional mTLS), JSON codec
internal/storage     write-ahead log (CRC32C records, fsync, torn-write recovery, compaction)
internal/kv          the replicated state machine, sessions and its HTTP API
internal/linz        linearizability checker (Wing-Gong/Lowe) and the key-value model
internal/pki         throwaway CA and node certificates for mutual TLS
internal/clock       the wall-clock implementation of raft.Clock
cmd/raftcert         writes the certificates for a cluster
web/                 visualizer (embedded with go:embed)
```

Details are in [docs/architecture.md](docs/architecture.md); the reasoning behind the choices is in [docs/design-decisions.md](docs/design-decisions.md).

## Demo scenarios

1. **Leader failure.** Press *Kill* on the node marked ♛. Within about 300 ms a follower times out, wins a higher term and becomes leader. *Restart* the old leader: it comes back as a follower, replaying its log from disk.
2. **Redirect.** Leave the target on "any node" and press *Set*. The client log shows the request redirected from a follower to the leader. The entry appears in every log, and the commit marker moves once a majority has it.
3. **Split brain that isn't.** Press *Isolate leader*, then write a key straight at it. The old leader cannot commit; after about one election timeout CheckQuorum makes it notice it has lost its majority and step down, while the other side elects a new leader and commits. *Heal*: the old node rejoins as a follower and its uncommitted entries are overwritten.
4. **No quorum.** Tick three nodes and press *Partition selected*. The two-node side cannot elect a leader and writes sent there time out, but its terms stay put: those nodes only send *pre-votes* (grey), which change nothing, so when you *Heal* they cannot depose the three-node side's leader. The three-node side carries on.
5. **Bad network.** Raise latency, drop and duplicate rates. Elections get slower and dropped packets (red) show up, but the logs still converge.
6. **Snapshots.** Write about 40 keys. Nodes compact their logs (grey *snap* cell). Kill a node, write more, restart it: a far-behind follower is caught up with `InstallSnapshot` (purple packets, streamed in chunks).
7. **Scale live.** Change *Cluster size* and press *Scale*. New servers join empty and are filled in by the leader; shrinking removes the highest IDs. Data and leadership survive, including when the leader itself is removed.

## Testing

`go test -race ./...` runs everything: election (PreVote, CheckQuorum), replication (conflict repair, the election restriction, prior-term commitment, pipelining and its recovery from loss), membership changes, ReadIndex, persistence and recovery, torn writes, chunked snapshots, the KV layer and its sessions, mutual TLS, the HTTP APIs and the visualizer backend.

The chaos test (`internal/kv/chaos_test.go`) runs a 5-node cluster on a fake clock for 40 virtual seconds while it randomly crashes and restarts nodes, partitions the network and injects loss, duplication and delay. Four clients issue sets, deletes and reads (half through the log, half through ReadIndex) against two shared keys throughout. It checks election safety and committed-prefix agreement on every 10 ms step; then it heals everything, verifies that replicas converge, and feeds the complete recorded history to the linearizability checker in `internal/linz`. Odd seeds run without PreVote and CheckQuorum so that a deposed leader lingers and stale reads are possible.

That checker is what gives the other tests teeth. Breaking ReadIndex so it skips the leadership confirmation, or removing request deduplication, makes seeds within the first 40 fail with the offending operation named; the correct code passes all 100.

```sh
CHAOS_SEEDS=200 make test-chaos            # many seeds
CHAOS_SEED=17 go test -race -run Chaos -v ./internal/kv   # replay one seed
```

A failure message always begins with `seed=N`.

## Roadmap

Everything from the original roadmap is done; see [rundown.md](rundown.md) for what was built and what it showed. What is left:

- Membership changes in `raftnode`: configuration entries would need to carry addresses, and the HTTP transport would learn peers at runtime
- A non-voting catch-up phase for new servers, so adding one that is far behind cannot stall commits
- Telling removed servers they are gone (today they are only kept out by PreVote and the leader lease)
- Versioned WAL records, so a format change does not strand an old data directory
- Leader leases as a cheaper alternative to ReadIndex, if a trustworthy clock bound is available

## License

MIT
