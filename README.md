# raftlab

A from-scratch implementation of the [Raft](https://raft.github.io/raft.pdf) consensus algorithm in Go, with a replicated key-value store and a live visualizer where you can kill nodes, cut the network and watch the cluster recover.

- **Complete Raft**: leader election, log replication, conflict repair, the election restriction, persistence, snapshots and `InstallSnapshot`.
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
| `GET /kv/{key}` | `200` value or `404`; reads go through the log, so they are linearizable |
| `DELETE /kv/{key}` | `204` |
| `GET /status` | role, term, leader, commit index |

Writes should carry `X-Client-ID` plus a per-client increasing `X-Request-ID`. If a request times out (`504`), retry it with the same pair: it is applied at most once. A request to a follower gets a `307` to the leader, or `503` with `Retry-After` while there is no leader.

Set `RAFTLAB_TOKEN` in every node's environment to require a shared secret on the peer RPC endpoint (see [security notes](docs/design-decisions.md#security-model)).

## Architecture

```
cmd/raftnode   one node: HTTP KV API + peer RPCs + WAL
cmd/raftlab    five nodes in-process + fault-injecting network + visualizer
internal/raft        the algorithm; knows only the Transport, Clock, Storage and StateMachine interfaces
internal/raft/raftest   fake clock and a deterministic cluster harness for tests
internal/transport   in-memory network with faults, manual in-memory transport, HTTP transport, JSON codec
internal/storage     write-ahead log (CRC32C records, fsync, torn-write recovery, compaction)
internal/kv          the replicated state machine and its HTTP API
internal/clock       the wall-clock implementation of raft.Clock
web/                 visualizer (embedded with go:embed)
```

Details are in [docs/architecture.md](docs/architecture.md); the reasoning behind the choices is in [docs/design-decisions.md](docs/design-decisions.md).

## Demo scenarios

1. **Leader failure.** Press *Kill* on the node marked ♛. Within about 300 ms a follower times out, wins a higher term and becomes leader. *Restart* the old leader: it comes back as a follower, replaying its log from disk.
2. **Redirect.** Leave the target on "any node" and press *Set*. The client log shows the request redirected from a follower to the leader. The entry appears in every log, and the commit marker moves once a majority has it.
3. **Split brain that isn't.** Press *Isolate leader*, then write a key. The old leader keeps its role (and its stale term) but cannot commit; the majority side elects a new leader and commits. *Heal*: the old leader sees the higher term, steps down, and its uncommitted entries are overwritten.
4. **No quorum.** Tick three nodes and press *Partition selected*. The two-node side cannot elect a leader; writes sent there time out. The three-node side carries on.
5. **Bad network.** Raise latency, drop and duplicate rates. Elections get slower and dropped packets (red) show up, but the logs still converge.
6. **Snapshots.** Write about 40 keys. Nodes compact their logs (grey *snap* cell). Kill a node, write more, restart it: a far-behind follower is caught up with `InstallSnapshot` (purple packets).

## Testing

`go test -race ./...` runs everything: election, replication (including conflict repair, the election restriction and prior-term commitment), persistence and recovery, torn writes, snapshots, the KV layer, the HTTP APIs and the visualizer backend.

The chaos test (`internal/kv/chaos_test.go`) runs a 5-node cluster on a fake clock for 40 virtual seconds while it randomly crashes and restarts nodes, partitions the network and injects loss, duplication and delay, with four clients writing and reading throughout. It checks election safety and committed-prefix agreement on every 10 ms step, then heals everything and verifies that replicas converge, that every acknowledged write survived, and that every read returned the last acknowledged write.

```sh
CHAOS_SEEDS=200 make test-chaos            # many seeds
CHAOS_SEED=17 go test -race -run Chaos -v ./internal/kv   # replay one seed
```

A failure message always begins with `seed=N`.

## Roadmap

- PreVote and CheckQuorum, so a partitioned node cannot disrupt a healthy leader when it rejoins
- Membership changes (joint consensus)
- ReadIndex / lease reads, so reads need not go through the log
- Chunked `InstallSnapshot` and snapshot streaming
- Log pipelining and flow control for AppendEntries
- mTLS between nodes, and server-issued client IDs with session expiry
- Linearizability checker (Porcupine-style) in the chaos test

## License

MIT
