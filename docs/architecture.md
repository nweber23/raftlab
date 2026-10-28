# Architecture

## The core is a pure state machine with four seams

`internal/raft.Node` implements Figure 2 of the Raft paper. It has no goroutines of its own and no knowledge of sockets or wall-clock time. Everything it touches arrives through an interface:

| Interface | Direction | Implementations |
|---|---|---|
| `Transport` (`Send`) | out: RPCs to peers | `transport.Network` (in-process, faults), `transport.Memory` (manual delivery), `transport.HTTP` |
| `Clock` (`AfterFunc`) | in: timeouts | `clock.System`, `raftest.FakeClock` |
| `Storage` | out: durable state | `storage.WAL` |
| `StateMachine` | out: committed commands | `kv.Store`, `raftest.Recorder` |

Input arrives via `Node.Handle(Message)`, timer callbacks and `Node.Propose`. All of them take one mutex; handlers assume it is held. Because `Send` never calls back synchronously and `Apply` runs inside the critical section, there is no lock ordering to get wrong inside the core.

## Messages

`RequestVote`, `AppendEntries` and `InstallSnapshot`, each with a reply, are plain structs passed by value in a `Message{From, To, Body}`. A reply carries enough to be placed without remembering the request (`MatchIndex`, conflict hints), so duplicated and reordered replies are harmless.

## Log layout

`log[0]` has index `snapIndex+1`. `termAt(snapIndex)` returns `snapTerm`, so the "previous entry" check works at the snapshot boundary. A leader's first entry in a term is an empty no-op so entries from earlier terms can commit (§5.4.2); empty commands are never applied.

## Persistence

Before a node replies to anything that depends on it, it saves `currentTerm`/`votedFor` and any log change via `Storage`. The WAL is one file of records `| len | crc32c | type | payload |`: hard state, append-after-index (which also expresses truncation), and snapshot. Open replays the file, stops at the first record that does not verify, and truncates there. Snapshots rewrite the file to a temp file, fsync, rename over the original and fsync the directory. A storage error panics the node; answering from unsaved state could violate safety.

## Snapshots

When `SnapshotThreshold` applied entries have accumulated, the core asks the state machine for an image, saves it with the retained log tail, and drops the prefix. A leader whose `nextIndex` for a follower falls inside the compacted prefix sends `InstallSnapshot`. The follower keeps any log suffix that matches the snapshot's last entry and discards everything else, then restores the state machine.

## Key-value layer

Every operation (including `GET`) is serialized as JSON into the log. `kv.Store.Apply` executes it and records the result in a per-client session table `(client, last seq, last result)`. A retry with a seq already applied returns the cached result, so a write is applied at most once even when it lands in the log twice. `Submit` registers a waiter keyed by `(client, seq)` *before* proposing; the waiter is released when that pair is applied, so it does not matter which log index or leader got it there. Sessions are part of the snapshot.

## Test infrastructure

`raftest.Cluster` wires N nodes to a `FakeClock` and a `transport.Network`, with a WAL per node (fsync off, crashes modeled at process level). Message delivery is a clock timer, so a run is a pure function of the seed: same seed, same elections, same chaos. `Cluster.Run` checks election safety and committed-prefix agreement after every step.

## Visualizer

`cmd/raftlab` creates a `Lab`: N nodes on the real clock, a shared `transport.Network` and WALs in a temp dir. The network reports every send (with its fate: delivered after X ms, dropped, partitioned) to an observer; a hub fans those events out over WebSocket together with a 100 ms state snapshot. The browser animates one dot per message (every packet gets at least 450 ms of visible flight, otherwise zero-latency traffic would be invisible). Control endpoints (`/api/crash`, `/api/restart`, `/api/partition`, `/api/heal`, `/api/network`) call straight into the lab; the key-value API of each node is mounted at `/node/{id}/` so redirects work in the browser.
