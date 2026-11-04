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

`RequestVote` (also used as the pre-vote probe), `AppendEntries` and `InstallSnapshot`, each with a reply, are plain structs passed by value in a `Message{From, To, Body}`. A reply carries enough to be placed without remembering the request (`MatchIndex`, conflict hints, the snapshot offset the follower wants next), so duplicated and reordered replies are harmless.

## Elections: PreVote and CheckQuorum

A follower whose election timer fires does not bump its term. It sends `RequestVote{PreVote: true}` for term+1; receivers answer without changing term, vote or timers, and grant only if the candidate's log is at least as up to date *and* they have not heard from a leader since their own timer last fired. Only a majority of grants starts the real election. A partitioned node therefore never inflates its term, and rejoining it cannot depose the leader.

CheckQuorum is the leader's half: the election timer, re-armed as a window, makes a leader step down unless a majority of the configuration replied during the last `ElectionTimeoutMin`. Nodes that heard from a leader recently also ignore real `RequestVote`s from higher terms (thesis §4.2.3). Both are `Config` switches, off in the bare core and on in every binary and in the test harness; `raftest.Options.Classic` turns them off to show what they prevent.

## Membership changes

The cluster configuration is an `Entry{Kind: EntryConfig}` in the log, and a node acts on the newest one in its log the moment it is appended, committed or not. A change runs as joint consensus: the leader appends `C(old,new)`, under which every decision (commit, election, pre-vote, CheckQuorum, ReadIndex) needs a majority of *both* sets (`Membership.Quorum`); once that commits it appends `C(new)` itself; a leader that is not in `C(new)` steps down when it commits. One change at a time, and only after the leader has committed something in its own term. Because the configuration is derived from the log, truncating the log can roll it back, so `refreshConfig` recomputes it after any truncation or snapshot install. Snapshots and the WAL carry the membership as of their index.

A joining server boots with no peers and stays passive (it never campaigns) until a leader replicates a configuration containing it. New servers count towards quorum as soon as `C(old,new)` is appended; there is no separate catch-up phase.

## Replication pipeline

The leader keeps, per follower, `nextIndex`, `matchIndex` and a mode. A follower is *probed* until it first accepts an `AppendEntries`: one message at a time, `nextIndex` moving only on acknowledgement, because a reply is the only way to learn where its log really ends. After that it is *replicating*: `nextIndex` advances at send time, and the leader keeps sending while fewer than `MaxInflight` batches (64 entries each) are unacknowledged. A conflict reply drops the follower back to probing. Heartbeats to a replicating follower are empty messages anchored at `matchIndex`, so they can never fail the consistency check and cause a spurious rewind; if a follower makes no progress for two heartbeats while entries are outstanding, the leader assumes loss, rewinds to `matchIndex+1` and probes again.

## Reads without the log

`Node.ReadIndex(fn)` records the leader's commit index, bumps a round counter stamped on outgoing `AppendEntries`, and waits until a quorum of followers has echoed that round (proof it was still leader after the read began) and its own `lastApplied` has reached the recorded index. Then it calls `fn` inside the critical section, where the state machine can be read directly. Losing leadership fails every pending read with `ErrNotLeader`. A fresh leader refuses until it has committed its no-op, since until then its commit index may be behind.

## Log layout

`log[0]` has index `snapIndex+1`. `termAt(snapIndex)` returns `snapTerm`, so the "previous entry" check works at the snapshot boundary. A leader's first entry in a term is an empty no-op so entries from earlier terms can commit (§5.4.2); empty commands are never applied.

## Persistence

Before a node replies to anything that depends on it, it saves `currentTerm`/`votedFor` and any log change via `Storage`. The WAL is one file of records `| len | crc32c | type | payload |`: hard state, append-after-index (which also expresses truncation; each entry carries a kind byte, command or config), and snapshot (which carries the membership as of its index). Open replays the file, stops at the first record that does not verify, and truncates there. Snapshots rewrite the file to a temp file, fsync, rename over the original and fsync the directory. A storage error panics the node; answering from unsaved state could violate safety.

## Snapshots

When `SnapshotThreshold` applied entries have accumulated, the core asks the state machine for an image, saves it with the retained log tail and the membership as of that index, and drops the prefix. A leader whose `nextIndex` for a follower falls inside the compacted prefix streams the image as `InstallSnapshot` chunks of `SnapshotChunkSize` bytes, with a per-follower byte cursor. The follower appends chunks that arrive at exactly the offset it expects, answers duplicates and gaps with the offset it wants next (so the leader's cursor moves back or forward as needed), and restarts if a new leader or snapshot begins at offset 0. Only when the last chunk arrives does it keep any log suffix matching the snapshot's last entry, discard the rest, and restore the state machine. Chunks are tied to the sending leader's term, because two leaders may serialize the same state differently.

## Key-value layer

Writes are serialized as JSON into the log. `kv.Store.Apply` executes them and records the result in a per-client session table `(client, last seq, last result, last used)`. A retry with a seq already applied returns the cached result, so a write is applied at most once even when it lands in the log twice. `Submit` registers a waiter keyed by `(client, seq)` *before* proposing; the waiter is released when that pair is applied, so it does not matter which log index or leader got it there. Sessions are part of the snapshot.

`GET` uses `Store.Read`, which goes through `Node.ReadIndex` and reads the map directly. (Submitting an `OpGet` through the log still works and is also linearizable; the chaos test uses both.)

Sessions can be server-issued: `OpRegister` returns an ID built from its log index, which is unique and identical on every replica. `Submit` stamps each op with the proposer's wall-clock time, and that stamp, never a replica's own clock, is the state machine's only notion of time. A session whose last op is more than `SessionTTL` older than the op being applied is expired, and with `RequireSessions` an op from an unknown or expired ID is rejected without executing. Replicas therefore agree on exactly which requests ran, however their clocks differ. Anonymous requests carry `Anon` and create no session.

## Transport security

`transport.HTTP` can dial peers with a `tls.Config` (`WithTLS`), and `transport.Handler` can require a verified client certificate whose Common Name equals `raftlab-node-<sender id>` (`RequirePeerCert`), so a node can only speak as itself; the server asks every client for a certificate but only `/raft` demands one, which leaves the key-value API usable with plain HTTPS. `internal/pki` and `cmd/raftcert` produce the CA and node certificates.

## Linearizability checking

`internal/linz` implements the Wing & Gong / Lowe algorithm: a depth-first search for an order of the operations that respects real time and satisfies a sequential model, backtracking on dead ends and caching (set of placed operations, model state) pairs already known to fail. It is generic over the model; `linz.CheckKV` splits a key-value history per key (keys are independent registers) and checks each. Operations that touch in time are treated as concurrent, which can only make the checker more permissive. The search is exponential in concurrency, which is fine for four clients and is bounded by a step budget that yields "unknown" instead of hanging.

## Test infrastructure

`raftest.Cluster` wires N nodes to a `FakeClock` and a `transport.Network`, with a WAL per node (fsync off, crashes modeled at process level). Message delivery is a clock timer, so a run is a pure function of the seed: same seed, same elections, same chaos. `Cluster.Run` checks election safety and committed-prefix agreement after every step. `AddNode` boots a joining server for membership tests.

## Visualizer

`cmd/raftlab` creates a `Lab`: N nodes on the real clock, a shared `transport.Network` and WALs in a temp dir. The network reports every send (with its fate: delivered after X ms, dropped, partitioned) to an observer; a hub fans those events out over WebSocket together with a 100 ms state snapshot. The browser animates one dot per message (every packet gets at least 450 ms of visible flight, otherwise zero-latency traffic would be invisible). Control endpoints (`/api/crash`, `/api/restart`, `/api/resize`, `/api/partition`, `/api/heal`, `/api/network`) call straight into the lab; `resize` runs a live membership change and waits for it to commit; the key-value API of each node is mounted at `/node/{id}/` so redirects work in the browser.
