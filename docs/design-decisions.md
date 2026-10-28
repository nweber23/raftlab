# Design decisions

**Standard library only in the core.** Raft is small enough to read in one sitting, and a dependency would hide the interesting parts. The only third-party module is a WebSocket library for the visualizer, because the standard library has no WebSocket server.

**Time and I/O behind interfaces.** This is what makes deterministic tests possible: a fake clock plus an in-process network turns a distributed system into a single-threaded program whose runs are reproducible from a seed.

**Timers are replaced, not reset, and carry a generation.** A real clock can run a callback that fired an instant before `Stop`. Without the guard such a callback starts a spurious election, which bumps the term and deposes a healthy leader. The fake clock cannot reproduce this, so a test using a hand-driven clock covers it.

**Reads go through the log.** Simplest linearizable option. The cost is a full replication round per `GET`. ReadIndex or leases are on the roadmap.

**State machine is called inside the node's critical section.** It keeps apply order and snapshot points trivially consistent. The price: `Apply` must be fast and must not call the node. `kv.Store` never does; it only takes its own lock, and `Submit` never holds that lock while calling `Propose`, so the two lock orders cannot invert.

**The core owns compaction.** It snapshots at `lastApplied` when the threshold is reached, inside the same critical section, so the image always matches that index. The alternative (the application calls `Snapshot(index, data)`) needs extra coordination for no gain here.

**Snapshots are one message.** `InstallSnapshot` sends the whole image. Fine for a demo-sized store; chunking is on the roadmap.

**Client sessions for exactly-once writes.** Raft thesis §6.3: each client has an ID and a sequence number, the state machine remembers the last result per client. A waiter keyed by `(client, seq)` makes retries across leader changes safe. Session entries are never expired (a known ceiling, marked in the code).

**Faster log repair.** `AppendEntries` failures return a conflict term and index so the leader skips back a whole term per round trip. Replies also carry `MatchIndex`, so stale and duplicated replies cannot move `nextIndex` backwards.

**Truncate only on a real conflict.** A delayed `AppendEntries` must not erase entries a newer one delivered (a classic subtle bug). There is a test for it.

**WAL: one file, checksummed, rewrite to compact.** A crash can tear only the tail, so replay stops at the first bad record and truncates. Corruption in the middle of the file is indistinguishable from a torn tail and is treated the same way; a record that verifies but is logically impossible is a hard error. Compaction writes a new file and renames it, so a crash leaves the old or the new file, never a mix.

**Per-peer queues over HTTP.** Each peer has a bounded queue and one sender; a dead peer cannot block the node, and drops are fine because Raft retries.

## Security model

raftlab is a teaching and testing tool. What is and is not protected:

- **Peer RPCs** (`POST /raft`): the handler rejects messages not addressed to the node or from an unknown sender (including itself), and, if `RAFTLAB_TOKEN` is set, requires it as a bearer token. The token comes from the environment only, so it is not visible in the process list. There is no TLS: without it the token and all traffic are readable and replayable by anyone on the network path. Run nodes on a trusted network or behind a TLS-terminating proxy. mTLS is on the roadmap.
- **Client API**: unauthenticated. Client IDs are chosen by clients, so a client that knows or guesses another's ID can burn its sequence numbers or read its cached result. Real deployments should authenticate clients and bind IDs to identities, or have the server issue IDs.
- **Visualizer**: its control API (kill nodes, change the network) is unauthenticated by design. It is meant for localhost.
