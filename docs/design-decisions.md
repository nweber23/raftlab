# Design decisions

**Standard library only in the core.** Raft is small enough to read in one sitting, and a dependency would hide the interesting parts. The only third-party module is a WebSocket library for the visualizer, because the standard library has no WebSocket server.

**Time and I/O behind interfaces.** This is what makes deterministic tests possible: a fake clock plus an in-process network turns a distributed system into a single-threaded program whose runs are reproducible from a seed.

**Timers are replaced, not reset, and carry a generation.** A real clock can run a callback that fired an instant before `Stop`. Without the guard such a callback starts a spurious election, which bumps the term and deposes a healthy leader. The fake clock cannot reproduce this, so a test using a hand-driven clock covers it.

**Reads use ReadIndex, not leases.** A read costs one heartbeat round and no log write, and its safety does not depend on clocks. Lease reads would save that round, but they are only correct if clock drift is bounded, and they must be paired with a rule that followers refuse to vote while the lease may be live. That trade is not worth it here; the interface (`Node.ReadIndex`) would not change if it were.

**State machine is called inside the node's critical section.** It keeps apply order and snapshot points trivially consistent. The price: `Apply` must be fast and must not call the node. `kv.Store` never does; it only takes its own lock, and `Submit` never holds that lock while calling `Propose`, so the two lock orders cannot invert.

**The core owns compaction.** It snapshots at `lastApplied` when the threshold is reached, inside the same critical section, so the image always matches that index. The alternative (the application calls `Snapshot(index, data)`) needs extra coordination for no gain here.

**Snapshots stream in chunks, one per round trip.** The leader holds a byte cursor per follower, and the follower's reply says which offset it wants next, so loss, duplication and reordering need no timers of their own: a heartbeat resends the current chunk. One chunk per round trip is slow for a huge image over a long link; windowing chunks the way entries are windowed would fix that and has not been needed. A duplicate of chunk 0 restarts the transfer rather than being recognised, which is wasteful but cannot corrupt the image, and chunks are keyed by the leader's term because two leaders may serialize the same state differently.

**PreVote and CheckQuorum are on together.** Each fixes half of the same problem: PreVote stops a node that cannot win from inflating terms, and CheckQuorum (with the rule that nodes who recently heard from a leader refuse to vote) stops a leader that cannot reach a majority from lingering. Leader stickiness is based on "has my own election timer fired since the last leader message", not on timestamps, so the core still needs no clock beyond `AfterFunc`.

**Joint consensus, not single-server changes.** Single-server changes are simpler but have a known safety bug when a leader changes mid-change, which is fixed by an extra rule (commit in your own term first). Joint consensus is the original, general mechanism and arbitrary replacements (3 servers for 3 new ones) need no sequencing. The cost is that every quorum decision goes through `Membership.Quorum`, which is a single small function. A new server counts towards quorum immediately; a non-voting catch-up phase would avoid stalls when adding one far behind, and is on the roadmap.

**Removed servers are kept quiet by the protocol, not told.** A server that never learns it was removed keeps its old configuration and may time out. PreVote makes that harmless while a leader is alive (its probes are refused), and a server that is not in its own configuration never campaigns. It is not notified because the leader stops replicating to servers outside `C(new)` the moment it is appended.

**Optimistic pipelining bounded by outstanding entries, not message counts.** The window is `MaxInflight` batches measured as `nextIndex-1 - matchIndex`, which needs no counters to keep in step with replies (heartbeat replies and duplicates would corrupt a counter). Loss is detected by lack of progress over two heartbeats rather than by timers per message. The price is that on a link whose round trip exceeds two heartbeats the leader resends the window every other tick; that is the old behaviour, no worse.

**Time inside the state machine is a number from the log.** Session expiry must give every replica the same answer, so it never reads a clock: `Submit` stamps each op, and the state machine's "now" is the largest stamp applied. The consequences are deliberate: time only advances when something is written (a cluster nobody writes to expires nothing, which is harmless), and a proposer with a badly wrong clock can age sessions early or late for everyone, which is why `-session-ttl` should be long compared to expected clock skew.

**Anonymous requests have no session.** They used to be given a throwaway client ID, which left one session per request in the table forever. They are now flagged `Anon` and skip session handling entirely; they were never deduplicated anyway.

**Linearizability is checked per key.** Keys are independent registers, and linearizability is compositional (a history is linearizable iff each object's sub-history is), so splitting is exact, not an approximation. The remaining search is exponential in concurrency, which is tame for a handful of clients and bounded by a step budget.

**Client sessions for exactly-once writes.** Raft thesis §6.3: each client has an ID and a sequence number, the state machine remembers the last result per client. A waiter keyed by `(client, seq)` makes retries across leader changes safe. Session entries are never expired (a known ceiling, marked in the code).

**Faster log repair.** `AppendEntries` failures return a conflict term and index so the leader skips back a whole term per round trip. Replies also carry `MatchIndex`, so stale and duplicated replies cannot move `nextIndex` backwards.

**Truncate only on a real conflict.** A delayed `AppendEntries` must not erase entries a newer one delivered (a classic subtle bug). There is a test for it.

**WAL: one file, checksummed, rewrite to compact.** A crash can tear only the tail, so replay stops at the first bad record and truncates. Corruption in the middle of the file is indistinguishable from a torn tail and is treated the same way; a record that verifies but is logically impossible is a hard error. Compaction writes a new file and renames it, so a crash leaves the old or the new file, never a mix.

**Per-peer queues over HTTP.** Each peer has a bounded queue and one sender; a dead peer cannot block the node, and drops are fine because Raft retries.

## Security model

raftlab is a teaching and testing tool. What is and is not protected:

- **Peer RPCs** (`POST /raft`): the handler rejects messages not addressed to the node or from an unknown sender (including itself), and, if `RAFTLAB_TOKEN` is set, requires it as a bearer token. The token comes from the environment only, so it is not visible in the process list. There is no TLS: without it the token and all traffic are readable and replayable by anyone on the network path. Run nodes on a trusted network or behind a TLS-terminating proxy. See the next bullet for mTLS.
- **Peer RPCs with mTLS** (`-tls-ca/-tls-cert/-tls-key`): the connection is encrypted and both ends are authenticated against one CA; a message is accepted only if the sender's verified certificate is named for the node the message claims to be from, so a compromised node can speak as itself but not as another. Not covered: revocation and rotation (certificates last a year, there is no CRL), and a node holding a valid certificate is trusted fully, since Raft has no defense against a Byzantine member. `raftcert` discards the CA key after issuing.
- **Client API**: unauthenticated and, without `-tls-*`, in clear text. With `-require-sessions` client IDs are issued by the cluster rather than chosen, which stops a client from squatting on a guessed ID, but an ID is a bearer token: anyone who sees it can use it. Real deployments should authenticate clients and bind sessions to identities.
- **Visualizer**: its control API (kill nodes, change the network) is unauthenticated by design. It binds to loopback by default and warns otherwise; state-changing requests carrying a foreign `Origin` are refused, and on a loopback bind the `Host` header must be a loopback name (against DNS rebinding), so a web page elsewhere cannot drive it through your browser.
