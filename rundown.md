# Progress rundown: working through the roadmap

*4 November 2026. Branch `roadmap`: seven feature commits plus docs on top of `e0316af`.*

raftlab started this round as a complete but textbook Raft: elections, replication, persistence, snapshots, a replicated key-value store and a live visualizer. Its README ended with a roadmap of seven things real systems add on top. All seven are now built, tested, and documented.

| | Roadmap item | Commit | What it does now | Evidence |
|---|---|---|---|---|
| 1 | PreVote and CheckQuorum | `f92787f` | A partitioned node can no longer depose a healthy leader on rejoin; a leader that loses its majority steps down on its own | A/B test: same scenario, classic protocol is disrupted, PreVote is not |
| 2 | Membership changes (joint consensus) | `449eeee` | Servers are added and removed live, including replacing every server and removing the leader | 7 scenario tests + a 6-seed churn test; a mutation that weakens the quorum rule is caught |
| 3 | ReadIndex reads | `033065b` | `GET` no longer writes to the log; one heartbeat round proves leadership | A stale-read scenario; the mutation that skips the proof returns stale data |
| 4 | Chunked snapshots | `d687ae8` | Snapshots stream in bounded chunks with a per-follower cursor | Loss/duplication/reordering test; a mutation that skips the offset check corrupts the image |
| 5 | Log pipelining and flow control | `4aff09e` | The leader streams entries ahead of acknowledgements within a window | A 1,000-entry burst: **2,012,480 → 2,000 entries on the wire** |
| 6 | Server-issued client IDs, session expiry, mTLS | `d7606c6` | Sessions expire deterministically on every replica; peer RPCs need a certificate named for the sender | Real 3-process cluster over mTLS; impersonation test |
| 7 | Linearizability checker | `f2230fd` | The chaos test now proves its whole history linearizable | Injected bugs are caught and named |

Test functions went from 74 to 116, and Go code from 5.2k to 8.2k lines (3.0k → 4.5k of it non-test). `go test -race ./...` passes, as do 100 chaos seeds (`make test-chaos`, about two minutes).

## 1. PreVote and CheckQuorum

**Problem.** In plain Raft a node cut off from the cluster times out over and over, and each timeout raises its term. When it reconnects, its inflated term forces the healthy leader to step down. A leader that is itself cut off, meanwhile, keeps believing it leads.

**Approach.** A follower whose timer fires first sends a non-binding pre-vote for term+1. Receivers change nothing and grant only if the candidate's log is current *and* they have not recently heard from a leader. Only a majority of grants starts a real election. For the leader, the election timer is re-armed as a window: no majority heard from within it means step down. "Recently heard from a leader" is tracked as "has my own election timer fired since the last leader message", so the core still needs no clock beyond `AfterFunc`.

**Verified.** `TestPreVoteKeepsPartitionedNodeFromDeposingLeader` and `TestWithoutPreVoteRejoiningNodeDisruptsLeader` run the identical scenario both ways; the second one exists so the first cannot pass vacuously.

## 2. Membership changes by joint consensus

**Problem.** The cluster was a fixed list. The visualizer's "Scale" button worked by wiping everything and starting over, with a comment in the code saying why.

**Approach.** The configuration is a log entry, and a node uses the newest one in its log as soon as it is appended. A change goes through `C(old,new)`, under which every decision needs a majority of both sets, and then `C(new)`, which the leader appends by itself; a leader that is not in `C(new)` steps down. Truncating the log can roll a configuration back, so it is recomputed from the log after any truncation or snapshot install. Snapshots and the WAL carry the membership. The visualizer's Scale control now performs a real live change and keeps data and leadership.

**Verified.** Adding a server far behind (needs `InstallSnapshot`), removing followers, removing the leader itself, replacing all three servers, a change that must not commit with only the new half reachable, errors, restart persistence, and a randomized churn test that grows and shrinks the cluster under packet loss and crashes while safety invariants run every 10 ms.

**A weak test, caught.** My first version of the joint-consensus test passed even after I deleted the old-majority requirement from the quorum function, so it proved nothing: the old side could not have committed anyway in that setup. I rebuilt the scenario (leader on the new side, cut off from the old) so that only the old-majority rule stops the commit, and confirmed the mutant now fails it.

## 3. ReadIndex

**Approach.** The leader notes its commit index, stamps a round number on its next `AppendEntries`, waits for a quorum to echo it (proof it was still leader after the read began), waits for its state machine to reach the noted index, and then reads locally. Losing leadership fails every pending read. A brand-new leader refuses until it has committed its first entry. Lease reads were considered and rejected: they trade correctness for a clock-drift assumption (see `docs/design-decisions.md`).

**Verified.** The key test isolates a leader that does not know it has been replaced, lets the other side commit a newer value, and checks that the old leader's read never returns. Replacing the quorum proof with `confirmed = true` makes it return the stale value, and the test fails.

## 4. Chunked InstallSnapshot

**Approach.** The leader keeps a byte cursor per follower. The follower accepts a chunk only at the exact offset it expects and otherwise answers with the offset it wants, so loss, duplication and reordering move the cursor and need no extra timers. A snapshot is installed only when the final chunk arrives. Chunks are keyed by the sending leader's term because two leaders may serialize the same state differently.

**Verified.** Eight seeds with 25% loss, 30% duplication and jitter on 40-byte chunks. Removing the offset check makes the assembled image invalid JSON and the follower panics in `Restore`.

## 5. Pipelining and flow control

**Problem.** Every proposal re-sent everything the follower had not yet acknowledged. A code comment already admitted it ("harmless because followers skip entries they already hold").

**Measured.** 1,000 proposals in a burst, three nodes, 20 ms one-way latency:

| | entries sent on the wire | time to apply everywhere |
|---|---|---|
| before | 2,012,480 | 670 ms |
| stop-and-wait (window 1) | n/a | 670 ms |
| pipelined, window 8 | 2,000 (exactly one copy per follower) | 120 ms |

**Approach.** Each follower is *probed* one message at a time until it first accepts an `AppendEntries`, then *replicated to* optimistically, bounded by `MaxInflight` batches of unacknowledged entries. Heartbeats to a replicating follower are anchored at the last known match so they cannot fail the consistency check and cause a spurious rewind. A follower that makes no progress for two heartbeats is assumed to have lost the messages and is rewound.

**Another weak test, caught.** My first loss test passed with the stall recovery disabled, because random loss triggers leader changes that reset replication state and mask the bug. I added a deterministic one: lose an entire burst for 60 ms (shorter than CheckQuorum's window, so the leader stays) with nothing new proposed; only the stall check can recover, and the test fails without it.

## 6. Sessions with expiry, and mTLS

**Sessions.** `POST /session` returns a client ID built from the log index, unique and identical on every replica. With a TTL, idle sessions are forgotten; with `-require-sessions`, an unknown or expired ID is rejected instead of silently starting a fresh session, which would let a very late duplicate of an old request run twice. Replicas must agree on who expired, so the state machine never reads a clock: each op is stamped with the proposer's time when it is submitted, and "now" inside the store is the largest stamp applied. Side fix: anonymous requests used to leave one session each in the table forever; they are now untracked.

**mTLS.** `raftnode` takes `-tls-ca/-tls-cert/-tls-key`; `raftcert` writes a throwaway CA and one certificate per node. Beyond encryption, the peer handler requires the sender's verified certificate to be named for the node the message *claims* to be from, so a node holding a valid certificate cannot speak as another. The client API stays reachable over plain HTTPS.

**Verified.** Impersonation (valid cert, wrong claimed sender → 403), no certificate (401), a certificate from another CA (handshake fails; mutating the name check makes impersonation succeed and the test fail). Also a real run: `raftcert`, three `raftnode` processes, a write through a follower with a redirect, a read, and a forged peer message with no client certificate refused.

## 7. The linearizability checker

`internal/linz` implements the Wing–Gong/Lowe search with memoization of (placed operations, model state), generic over the sequential model, plus a per-key register model (keys are independent, so splitting the history is exact). Unit tests include known-good and known-bad histories, 200 random histories that are linearizable by construction (all pass), and the same histories with one read corrupted (all fail).

The chaos test was rewritten around it: four clients on two shared keys issue sets, deletes, log-served reads and ReadIndex reads while nodes crash, the network partitions, and traffic is dropped, duplicated and delayed. Every operation is recorded with call and return times, and the full history (1,000–1,700 operations per seed) must linearize.

**Does it catch real bugs?** I injected two, then restored the code:

| Injected bug | Caught by | Report |
|---|---|---|
| ReadIndex serves without confirming leadership | seeds 25 and 39 of 40 | `get "a" -> "client0#156"` cannot be placed: a stale read |
| Session deduplication removed (retries execute twice) | seed 35 of 40 | `delete "a" (found=false)` cannot be placed |

The first one initially slipped through all 40 seeds, which was informative: with CheckQuorum and PreVote on, a deposed leader steps down before a new one can commit anything, so the stale-read window barely exists. The chaos test therefore runs odd seeds in the classic protocol. A checker can only flag what the system does.

## Honest limits

- **`raftnode` cannot change membership.** The core, the test harness and the visualizer can; the standalone binary has a fixed address list, and supporting it needs addresses in the configuration entries and a transport that learns peers at runtime.
- **No catch-up phase.** A new server counts towards quorum the moment `C(old,new)` is appended, so adding one that is far behind can stall commits until it catches up.
- **WAL format changed** (entries have a kind byte; snapshot records carry the membership). Data directories from before this branch are not readable.
- **Session time only advances when something is written**, and a proposer with a badly wrong clock skews expiry for everyone; the TTL should be long compared to clock skew.
- **Snapshot chunks go one per round trip**, which is slow for a very large image over a long link.
- **The checker is exponential in concurrency.** Fine for four clients; it has a step budget and reports "unknown" rather than hanging.
- **Certificates:** no revocation or rotation, and `raftcert` discards the CA key.
- **Lint:** the installed `golangci-lint` could not type-check Go 1.27, so I built v2.15 (the version CI pins) with the project's toolchain and ran it locally: 0 issues, and the formatter reports no diff.

## Reproducing

```sh
make test                                   # everything, with the race detector
make test-chaos                             # 100 seeds, ~2 minutes
CHAOS_SEED=35 go test -race -run Chaos -v ./internal/kv
make run                                    # visualizer; try the new "Scale live" demo
go run ./cmd/raftcert -dir certs            # then see README, "Securing the cluster"
```
