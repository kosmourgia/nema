# Acceptance evidence

Last run on 12 September 2026 in the canonical Arch ARM guest.

| Check | Result |
|---|---|
| `./bin/nema doctor` | Flix 0.75.3, Java 26, Codex 0.154.0, SQLite 3.53.4, Python 3.14.7 |
| `./bin/nema check` | pass |
| `./bin/nema test` | 14 Flix + 11 Python tests pass |
| real initialize/feature-list | pass, 140 features, no model turn |
| delayed bidirectional fixture | pass, 10 exact records |
| live dynamic-tool interaction | pass, separate responder, native continuation |
| fixture fork controller | pass across processes using two Flix recipes |
| native live fork | pass, two modes, chooser, attributed parent delivery |
| actual hook path | pass, 4 exact offline-spooled payloads, 0 gaps |
| independent rendezvous processes | pass, both receive peer payload |
| raw + semantic replay | pass, no mismatches or missing/corrupt artifacts |
| systemd unit/restart/cleanup | pass; forced failure restarted with a new PID and normal stop removed socket |

Covered properties include exact unknown/malformed retention, typed ID and peer
correlation, single-writer routing, slow-wait independence, durable
single-answer/stale/retry/conflict semantics, one-snapshot multi-view rendering,
cursor catch-up, Datalog affordance changes, native fork ancestry separate from
delegation, ephemeral output retention, explicit provenance-preserving parent
delivery, Flix multi-resumption, additive hook merging and concurrent spool
recovery, non-JVM IPC participation, bounded rendezvous failure modes, socket
cleanup, and event/projector replay without model or material execution.

The live evidence is locally inspectable by experiment ID:

- `live-tool-a41426ab5278`
- `live-fork-116e2dbda64e`
- `fixture-parser-1789199022937341854`
- rendezvous `live-seed`, generation `1`

Deliberately unclaimed: thread resume, steering, interrupt, native history
injection, and write-branch filesystem isolation. The chosen branch task was
read-only. Restart reconstructs protocol state rather than reviving a dead
native request or Flix continuation. The user service is shipped and verified
but not installed or enabled automatically.
