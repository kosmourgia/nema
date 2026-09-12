# Nema progress

Last updated: 12 September 2026

## Current boundary

The v0.0.0.1 seed now has runnable evidence through milestones A-E: exact
bidirectional capture, durable interactions and views, a live custom-tool
round trip, native persisted and ephemeral historical forks, explicit
selection and parent delivery, Flix multi-resumption recipes coupled to the
fixture controller, replay, hooks, Unix IPC, and a bounded rendezvous.

This is a narrow working seed, not a general agent framework. Python currently
owns OS processes, SQLite, and JSONL/Unix-socket adapters. Flix owns executable
effect, CSP, reducer, associated-type/effect, and Datalog experiments. Raw
operational observations remain distinct from both typed reductions and
semantic projections.

Obsolete Mac/Lima bridge notes, placeholders, and unrelated starter debris
were removed after the Arch remote project became canonical.

## Observed environment

| Component | Observed version | Status |
|---|---:|---|
| Linux | 7.2.4-1-aarch64-ARCH (`aarch64`) | canonical runtime |
| Flix | 0.75.3 (`flix` AUR package 0.75.3-1) | installed; check/tests pass |
| Java | OpenJDK 26.0.2.1 | default runtime; check/tests/labs pass |
| Codex | 0.154.0 | installed; logged in; schemas and live runs captured |
| SQLite | 3.53.4 | raw and domain journals replay deterministically |
| Python | 3.14.7 | OS/SQLite adapter and non-JVM clients |

Java 21 is only Flix's compatibility floor. Nema uses the VM's default Java
26 and does not install or pin a second JDK.

## Evidence completed

- Generated and hashed all 426 experimental Codex 0.154.0 schema files.
- Captured a successful real initialize and 140-row experimental feature list
  without a model turn; the checked-in report is sanitized.
- Journaled exact app-server stdin/stdout/stderr bytes online with connection,
  epoch, direction, stream, monotonic time, and sequence. Unknown fields,
  malformed frames, peer role, and JSON ID type survive versioned replay.
- Proved a delayed server-origin request cannot block unrelated observation;
  the router is the sole child-stdin writer.
- Proved a live dynamic `nema_invoke` call can wait while a separate process
  resolves its durable interaction, after which the same Codex turn continues.
  Evidence: interaction `live-tool-a41426ab5278`, 37 raw records, exit 0.
- Completed native fork experiment `live-fork-116e2dbda64e`: persisted and
  ephemeral forks share the exact completed parent boundary and native
  `forkedFromId`; a separate chooser selected one artifact; the exact delivery
  input was persisted before a subsequent parent turn; delivery completed.
  Its successful app-server session retained 428 records and exited 0.
- Stopped Codex and replayed the live experiment from SQLite alone, including
  four branch rows across live and fixture experiments and nine immutable
  artifacts with verified hashes.
- Compiled and ran Flix probes for multiple resumptions, handler ordering,
  escaped later resumption, CSP worker-local effect handling, channels,
  higher-kinded associated types, associated effects, and Datalog `inject`.
  The multi-resumption output is the direct input to the fixture fork controller.
- Added transactional terminal, JSON, and prompt surfaces, plus cursor-based
  catch-up and deliberate stale/retry/conflict dispositions.
- Added a mode-`0600` Unix JSONL daemon and standard-library clients. A stalled
  partial-frame client does not block another control request.
- Captured an actual Codex run's SessionStart, UserPromptSubmit, Stop, and
  SessionEnd payloads through the atomic offline spool, then ingested all four
  exactly with no gaps. Hook installation is additive and idempotent in tests.
- Ran two independent rendezvous client processes to release generation 1.
  Tests also cover timeout, explicit cancellation, generation isolation, and
  a disconnected participant; rendezvous projections rebuild from events.
- `systemd-analyze --user verify ops/nema.service` passes. A transient user
  service survived a forced child failure with a new PID, served IPC again,
  and removed its socket on normal stop.

## Last passing verification

```text
./bin/nema test
  Flix: 14 passed, 0 failed
  Python: 11 passed, 0 failed

./bin/nema demo bidirectional --mode fixture
  10 raw records; malformed 1, notifications 3, requests 3, responses 3

./bin/nema demo fork-compare --mode fixture
  Flix emits two recipes; separate processes select and advance to a bundle

./bin/nema demo live-tool --mode live
  custom tool -> separate process -> accepted answer -> same turn continues

./bin/nema demo fork-compare --mode live
  parent checkpoint -> two native forks -> selection -> explicit parent turn

./bin/nema hooks inspect
  12 configured lifecycle events; 4 exact live observations; 0 gaps/spooled

./bin/nema replay
  raw capture and all current domain projections deterministic
```

## Known limits / next concrete work

- `thread/resume`, mid-turn steering, interrupt, and native history injection
  remain deliberately unclaimed. Parent reintegration uses an attributed next
  turn, not forged history.
- Ephemeral `thread/read` with `includeTurns` failed by the current server
  contract; Nema uses metadata plus retained completion notifications.
  Paginated ephemeral historical fork required `excludeTurns: true`.
- Live branches were read-only in one shared workspace. Conversation ancestry
  is not filesystem isolation; no write-branch/worktree experiment is claimed.
- Restart reconstructs durable protocol/controller state, not a Flix stack or
  a native request from a dead app-server connection. Unknown delivery outcome
  remains an explicit controller state.
- Hook raw bytes may contain private prompts and stay ignored under `.nema/`.
  The 64 MiB fallback ceiling records a hash-and-length gap rather than silently
  claiming unavailable bytes.
- The checked-in user service is not automatically installed or enabled.
- A future stream-native/Aeron-style causal substrate remains compatible with
  the record/projection boundary but is outside v0.
