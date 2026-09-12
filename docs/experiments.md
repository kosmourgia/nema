# Experiments

## Flix semantics and fixture fork

`./bin/nema test` compiles and runs the Flix 0.75.3 evidence. The multi-shot
handler resumes one continuation twice into independent immutable branch
recipes. `./bin/nema demo fork-compare --mode fixture` obtains those recipes
from the Flix entry point, persists their artifacts, exposes one shared
selection, and advances a restart-safe controller in separate processes.

The order-sensitive test observes handler-before, resumed body, then
handler-after. An escaped resumption works only when the later invocation site
supplies its remaining effect handler; this is neither JVM portability nor a
serialized continuation.

## Real app-server and bidirectional fixture

`./bin/nema probe app-server` performs a real initialize and feature-list
request without a model turn. The sanitized report has 140 feature rows.

`./bin/nema demo bidirectional --mode fixture` emits malformed input, an unknown
notification with nested future data, and a server-origin request. While that
request's worker waits 250 ms, the unrelated feature response is observed; the
router later sends one explicit disposition. Replay reclassifies all ten blobs.

## Live dynamic interaction

`./bin/nema demo live-tool --mode live` starts an ephemeral real thread with a
dynamic `nema_invoke` tool. The model calls it, the app-server asks Nema to run
it, and Nema publishes durable interaction `live-tool-a41426ab5278`. A distinct
Python process answers `candidate-a`; the waiting native tool receives that
answer and the same turn completes with “The answer is Candidate A.” The
successful session has 37 raw records and exits 0.

## Live native fork, selection, and delivery

`./bin/nema demo fork-compare --mode live` produced experiment
`live-fork-116e2dbda64e` using the installed default model and effort. A
persisted parent completed a checkpoint. Persisted and ephemeral native forks
were created at that exact `lastTurnId`, produced different JSONL parser
designs, and reported the same parent through native `forkedFromId`.

A separate chooser thread used the same dynamic interaction mechanism and
selected the ephemeral output. Nema created an attributed result bundle,
persisted the exact delivery representation before send, delivered it as a new
parent turn, and retained the parent's completion. After Codex exited, replay
rebuilt the experiment and all artifacts without a model request. This is
explicit reintegration, not transcript concatenation or history impersonation.

The first attempt retained useful negative evidence: ephemeral paginated fork
requires `excludeTurns: true`. Ephemeral `thread/read` also rejects
`includeTurns`; Nema derives no hidden history from that limitation.

## Hooks and rendezvous

An actual no-tool Codex run with reviewed one-off trust bypass produced four
project-hook calls. Nema was intentionally offline, so concurrent-safe files
captured exact stdin bytes; `./bin/nema hooks drain` committed all four with
zero gaps. The helper emits no stdout.

With `./bin/nema serve` running, two separately launched
`./bin/nema rendezvous` processes exchanged JSON payloads and released together.
Automated tests additionally verify timeout, explicit cancellation, generation
isolation, disconnected-client cancellation, and replay.
