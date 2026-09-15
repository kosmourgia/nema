# Architecture

Nema keeps observation, interpretation, interaction state, and presentation as
separate layers. The SQLite sequence is ingestion order, not a claim of total
causal or semantic order.

```text
Codex app-server <-> single writer / non-blocking router
                           |
                    exact raw records
                           |
                versioned typed interpretations
                           |
        immutable domain events -> current projections
                           |
             terminal / JSON / prompt surfaces

Codex hooks -> stdout-silent IPC-or-spool capture -> external raw ingress
Non-JVM clients <-> versioned Unix JSONL daemon <-> commands / cursor / rendezvous
```

## Raw protocol record

Every captured line retains exact available bytes plus connection ID, epoch,
local sequence, direction, stream, and monotonic observation time. Requests,
responses, and notifications are roles independent of direction. Correlation
adds initiating peer and the original JSON ID type, so integer `1` and string
`"1"` cannot collide.

`raw_records` is not rewritten when the `capture-v1` interpretation recognizes
a request, response, notification, diagnostic, malformed frame, or unknown
value. A later reducer can coexist under another version.

## Interaction record

Accepted commands and rejected attempts append immutable `domain_events`.
`interactions` is a current projection that replay can independently rebuild
and compare. The interaction's `relevant_input_revision` is stable while its
inputs remain current; it is deliberately not the latest global event number.

One transactional surface snapshot returns a watermark, interactions,
affordances, experiments, and artifacts. A client then asks for events strictly
after that watermark. This closes the snapshot/tail race without holding a
transaction while waiting for a person, model, or process.

## Fork bench

The fixture controller persists a named/versioned control point, source
checkpoint/frontier, branch rows, immutable hashed artifacts, shared selection
ID, result-bundle ID, and delivery status. Fork ancestry and delegation ancestry
are different fields. Advancing after restart reconstructs protocol state from
rows; it never claims to revive a dead continuation or native request.

The live controller separately records native thread IDs, a completed boundary,
source history, persisted and ephemeral fork ancestry, outputs, chooser result,
delivery input, native turn, and observed result. A parent next turn imports an
attributed bundle; it does not rewrite pre-fork history. Fixture persistence
modes remain recipe labels rather than native claims.

## Language boundary

Flix owns the executable semantic experiments: multi-shot effects,
handler ordering, interaction reduction, Datalog affordance derivation, CSP
coordination, and backend type/effect families. Its multi-resumption entry point
produces the actual recipes consumed by the fixture controller. Python is the
current small OS-process, SQLite, JSONL router, and non-JVM client adapter. The
boundary is explicit and replaceable; no in-process JVM object crosses it.

## Vivarium boundary

`Nema.Lab.Vivarium` is a deliberately isolated thought experiment. Its World
is immutable data: locally ordered Pulses drive Beats, live Observer roles are
derived from bindings, systole stages work against the prior closed state,
commit publishes revisions and effect invocations, and diastole dispatches or
resolves them. Measures coordinate independent Pulse frontiers without adding
a global clock. Datalog reads a fact projection of that World and never drives
it.

The result-bearing `Ask` is the one intentional runtime edge. Its Flix handler
returns a live local resumption, while the World retains only a stable
continuation address and its time-relative binding. The specimen does not
serialize JVM continuations, add persistence, or alter the existing fork bench.

## Durability

SQLite uses WAL, foreign keys, `synchronous=FULL`, and a five-second busy
timeout. No transaction is held across external waits. A successful write is
not described as model consumption or material external completion. Unknown
outcomes remain explicit.

Hooks are another raw source, not app-server events in disguise. Their exact
stdin is stored in `external_ingress`; a derived event label only indexes it.
If the daemon is absent, independent atomic spool files avoid a shared append
race. A size ceiling produces an explicit gap record.

Rendezvous groups are generation-scoped domain projections. Each join and
release/cancellation is an immutable event. One Unix request handler may wait,
but the threaded server remains able to accept the peer; timeout and disconnect
cancel the bounded generation rather than leaving an invisible waiter.

An Aeron-style stream-native causal substrate remains a future direction. The
current record/projection split is intentionally compatible with one, but v0
does not depend on it.
