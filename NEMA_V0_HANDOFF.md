# Nema v0.0.0.1 — the membrane and the fork bench

Implementation handoff · 12 September 2026 · target: `~/nema` on a dedicated Arch Linux VM

Revised 12 September 2026 after comparison with the six numbered recommendations in the accompanying capstone message. The original already includes their substantive architecture and experiments. This revision makes the handler vocabulary and required durable fork controller explicit. The current remote-project environment below supersedes the earlier Mac/Lima bridge arrangement.

This is an execution brief, not a claim that any code below already exists or compiles. Build the software. The accompanying conversation is conceptual background; the installed compiler, generated harness schemas, and executable tests settle implementation facts. This brief deliberately supersedes earlier expansive lists of foundational concepts and schematic APIs.

## 1. Mission and the first thing to make real

Build a small, observable, Flix-first host for Codex app-server. It must preserve the interactions that pass through it, expose several live views of the same interactions, and make conversation forks usable as experimental objects.

The signature demonstration is:

> Capture a parent conversation at a completed checkpoint; run two independent forks from that checkpoint; retain both runs, including an ephemeral fork when supported; make their outputs available through one shared selection interaction; resolve that interaction from either a human client or another agent; update the other views; carry an explicitly attributed result bundle into a subsequent parent turn; reconstruct the experiment from Nema's own records with Codex stopped.

Alongside that, build a genuine Flix multi-resumption experiment whose results are executable branch recipes. Do not simulate the effect feature with an ordinary loop and declare success.

Nema is not primarily a review/approval framework, a universal ontology, a new sandbox, or a replacement for the native agent loop. It is an instrument for observing and composing agency. Preserve native capabilities rather than gratuitously reimplementing them.

The implementation is permitted to start with one daemon, one SQLite database, one Codex subprocess, a tiny external client protocol, and a terminal inspector. Two concurrent fork threads are enough to establish the important behavior. A sophisticated window manager is not a prerequisite.

## 2. Working commitments

Use Flix for meaningful behavior: effects and their interpreters, routing and concurrency coordination, state reduction, relational derivation, and the branch experiment. Thin Java bridges for process streams, Unix sockets, SQLite/JDBC, or terminal support are acceptable. Do not turn Flix into a decorative wrapper around a TypeScript application.

The VM is intended as Nema's working environment. Prefer ordinary processes, files, Unix sockets, and systemd user services. No containers, Kubernetes, hosted coordination service, authentication product, or distributed deployment is needed. Prove one non-JVM client can participate without assuming in-process JVM objects cross the boundary.

Preserve distinctions before inventing abstractions. Source observations, interpretations, user/agent commands, submitted bytes, acknowledgments, and completed operations are not interchangeable. Keep exact native identities alongside Nema identities.

One observer must not impose its narrative on all the others. A central append sequence is a convenient ingestion order, not a universal causal ordering or canonical interpretation. Views can select different subsets and presentations of the same recorded interactions.

Do not gate progress on a vocabulary freeze. Use functional names in code. `Nema.Membrane`, `Nema.Capture`, `Nema.Interaction`, `Nema.Projection`, `Nema.Fork`, and `Nema.Lab` are fine. `Praxis` can name a reusable application/procedure namespace once one genuinely exists. Reserve the other evocative names—Eidos, Choreia, Dynamis, Organon, Nomos, Antikythera—rather than assigning each a subsystem now.

Maintain a small durable work plan and progress log inside the repository. Inspect existing files and instructions before changing anything; do not assume `~/nema` is empty. Preserve pre-existing work. Implement continuously through the acceptance tests rather than repeatedly stopping for design approval. Ask only for a genuinely indispensable missing credential, privilege, or destructive decision; keep completing independent work when one live capability is blocked.

Do not purchase services, publish private material, erase unrelated data, or alter global configuration merely to make a test pass. Use existing configured accounts and the tool permissions available to this run. Routine project-local development does not need repeated approval ceremonies.

## 3. Compact model: not everything is an effect

Use a small substrate, with concrete protocol-specific types where helpful:

- **Reference / artifact:** stable identity; immutable versions or content-addressed bytes for things that must outlive their current rendering.
- **Interaction:** an identified request, its operation contract, inputs, origin, status, and eventual result. `Pending` is an interaction state, not a mystical new object species.
- **Record:** an occurrence Nema captured, including source and correlation/provenance. Recording an artifact's creation does not make every artifact an event.
- **Binding:** the association between a participant/execution scope and an implementation, process, or native harness thread.
- **Surface:** a versioned observer projection containing representations and currently available operations over the underlying interactions.

Do not create foundational `Finding`, `Evidence`, `Goal`, `Plan`, or `Decision` effects merely because these words appeared in the conversation. A concrete selection protocol can have a `SelectionResult` type. Artifact metadata and application-defined predicates can carry richer domain content later.

Keep the native Codex `Thread`, `Turn`, and `Item` concepts in the Codex adapter. Do not add a second universal Conversation hierarchy unless actual code needs it. A subagent relationship is a relation between participants/bindings, not a duplicate family of `SubagentThread`/`SubagentConversation` classes. Fork ancestry is a different relation from delegation ancestry.

Relations are typed predicates/links with explicit roles. They are not an instruction to implement a generic RDF database or an all-powerful `Relation` effect. A claimed relationship can carry source records, a producer, and a derivation version without being treated as indisputable truth.

Effects express requests whose interpretation varies by scope. Events/records support fan-out. Pure functions build documents and reduce state. Ordinary functions remain ordinary functions when no contextual interpretation is needed.

Use **handler** for the actual algebraic-effect construct; **interpreter** for an implementation of an operation vocabulary, which may contain handlers, ordinary functions, or process adapters; and **renderer** for the production of an observer representation. Avoid using **evaluator** as an interchangeable name for all three. The model may implement a handler's decision procedure; the surrounding runtime owns validation, the accepted interaction transition, and resumption.

## 4. Architectural shape

```text
                       operational / causal path

  Codex app-server <----> transport + router <----> small Flix interpreters
       hooks ------------------+                         |
  other processes / CLI -------+                         |
                               v                         v
                    append-only captured records + immutable artifacts
                                         |
                               deterministic reductions
                                         |
                       finite snapshot + Flix inject / Datalog
                                         |
                          versioned observer surfaces
                           /             |             \
                     terminal       agent adapter     external client
                           \             |             /
                             identified invocations
                                         |
                         route / interpret / execute / record
```

This is the same conceptual membrane at several boundaries, not one gigantic handler. Route responses immediately. Dispatch long-running inbound requests to workers. Keep transport reading and writing independent of any human/model wait. Install needed algebraic handlers inside spawned workers; do not rely on dynamic scope crossing a process/thread boundary.

An interpreter should remove a small responsibility and introduce explicit lower-level ones. For example, a local `ChooseCandidate` effect may be interpreted into an interaction, and awaiting that interaction into a channel wait. It must not also own transport parsing, UI rendering, SQLite schema management, and fork policy.

Treat a running subagent or program as a bidirectional process: command endpoint, observations, and lifecycle. Spawning and waiting are operations; the participant itself is not one long function result pretending to be a process.

## 5. Establish actual capabilities first, with executable probes

Pin Flix, the JDK, and library versions. Record their exact versions and hashes where practical. Use the installed Codex binary as the protocol authority. Run its help/version commands, generate the supported JSON Schema bundle, and preserve the bundle with its version. Verify the initialization handshake and experimental capability opt-ins.

Create `docs/capabilities.md` with each feature marked supported, unsupported, untested, or blocked, with the test/source behind that status. Do not rely on remembered method names, development-branch behavior, or syntactically plausible Flix.

Probe the native protocol for the specific capabilities the build wants: start/resume/read threads, start/interrupt turns, notifications and server-origin requests, fork at a completed boundary, ephemeral fork behavior, dynamic tool configuration, steering constraints, and history injection. Preserve unknown fields and unsupported capability results.

Add small compile/run probes for:

1. Basic effect translation and effect-polymorphic composition.
2. Multiple resumptions producing independent immutable values.
3. Code before and after a resumption, including nesting order.
4. Higher-kinded associated types and associated effects in the chosen compiler.
5. Handling inside spawned workers and cancellation/join behavior.
6. Whether a resumption can escape its handler, be invoked later while relevant scopes remain alive, and what happens under another handler environment.

For probe 6, distinguish compile rejection from runtime/lifetime constraints. Save minimal counterexamples and compiler diagnostics. Do not assert either that resumptions are freely portable or that they cannot escape without testing. Never serialize JVM closures as durable continuation state.

Keep these experiments bounded. If a language feature is absent, implement the main product through explicit interaction state and keep the failed experiment as useful evidence. Generic/parameterized effect declarations are optional, not a bootstrap dependency.

## 6. The protocol membrane

Represent requests, responses, and notifications separately from direction. `Call`/`Request` and `Reply`/`Response` are naming pairs, not client/server directions. Both peers can initiate requests. Notifications have no reply.

A correlation key must preserve connection identity, connection epoch, initiating peer/direction, and the original JSON-RPC ID and its JSON type. The response travels in the opposite direction but resolves that original key. Do not conflate JSON-RPC request IDs with model tool-call IDs or thread/turn/item IDs.

The Codex wire format and supported transports must come from its own schema/docs; do not assume a generic JSON-RPC library's exact envelope is correct. Begin with stdio JSONL to one child app-server. Keep daemon/client IPC distinct from Codex's own Unix-socket or WebSocket transport.

Implement a typed subset plus a retained unknown variant. A future method, extra field, or unrecognized item must be recorded without crashing the whole observation path. Unknown server requests need an explicit disposition appropriate to the peer, not silent disappearance or an invented successful result.

Recommended worker boundaries: stdout reader/framer, stderr reader, writer, correlating router, bounded operation workers, journal/state owner, and surface subscribers. A blocked custom tool must not prevent processing turn termination, cancellation, another thread's messages, or another request's response.

Preserve partial frames and malformed input as diagnostic records. Respect final native item/turn payloads rather than assuming concatenated deltas are authoritative. Keep the deltas as observations even when they differ from a final item.

Handle out-of-order responses, duplicate responses, cancellation racing completion, EOF, process exit, and shutdown. At most one local terminal resolution should win for one single-answer interaction. Do not advertise exactly-once remote effects: a crash after a send can leave completion unknown.

A future transparent relay can bind two independently correlated connections to the same Nema interaction. Do not require a byte-transparent proxy compatible with every native UI for v0. The host-side adapter is the first useful membrane.

## 7. Retention and the boundary between input and ingestion

Use SQLite with a serialized append/transition path; choose and document its durability settings. Store large immutable payloads in content-addressed files if useful, otherwise SQLite blobs are fine initially. Raw retention and typed projections must not become two competing mutable sources of truth.

Capture, from the start:

- Native inbound and outbound frames and stderr, with exact available bytes.
- Nema client commands, materialized outgoing inputs, hook payloads, native replies/notifications, and local operation results.
- Process/session/connection identity, local sequence, receive/send time, source-native timestamps where supplied, and native thread/turn/item/tool IDs.
- Fork origin and boundary, configuration/instruction/skill snapshots or explicit unavailable markers, and the source records used in a rendered context/result bundle.
- Unknown payloads, decoder failures, gaps, and failed sends.

For every submitted object distinguish, where observable:

```text
created -> projected/encoded -> queued -> written -> acknowledged -> observed downstream
```

These are observations, not a promise that every stage can always be known. An acknowledgment does not prove the model consumed the object. A hook can describe processing not visible on app-server; retain it as a separate source and correlate using explicit IDs when possible. A timestamp guess is a labeled heuristic, not an identity match.

Do not deduplicate different sources into one record merely because their text looks the same. Deduplicate projection application through stable keys where needed; retain the source observations. Source/parent links define a partial causal graph. The database sequence is ingestion order.

Hooks need a small stdin capture helper and an idempotent project-local install/merge command. Inspect the actual installed hook contract, retain unknown fields, and preserve pre-existing hooks. Avoid incidental stdout that changes hook behavior. Provide a bounded local spool fallback if the daemon is unavailable; record delayed ingestion and gaps. Concurrent hooks must not corrupt a shared append file. Test at least one actual hook path if supported and available; otherwise label the limitation while testing the helper with fixtures.

Before forking a pre-existing thread, capture the observable source history through the chosen boundary, not only events generated after the fork. Store lineage and inherited-prefix references. If the harness exposes only partial history or summarized items, make that incompleteness explicit.

An ephemeral Codex fork must still have Nema records and artifacts. That is preservation of observed history, not a guarantee that its native hidden state can be reconstructed after app-server exits.

Keep private traces and machine-specific configuration out of Git. Do not sweep credential stores or environment secrets. Redact known authentication material from captured auth exchanges and mark any redaction explicitly; never call a redacted capture byte-identical. Retain ordinary experimental content locally rather than silently discarding it.

Use a durable outbox/intention record before external sends where practical. Record attempted/written/acknowledged states. After ambiguous failures, expose `unknown` rather than blindly retrying a potentially material operation. No database transaction may remain open while waiting for a model, child process, or person.

## 8. State, Datalog, and reactive views

Build pure reducers over the recorded typed observations and accepted local commands. IDs, wall clocks, and other nondeterministic choices are created at the boundary and recorded, not recreated during replay. Recomputing a small state snapshot is sufficient.

Use actual Flix `inject` and Datalog for at least one nontrivial affordance query. The fork experiment offers a good bounded vocabulary:

```text
ForkedFrom(branch, parent, checkpoint)
OutputOf(artifact, branch)
Awaiting(selection, experiment)
Candidate(selection, artifact)
Resolved(selection, answer)
Imported(bundle, parent)
```

Derive which selections are answerable from completed outputs and which follow-up actions exist. A resolution removes the answer operation and enables the next relevant operation. Interpret negation over the intended finite current snapshot, not over an incomplete replica treated as complete. Keep a straightforward pure reference function for equivalence tests.

Rules derive state/eligibility. They must not secretly launch jobs whenever a fixpoint is recomputed. An explicit command and transition starts work.

Produce at least two surfaces from the same interaction state: a terminal view and an agent-facing machine-readable surface. A minimal document representation needs only sections, text, references, and operation descriptions; it is not a new macro compiler. Render the same snapshot into terminal text, JSON, and a prompt fragment where useful.

A surface carries stable interaction IDs and a revision/frontier. Clients subscribe from a known revision or request a current snapshot and continue from its watermark. Test the snapshot/subscription race. Do not let slow repainting block control traffic; slow clients can catch up from retained records. Queue overflow or loss must be explicit.

A stale response is checked against the specific pending interaction and its relevant input revision, not rejected merely because an unrelated global record was appended. Duplicate identical answers can return the existing receipt; conflicting later answers return an already-resolved result.

An agent is not continuously rerendered like a terminal. Record a newly derived desired surface separately from delivery/acknowledgment through the harness. Supply concise state-change descriptions and current action IDs in tool results or a next-turn input; do not rely on the model inferring what happened from a changed tool list alone.

## 9. Tools, pending interactions, and contextual interpretation

Start with a stable small native tool interface, such as `nema_actions` and `nema_invoke`, configured through whatever the installed harness supports. Names are provisional and must satisfy its naming rules. The current actions and their input schemas live in the surface; arguments are validated and decoded to concrete application types at the boundary.

This avoids requiring live tool-schema replacement to make dynamic affordances real. Native per-action tools can be an additional rendering when supported. Preserve all tool calls/results and send the original pending native request exactly one local answer, or the appropriate cancellation/error disposition.

A pending interaction is simply an addressable unanswered request. Its records include request ID, operation/schema version, inputs/references, requester, status, result, and correlation to a native request or local waiter. A wait can be implemented with a channel inside a live handler. Other clients answer via ordinary identified commands.

Local waiting and durable reconstruction are distinct:

- While the runtime is alive, a local continuation can wait for the interaction's answer.
- After restart, records preserve the unanswered question and any later answer. Resume a named/versioned controller step from explicit state where implemented; otherwise report that the original live execution was lost.
- A native request on a dead connection cannot be answered by pretending its JSON-RPC ID belongs to a new connection.

For the fork experiment specifically, implement a small durable controller, not only durable interaction rows. Persist its controller version, named control point, branch/checkpoint references, selection ID, result-bundle reference, and delivery status as applicable. After restart, reconstruct the controller from those explicit records and advance only where the recorded state justifies it. Prove restart while awaiting selection, then accept an answer and continue the controller; use the existing unknown-outcome/reconciliation path for any native execution or delivery whose completion is uncertain. This does not restore a Flix stack or a dead native request. Keep the three accomplishments distinct: a live waiter, a durable question/answer, and a durable controller.

Show one genuinely bidirectional example: a Codex-issued custom tool publishes a pending interaction, a human client answers it, the native tool receives the answer, and the same interaction is updated in the terminal surface. Implement an agent-responder variant using another thread, not the same model invocation that is blocked awaiting its own answer.

Show two interpretations of a small effect: deterministic fixture and live/interaction-backed. Keep concrete result contracts identical. An interpreter that proposes a material operation must not return a receipt claiming that it executed the operation.

One small context/backend trait with associated types/effects is desirable if it removes actual duplication between live and recorded/fixture implementations. Higher-kinded members may represent a view or reference family. Do not force every API into a vast `AgentCtx` type family, and do not add an `F[_]` around every value merely because HKTs exist.

## 10. Fork bench: the principal application

Forks are first-class experiments, not merely a synonym for subagent spawning. Implement the supported forms independently, including ordinary/persisted, completed-historical-boundary, and ephemeral forks. Preserve native lineage and Nema lineage even when they differ in available detail.

Every branch records at least:

```text
branch ID
source native thread + explicit completed boundary
source capture/frontier reference
native fork result and persistence mode
prompt/input and relevant configuration
workspace mode and optional workspace reference
outputs/artifacts and terminal status
parent/delegation links, separately from fork ancestry
```

Reject an unsupported fork mode explicitly. Do not silently turn a history-preserving fork into a fresh thread with a summary. Such reconstruction can be an explicitly named experimental mode later.

The first fixture task can be a small parser/stream-processing problem with two candidate approaches. A read-only/artifact-producing task is enough for the core loop. If branches edit files, give each its own fixture worktree or explicit isolated workspace; a conversation fork is not a filesystem fork. Never pretend isolation exists for shared databases or network effects merely because each branch has a different directory.

### Required end-to-end experiment

1. Create or load a parent, complete a seed turn, and capture the checkpoint.
2. Create two branches from that exact boundary. Include an ephemeral branch if supported; otherwise record and test the unsupported result.
3. Run different exploration inputs independently and capture completed output artifacts.
4. Create one selection interaction referencing both branches and artifacts.
5. Offer the same selection through terminal/CLI and a chooser-agent surface. The chooser is a separate runnable participant; keep the original parent idle until reintegration.
6. Resolve it once. Observe both views update from the same accepted transition.
7. Construct a result bundle that records which branch outputs were selected, combined, or rejected and includes the source references. Selection and synthesis are distinct actions; do not call concatenated transcripts a semantic merge.
8. Deliver that bundle explicitly to the parent and obtain a subsequent response. Prefer a supported next-turn input first; add native history injection at a suitable boundary when available. Never forge an original user utterance or misattribute a branch's assistant message to the parent. Persist the exact delivered representation and source mapping.
9. Stop Codex. Replay the stored experiment and inspect all branch outputs and lineage from Nema alone.

Importing branch knowledge must not silently import its entire viewpoint/history or rewrite the parent before the fork. Native history injection, message delivery, workspace patch application, and semantic synthesis are separate operations with separate records.

## 11. The continuation lab — a real experiment, not a slogan

Create a pure small Flix application with a choice effect. Interpret the choice by resuming the captured continuation at least twice. The remainder should build meaningful branch recipes, not just two printed booleans.

For example, each resulting recipe contains the parent checkpoint, a strategy-specific prompt, observer/context selection, and workspace mode. Persist each recipe as an artifact. Execute the recipes through the fork bench so that a host-level potential future becomes an explicit harness branch. Immediate multi-resumption is enough for the mandatory experiment.

Keep the three independent operations visible:

```text
Flix resumption exploration
native conversation fork
workspace/external-state branch
```

Neither the compiler nor the harness automatically couples them. Assign identities and source links to the coupling that Nema performs. A live material transaction or already-issued native request must not be duplicated by resuming an enclosing computation twice.

Retained/delayed resumption is a bounded research spike. Test later invocation, enclosing lifetime, captured mutable state, and the effect handlers actually used. If supported, demonstrate it inside a live scope and document the limits. If not supported or unsuitable, persist a recipe/control-point record and reconstruct a fresh execution. Do not present that reconstruction as the original captured continuation.

A recipe for later execution should identify code/version, named entry/control point, captured explicit values/references, source frontier, and supplied answer. It is not a universal serialized stack. Never blindly rerun a workflow from its beginning and duplicate external effects to simulate durability.

Add an order-sensitive handler test: call the continuation before recording a local post-action and show the actual order. Use it to explain why scope-end work is not automatically chronological execution or exception-safe cleanup.

## 12. Hooks, exec channels, and polyglot participation

Provide a documented local protocol over a Unix-domain socket with versioned JSON messages, request IDs, typed method contracts, and resumable observation by journal cursor. This is Nema IPC, not distributed objects. Use a small Python standard-library client as a proof that participation does not require Flix/JVM. Keep the wire boundary replaceable; NNG or a richer encoding is a future transport choice, not a prerequisite.

After the mandatory fork demo works, add a bounded rendezvous experiment:

```text
nema rendezvous <key> --generation <n> --payload <json> --parties 2 --timeout <duration>
```

Two independently runnable agent exec processes join the same rendezvous. The daemon records arrivals and releases both with the peer messages when the condition is met. Include cancellation, timeout, generation isolation, and disconnect behavior. Do not block the only worker needed to serve the second arrival. The helper uses the same interaction/journal machinery; it is not an invisible socat side universe.

Record what the actual harness does with a long-running exec: it may return a session handle, stream output, or require polling. Do not assume that stdout streaming means the model is continuously attending to it. Provide a simple documented usage contract; testing actual behavior is the point.

Provide a systemd user unit for the daemon and project-local installation/start/stop instructions. Keep paths/configurable directories explicit. Verify child-process cleanup and restart behavior. No automatic boot/linger/global service modifications are required merely to ship v0.

## 13. Work order and stopping conditions

Implement in vertical increments. Each increment must leave build/test commands working and update `PROGRESS.md` with evidence, remaining issues, and the next action.

### Milestone A — bootstrap and executable facts

Inspect root; pin tools; compile Flix hello/test; generate Codex schemas; compile effect/CSP/associated-type probes; create fake protocol peer; document capabilities. Do not spend the whole run on scaffolding.

### Milestone B — recorded bidirectional transport

Run a real app-server handshake if installed; implement raw capture, typed subset, concurrent router, single-writer output, native request response, cancellation, and process lifecycle. Prove a blocked tool does not block observation/routing. Offline fixtures remain runnable without account access.

### Milestone C — interactions, Datalog, and two views

Implement the pending state machine, terminal and agent surfaces, actual Flix Datalog action derivation, duplicate/stale response behavior, and snapshot-plus-subscription consistency. Complete the custom-tool -> human answer -> Codex continuation demonstration.

### Milestone D — fork, select, reintegrate, replay

Complete the fork-bench experiment, source-history capture, ephemeral retention, output artifacts, selection from either participant type, explicit result bundle, and parent continuation. Supply recorded and live modes. A failure of one native optional mode is a capability result, not permission to fake it.

### Milestone E — multi-shot and operational finishing

Compile/run the multi-resumption recipe experiment; connect its outputs to actual branch execution. Finish hook ingress, recovery/reporting, systemd unit, and the non-JVM client. Do the bounded delayed-resumption and rendezvous spikes after the main path works.

Do not stop at a design, an SDK interface, or mock-only evidence if live execution is available. Do not continue adding conceptual infrastructure after the mandatory demonstrations and tests pass. There is a finish line: a runnable, inspectable seed with documented seams and executable experiments.

If access or runtime support blocks live work, complete every independent component, preserve exact errors and reproductions, and say which live tests were not run. A fixture is not a successful live test. Do not claim that setting an internal flag or seeing an RPC acknowledgment proves an agent consumed the new state.

## 14. Acceptance tests and evidence

Provide executable tests for the following; group them into sensible suites rather than one brittle end-to-end script:

- Interleaved requests from both directions, including the same textual ID and distinct ID types, resolve correctly.
- A slow or unanswered tool request does not stop unrelated notifications, another branch, cancellation, or shutdown.
- Raw unknown fields, unknown variants, malformed frames, duplicate observations, and final-item/delta differences are retained and represented honestly.
- Input origin -> encoded submission -> send/ack/native observation correlation is inspectable; missing links remain missing, not guessed.
- Hook capture preserves native payloads and supports concurrent invocations/spool recovery without replacing existing hook configuration.
- One pending interaction appears in both views; one accepted answer updates both; identical retries and conflicting later answers behave deliberately.
- Datalog action derivation agrees with a reference implementation and changes after relevant state transitions; unrelated revisions do not invalidate an otherwise valid response.
- Subscriptions do not lose updates between snapshot and tail; a slow observer can catch up without blocking control traffic.
- Historical forks do not include later parent turns; native fork ancestry and delegation are not conflated.
- Ephemeral-fork outputs remain inspectable after Codex exits, without claiming native execution was restored.
- Branch-result import retains branch provenance and does not rewrite or impersonate earlier parent history.
- Replay rebuilds the same semantic projections without model requests or material tool execution. Repeated replay produces the same result under a fixed schema/reducer version.
- Restart marks disconnected live requests accurately and preserves explicit pending protocol state; it does not fabricate revived continuations or replay unknown external writes.
- The fork controller survives restart at the selection control point, reconnects to the same durable interaction, and advances after its accepted answer without duplicating an already-recorded delivery. Ambiguous native outcomes remain unknown until reconciled.
- Multi-shot Flix code actually resumes the captured continuation multiple times and produces independent immutable branch recipes.
- Separate workspace branches do not bleed fixture mutations; shared external resources remain explicitly shared.
- The non-JVM client reads a surface and resolves an interaction through the documented protocol.
- Child processes, sockets, terminal modes, and SQLite handles are cleaned up on normal exit and interrupt; no indefinite join on a forgotten worker.

Keep live tests small and bounded: two initial branches and one chooser are enough. Use configured models/effort or user-supplied settings rather than inventing a model name or a reasoning-effort enum from the word “ultra.” Make model/network use visible in the test report.

## 15. Deliverables and operator experience

Prefer one obvious entry point, such as `./bin/nema`, with commands for:

```text
doctor
serve
watch
thread start / resume / inspect
fork / fork inspect
interaction list / show / respond
artifact show
replay
hooks install / inspect
demo bidirectional --mode fixture|live
demo fork-compare --mode fixture|live
lab continuations
```

These are intended behaviors, not a requirement to preserve exact spelling. Document the actual executable interface and make README examples copyable. The inspector should show native thread IDs, branch lineage, pending interactions, artifacts, and recent records, with drill-down to raw payloads. Simple terminal rendering is sufficient; do not build a tiling compositor first.

Expected repository artifacts: working source, pinned build configuration, fake peer and fixtures, tests, demo commands, local IPC schema/examples, a systemd user unit, `README.md`, `ARCHITECTURE.md`, `PROGRESS.md`, `docs/capabilities.md`, `docs/experiments.md`, and an acceptance report with actual commands/results. Keep runtime traces out of the source tree unless explicitly sanitized test fixtures.

Before finishing, run the build, test suites, fixture demonstrations, and available live demonstrations. Report exactly what passed, what is unsupported, what is untested, and how to reproduce the tangible result. Link claims in the report to test output or stored experiment IDs. Prefer a working narrow implementation over broad placeholder APIs. Do not report “complete” for TODO bodies, pseudocode, or code that never compiled.

## 16. Explicitly deferred, without closing the doors

No general semantic ontology, generic actor calculus, distributed object system, automatic natural-language-to-fact truth oracle, incremental Datalog engine, full workflow compiler, cross-language continuation migration, universal effect registry, or elaborate policy framework.

No native tool hot-swap assumption. No claim that instructions alone implement a contract. No silent fallback from a fork to a summary-seeded fresh thread. No assumption that a conversation fork forks files. No claim that preserved observations include unavailable internal model/harness state.

Claude, Agents SDK, local-model, NNG, Graal/Truffle, and executable-workflow integrations are future bindings. Keep their seams visible through typed operations, explicit records, artifacts, and a documented IPC boundary. Do not build all of them now.

A later transaction interpreter can wrap managed workspace operations; first distinguish snapshots, proposed writes, and committed changes. Whole-turn locks across arbitrary shell/network effects are not a v0 promise.

The portable meaning is the operation contract and shared interaction identity, not a particular tool schema, UI widget, or programming language's call stack.

## 17. Sources and verification notes

The following primary sources were checked when preparing this handoff. URLs are reference inputs for the implementation agent. Re-check against installed versions; these are not a substitute for local generated schemas and compile probes.

**S1 — Codex app-server.** The documentation describes bidirectional request/response/notification traffic, schema generation, thread/turn/item boundaries, historical and ephemeral fork forms, dynamic tools, steering, and history injection. Some fields/operations are experimental. Keep claims tied to the tested binary.

```text
https://developers.openai.com/codex/app-server/
https://learn.chatgpt.com/docs/app-server
https://github.com/openai/codex/tree/main/codex-rs/app-server
```

**S2 — Codex hooks.** Hook sources include project-local configuration. The current docs describe lifecycle/tool/compaction hooks and concurrent matching hooks. Discover the actual accepted configuration and payload shapes; do not copy another harness's hook protocol.

```text
https://developers.openai.com/codex/hooks/
https://learn.chatgpt.com/docs/hooks
```

**S3 — JSON-RPC.** Requests and responses are correlation roles, not transport directions; notifications do not receive responses. Codex documents its own envelope variation.

```text
https://www.jsonrpc.org/specification
```

**S4 — Flix effects and CSP.** Deep multi-resumption handlers are documented. The effects chapter also describes restrictions around spawned computations and parameterized effects; repository capabilities can differ by version. Storage/escape/rebinding of a resumption is not established merely by the multi-shot example.

```text
https://doc.flix.dev/effects-and-handlers.html
https://doc.flix.dev/concurrency.html
https://doc.flix.dev/regions.html
```

**S5 — Flix type families and logic.** Associated types/effects and first-class fixpoints are documented. The compiler repository includes a higher-kinded associated-member test. Use real, version-pinned examples.

```text
https://doc.flix.dev/associated-types.html
https://doc.flix.dev/associated-effects.html
https://doc.flix.dev/fixpoints.html
https://raw.githubusercontent.com/flix/flix/master/main/test/flix/Test.Dec.Assoc.Type.Kind.flix
https://flix.dev/get-started/
```

## 18. Canonical execution environment

This is a Codex remote project rooted directly at
`/home/elkeegano.guest/nema` in the Arch Linux ARM guest. Read, edit, build,
test, run Codex app-server, and manage project processes natively in that
guest. Do not use the old macOS/Lima command shim, `/mnt/data` attachment
paths, or the unrelated macOS `~/nema` tree.

Keep live databases, sockets, journals, and private traces under `.nema/` and
out of Git. `AGENTS.md` and `PROGRESS.md` contain the current operational and
resume instructions.

## Final instruction

Build the observable membrane, then use it to make branching and answering a shared interaction visible. The point of the first version is to turn the conversation's most promising claims into experiments we can run, inspect, replay, and change—not to fossilize every attractive abstraction we mentioned.
