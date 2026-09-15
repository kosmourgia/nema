# Vivarium Live

`Nema.Lab.Vivarium.Live` is the live realization beside the immutable
`Nema.Lab.Vivarium` oracle. It is still a specimen, not production
architecture.

```text
pure World semantics
        |
        v
worker-local Pulse / Stage / Ask handlers
        |
        v
unbuffered CSP channels + region-bound processes
        |
        v
immutable event trace and Datalog topology projection
```

Run the deterministic live driver:

```sh
./bin/nema lab vivarium-live
```

Watch for the causal joints:

- `O17 WAIT P3.next` is sent immediately before the Pulse handler blocks in
  `Channel.recv` while holding the effect resumption.
- `DOWNBEAT` is emitted by the separate deterministic driver before it sends a
  Beat. The handler then emits `resume O17` and invokes the captured resumption.
- Beat 1 aborts with no publication. Beat 2 commits `I8` before two independently
  spawned sinks observe it during diastole; both receive the published closed
  Revision `R41`, never the staged intermediate value.
- At Beat 3 the `Ask` handler captures the requesting continuation and blocks.
  Stable address `C17` is rebound illustratively and resolved by driver Beat 4.
  The continuation resumes under that new `BeatContext`, then waits on
  `P3.next()` again.
- The Measure workers wait independently. P3 advances through 6/7 before P4
  reaches 9, opening at `{P3:7, P4:9}` and closing only after both obligations
  at `{P3:9, P4:11}`.

All waits and resumptions are coordinated by unbuffered channels. There are no
sleeps or wall-clock timers.

The deterministic driver is split into small Beat/Measure phase functions.
Besides making the causal protocol easier to read, this avoids a Flix 0.75.3
`ConstraintGen` stack overflow observed when the same driver was one deeply
nested block on a JVM with a 1 MiB thread stack.

## One observed Flix constraint

On Flix 0.75.3, leaving a region while its child is blocked in `Channel.recv`
waits rather than implicitly cancelling that child. The checked probe therefore
uses cooperative shutdown: the driver sends `Stop`, the Pulse handler resumes,
the Observer emits `Detached`, and only then does the region close. The live
resources remain region-bound; Object identity and Revision history are plain
semantic data outside their lifetime.

## Actual Flix REPL

Start `flix repl` in the repository root. Live channel endpoints and spawned
processes intentionally do not escape their region, so the REPL cannot retain
a half-running specimen between prompts. `scenario()` is the honest,
deterministic manual driver: each call starts the workers, advances every Beat
through explicit channel sends, shuts them down, and returns the observations.

The following commands and outputs were verified on Flix 0.75.3.

Inspect the inert identity, the first observed wait, an aborted Beat, and the
next committed Revision:

```text
flix> let r = Nema.Lab.Vivarium.Live.scenario(); (r#initialState#object, r#attachedView#waitingOnPulse, r#afterAbort#revision, r#afterNotify#revision)
(17, Set#{(17, 3)}, 40, 41)
```

Inspect the two observer-relative registrations, one authoritative `Notify`,
and its independent live interpretations:

```text
flix> let r = Nema.Lab.Vivarium.Live.scenario(); (r#attachedTopology#registrations, r#notifyInvocation, r#liveInterpretations)
(Set#{(18, specimen, 17), (19, same specimen, 17)}, Notify(8, 8, 17, awake), Set#{recorded:awake, witnessed:awake})
```

Inspect the unfinished result-bearing invocation, stable binding history, later
Beat context, and pending Datalog projection:

```text
flix> let r = Nema.Lab.Vivarium.Live.scenario(); (r#askInvocation, r#bindingHistory, r#resumedBeat, r#pendingView#pendingContinuations)
(Ask(9, 9, 17, 17, continue?), Live :: Durable(recipe:continue) :: Live :: Resolved(yes) :: Nil, 4, Set#{17})
```

Inspect the independently driven Measure frontiers:

```text
flix> let r = Nema.Lab.Vivarium.Live.scenario(); (r#measure#p3Beats, r#measure#p4Beats, r#measure#opening, r#measure#closing)
(6 :: 7 :: 8 :: 9 :: Nil, 9 :: 10 :: 11 :: Nil, (3, 7) :: (4, 9) :: Nil, (3, 9) :: (4, 11) :: Nil)
```

Query the quiescent topology through Flix's built-in fixpoint engine. This
query cannot tick a Pulse, dispatch an invocation, or resume a continuation:

```text
flix> let r = Nema.Lab.Vivarium.Live.scenario(); let q = Nema.Lab.Vivarium.Live.queryTopology(r#finalTopology); (q#dormantObjects, q#pendingContinuations, q#measuresClosed)
(Set#{17, 18, 19}, Set#{}, Set#{2})
```

Exercise the cooperative region-shutdown probe directly:

```text
flix> Nema.Lab.Vivarium.Live.scopeShutdownProbe()
(17, 40, true)
```

See [vivarium.md](vivarium.md) for the pure birth, locator-binding, Observer,
Beat, and deferred-effect commands. The illustrative `Durable(recipe)` binding
never serializes the held JVM continuation.
