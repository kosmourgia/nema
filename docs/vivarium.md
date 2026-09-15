# Vivarium

`Nema.Lab.Vivarium` is a compact, immutable laboratory model. It is not a
second runtime and does not replace the fork bench. It makes Object identity,
Revision history, observer-relative registration, Pulse/Beat progression,
transactional effect staging, escaped local resumptions, bounded Measures,
and Datalog projections visible in one deterministic specimen.

The corresponding effects/CSP realization is documented in
[vivarium-live.md](vivarium-live.md).

Run the whole thought experiment:

```sh
./bin/nema lab vivarium
```

Run its behavioral checks with the rest of Nema:

```sh
./bin/nema test
```

## Actual Flix REPL

Start the installed Flix 0.75.3 shell from the repository root:

```sh
flix repl
```

The shell evaluates one expression at a time. Each command below was copied
back through that shell on Flix 0.75.3; paste the complete line after `flix>`.

Birth an inert Object. Its Object, first Revision, and Value are distinct data;
Genesis contains no physical location:

```text
flix> let (w1, o) = Nema.Lab.Vivarium.birth("seed", Nema.Lab.Vivarium.Value.Text("asleep"), Nema.Lab.Vivarium.empty()); (Nema.Lab.Vivarium.showObject(o), Nema.Lab.Vivarium.describe(w1))
(O17, O17 object genesis="seed" head=R40 payload=V70 revisions=1 locators=Set#{})
```

Physical location is a mutable representation binding whose target can be an
Object, Revision, or Payload. Attach, replace, and remove a binding. `O17` and
`R40` remain unchanged, and zero or several locators are valid:

```text
flix> let (w1, o) = Nema.Lab.Vivarium.birth("seed", Nema.Lab.Vivarium.Value.Text("asleep"), Nema.Lab.Vivarium.empty()); let target = Nema.Lab.Vivarium.RepresentationTarget.Object(o); let w2 = Nema.Lab.Vivarium.bindLocator(target, Nema.Lab.Vivarium.Locator.Aeron("stream:17"), w1); let w3 = Nema.Lab.Vivarium.replaceLocator(target, Nema.Lab.Vivarium.Locator.Aeron("stream:17"), Nema.Lab.Vivarium.Locator.CAS("sha256:17"), w2); let w4 = Nema.Lab.Vivarium.unbindLocator(target, Nema.Lab.Vivarium.Locator.CAS("sha256:17"), w3); (Nema.Lab.Vivarium.showObject(o), Nema.Lab.Vivarium.objectHead(o, w4), Nema.Lab.Vivarium.locators(target, w2), Nema.Lab.Vivarium.locators(target, w3), Nema.Lab.Vivarium.locators(target, w4))
(O17, Some(RevisionId(40)), Set#{Aeron(stream:17)}, Set#{CAS(sha256:17)}, Set#{})
```

Attach a Pulse and driver, thereby making the same Object a live Observer:

```text
flix> let (w1, o) = Nema.Lab.Vivarium.birth("seed", Nema.Lab.Vivarium.Value.Text("asleep"), Nema.Lab.Vivarium.empty()); let (w2, p) = Nema.Lab.Vivarium.newPulse("repl", w1); let w3 = Nema.Lab.Vivarium.animate(o, p, Nema.Lab.Vivarium.Reaction.Notifier, w2); Nema.Lab.Vivarium.describe(w3)
O17 object genesis="seed" head=R40 payload=V70 revisions=1 locators=Set#{}
P3 pulse beat=0 driven-by="repl"
O17 observer has-pulse=P3 reaction=Notifier
```

Tick once and inspect the Beat phases. Systole reads `R40`; only commit makes
`R41` and `I8` authoritative; diastole dispatches afterward:

```text
flix> let (w1, o) = Nema.Lab.Vivarium.birth("seed", Nema.Lab.Vivarium.Value.Text("asleep"), Nema.Lab.Vivarium.empty()); let (w2, p) = Nema.Lab.Vivarium.newPulse("repl", w1); let w3 = Nema.Lab.Vivarium.animate(o, p, Nema.Lab.Vivarium.Reaction.Notifier, w2); let (_w4, r) = Nema.Lab.Vivarium.tickNotify(p, o, "awake", true, w3); Nema.Lab.Vivarium.reportTrace(r)
P3 Beat 1 SYSTOLE (visible R40)
  stage R41 + E8 Notify
  COMMIT E8 -> I8
  DIASTOLE dispatch I8 to 0 observer(s)
P3 Beat 1 SETTLED
```

Give another Observer its own name for the Object, tick, and inspect the
subject's committed Revision count and observer-relative names:

```text
flix> let (w1, o) = Nema.Lab.Vivarium.birth("seed", Nema.Lab.Vivarium.Value.Text("asleep"), Nema.Lab.Vivarium.empty()); let (w2, p) = Nema.Lab.Vivarium.newPulse("repl", w1); let w3 = Nema.Lab.Vivarium.animate(o, p, Nema.Lab.Vivarium.Reaction.Notifier, w2); let (w4, watcher) = Nema.Lab.Vivarium.birth("watcher", Nema.Lab.Vivarium.Value.Text("empty"), w3); let w5 = Nema.Lab.Vivarium.animate(watcher, p, Nema.Lab.Vivarium.Reaction.Recorder, w4) |> Nema.Lab.Vivarium.register(watcher, o, "my specimen", p); let (w6, _r) = Nema.Lab.Vivarium.tickNotify(p, o, "awake", true, w5); (Nema.Lab.Vivarium.revisionCount(o, w6), Nema.Lab.Vivarium.observerNames(o, w6))
(2, Set#{my specimen})
```

Query the same world as relations. These Datalog queries return topology; they
do not tick a Pulse or perform a transition:

```text
flix> let (w1, o) = Nema.Lab.Vivarium.birth("seed", Nema.Lab.Vivarium.Value.Text("asleep"), Nema.Lab.Vivarium.empty()); let (w2, p) = Nema.Lab.Vivarium.newPulse("repl", w1); let w3 = Nema.Lab.Vivarium.animate(o, p, Nema.Lab.Vivarium.Reaction.Notifier, w2); (Nema.Lab.Vivarium.queryLiveObservers(w3), Nema.Lab.Vivarium.queryReachableSubjects(w3))
(Set#{17}, Set#{})
```

Coordinate two independent Pulses through a Measure. Their local Beat numbers
need not match. The opening and closing frontiers, roles, activity, readiness,
and obligations remain visible:

```text
flix> let (w1, p1) = Nema.Lab.Vivarium.newPulse("left", Nema.Lab.Vivarium.empty()); let (w2, p2) = Nema.Lab.Vivarium.newPulse("right", w1); let (w3, m) = Nema.Lab.Vivarium.newMeasure("duet", List#{(p1, "caller"), (p2, "answerer")}, List#{"question", "answer"}, w2); let w4 = Nema.Lab.Vivarium.ready(m, p1, w3); let w5 = Nema.Lab.Vivarium.ready(m, p2, w4); let w6 = Nema.Lab.Vivarium.fulfill(m, "question", w5); let w7 = Nema.Lab.Vivarium.fulfill(m, "answer", w6); Nema.Lab.Vivarium.describeMeasure(m, w7)
M2 Closed [P3 role=caller activity=settled ready=true open=Some(0) close=Some(0); P4 role=answerer activity=settled ready=true open=Some(0) close=Some(0)] obligations=[question=true, answer=true]
```

Finally, commit a result-bearing `Ask`. Its handler captures a real local Flix
resumption at stable address `C17`; a later Beat resolves and resumes it under
that Beat's dynamic handler context:

```text
flix> let (w1, o) = Nema.Lab.Vivarium.birth("seed", Nema.Lab.Vivarium.Value.Text("asleep"), Nema.Lab.Vivarium.empty()); let (w2, p) = Nema.Lab.Vivarium.newPulse("repl", w1); let w3 = Nema.Lab.Vivarium.animate(o, p, Nema.Lab.Vivarium.Reaction.Notifier, w2); let (w4, session, _stage) = Nema.Lab.Vivarium.ask(p, o, "continue?", w3); let address = Nema.Lab.Vivarium.sessionAddress(session); let (w5, finished, resolution) = Nema.Lab.Vivarium.resolveAsk(p, "yes", session, w4); (resolution, Nema.Lab.Vivarium.finishedAt(finished), Nema.Lab.Vivarium.continuationBinding(address, w5))
(P3 Beat 2 DIASTOLE resolve C17 = "yes", Some(P3 Beat 2), Some(Resolved(yes)))
```

Exit with `:quit`. The local resumption never enters `World`; only its
`ContinuationAddress` and time-relative `Live`, `Durable`, `Resolved`, or
`Dropped` binding do. Durable continuation serialization is deliberately not
claimed.
