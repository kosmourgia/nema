# An actual Flix participant

The participant compiles with the repository's observed Flix 0.75.3 and default
Java runtime. It registers as a `client`, inspects a retained snapshot, issues a
typed result-bearing effect, and tails the same retained transitions as the
terminal inspector. No code from the fork bench, Vivarium, or Aeron is imported.

```sh
modules/zygon/flix/run check
modules/zygon/flix/run test
ZYGON_SOCKET=/absolute/path/to/private/zygon.sock \
ZYGON_TARGET_ID=the-registered-id \
ZYGON_TARGET_INCARNATION=the-current-incarnation \
ZYGON_OPERATION=demo.delay \
ZYGON_ARGUMENTS='{"seconds":0.5}' \
ZYGON_REQUIRE_OBSERVATION=1 \
modules/zygon/flix/run

PYTHONPATH=modules/zygon python -m unittest discover -s modules/zygon/flix -p test_live.py -v
```

Use the exact target ID and incarnation returned by inspection. The operation
and its arguments belong to the selected provider. `demo.delay` is the default.
Set `ZYGON_REQUIRE_OBSERVATION=1` when the operation should produce an observation
while pending. A quick `browser.read` may complete with no intervening observation.

`Zygon.LocalRequest` is a real Flix effect. Its interpreter lives inside a spawned
worker, sends the invocation once, then retains its continuation while receiving
on a dedicated result channel. A separate Flix worker pages the retained cursor
and sends observations to the main consumer over another CSP channel. The
consumer keeps rendering while the request is pending. Only the matching
invocation's first retained terminal record resumes the original correlation.

An invoke rejection sends a failure handshake to the waiting driver. Observation
transport loss resumes the waiter with a clearly local `OutcomeUnknown`, an
empty retained record ID, and `retainedTerminalEstablished:false`; it does not
invent a server transition or retry the command. A provider disconnect normally
produces the registry's retained `outcome-unknown`, whose actual record ID is
printed. No scope exit relies on implicit cancellation of a blocked Flix worker.

Flix records distinguish registration identity, incarnation, parent reference,
mode, medium, operations, presence, optional native binding, invocation, result,
and observation. Each typed projection keeps its raw JSON alongside recognized
fields. Unknown native statuses remain `NativeUnknown`; they are not silently
classified as pending, completed, or live.

Actual Flix `inject` and Datalog derive hosts, surfaces, live incarnations,
bindings, pending invocations, and recursive reachability from a finite snapshot.
Reachability requires exact parent incarnations and online presence. Unknown
presence removes a live-edge claim while preserving the host registration. No
query invokes an operation, starts a process, or assigns new identities.

## Native build observations

The launcher verifies compiler 0.75.3, limits heap to 1200 MiB and compiler workers
to two, and builds in a fresh module-local `.nema/b-*` directory on every run.
Concurrent test/browser/process demos have separate scratch files. Artifacts
remain ignored and available for diagnosis.

The small Java bridge owns bounded Unix socket reads/writes and JSON accessors.
It uses the Gson classes already present in the pinned compiler JAR. Those
classes are copied into the generated local bridge artifact because Flix's
external-JAR classloader is isolated. No Maven download, second JDK, handwritten
JSON parser, or unbounded response queue is introduced. A protocol exchange has
a five-second local I/O deadline and never retries a write.

Observed compiler diagnostics guided the implementation:

```text
The 'build' command does not support file arguments.
Undefined Java class 'zygon.flix.Bridge'.
java.lang.NoClassDefFoundError: com/google/gson/JsonElement
Expected '=>' before '|'.
Mismatched module and file: 'Zygon' in 'src/Main.flix'.
String.startsWith: expected '{ prefix = String }', got 'String'.
```

The pinned compiler's [Bootstrap source](https://github.com/flix/flix/blob/v0.75.3/main/src/ca/uwaterloo/flix/api/Bootstrap.scala)
shows directory-mode Java discovery under `lib/external`, while manifest mode
uses declared dependencies. The independent launcher uses directory mode in its
generated build directory and keeps the version pin in the module manifest.
Imports are scoped inside the Flix module; cases use separate patterns; string
prefix matching uses the observed named argument.

## Verification

Four compiled Flix tests pass: nested reachability and stale parent incarnations,
unknown presence/binding, terminal/native-unknown status projection, and a typed
effect resuming under its original correlation.

```text
Passed: 4, Failed: 0. Skipped: 0. Elapsed: 76.0ms.
```

`test_live.py` uses an actual Flix JVM against the real Unix protocol. Its provider
is explicitly a synthetic cooperative endpoint; it tests observations while
waiting, identical retained terminal IDs, stale-target rejection without a
stranded CSP worker, and retained uncertainty after provider disconnect.
Real owned-process and Chromium extension demonstrations have separate evidence.

```text
test_actual_flix_observes_and_resolves_same_retained_record ... ok
test_provider_disconnect_resumes_with_retained_uncertainty ... ok
test_rejected_stale_target_does_not_strand_csp_worker ... ok
Ran 3 tests in 15.548s
OK
```
