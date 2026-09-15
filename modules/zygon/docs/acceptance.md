# Observed acceptance — 15 September 2026

Native Arch Linux ARM, Python 3.14.7, Flix 0.75.3, default Java 26.0.2.1,
Chromium 153.0.8010.36. These are executed checks, not mock extension claims.

## Process and actual Flix demo

Command: `modules/zygon/bin/zygon demo` — exit 0.
Private report: `.nema/zygon/demos/c393f1ac/report.json`.
Selected actual output (unrelated topology lines omitted):

```text
FLIX accepted id=9c07321c-cdc2-4df6-bc4f-1f5d103c6f30 correlation=flix-request-cff1396d-2e42-4f74-bf4c-6fbdf63740a3 status=Accepted
FLIX observation while request waits id=ffcd486f-13af-4b8c-943f-85c55ca07f1c source=python-demo incarnation=process-f67a07685c4342388f07d8fcf55ecd80 seq=66 kind=capture.chunk
FLIX resolved id=9c07321c-cdc2-4df6-bc4f-1f5d103c6f30 correlation=flix-request-cff1396d-2e42-4f74-bf4c-6fbdf63740a3 status=Completed recordId=eac23cfe-20a8-49c4-b066-9ebe81996819 observations=9 value={"echo":"Flix-result","correlation":"9c07321c-cdc2-4df6-bc4f-1f5d103c6f30"}
{"PASS": "actual Flix and terminal share retained result", "recordId": "eac23cfe-20a8-49c4-b066-9ebe81996819"}
{"PASS": "pipe EOF and exit code observed", "exitCode": 0}
{"PASS": "PTY merged bytes resize cooperative interrupt and stop", "exitCode": 0}
{"PASS": "every accepted invocation has an explicit terminal state"}
{"PASS": "offline retained transitions reproduce snapshot", "cursor": "168"}
```

The same run checks a separate terminal process seeing the two nested child
surfaces, repeated calls, fast completion while a delayed call stays pending,
unsolicited capture, exact partial/invalid UTF-8 and stdin, stable owned identity
with a new incarnation, and attachment/detachment leaving the external host alive.
Flix Datalog shows pending populated during the request and empty after it.

## Real browser extension and actual Flix demo

Command: `modules/zygon/bin/zygon demo-browser --with-flix` — exit 0.
Private test profile: `.nema/zygon/b-31f721/profile`.
Selected actual output:

```text
{"annotation": "addressed by terminal", "check": "annotation-round-trip-shared-transition", "invocationId": "afdbfc69-32ef-4412-8925-657e04fcde42", "recordId": "94d3c66d-bd32-4d5f-b486-f6a8071b4f31", "status": "completed"}
{"check": "read-replace-focus", "focus": "completed", "read": "completed", "replace": "completed"}
{"check": "flix-targeted-real-document", "completed": true, "invocationId": "81a058d3-2c45-4459-b1a4-97e72b6255f2", "recordId": "301b4dde-a14f-4a31-a5a0-f7274ed4a610"}
{"check": "worker-reconnected", "documentContinuity": true}
{"check": "transport-loss-reconnect", "preservedDocumentIncarnation": true}
{"check": "bridge-restart-same-browser", "preservedDocumentIncarnation": true, "preservedStableId": true}
{"browserStillRunning": true, "check": "detach-preserves-browser"}
{"check": "all-browser-demo-invocations-terminal", "passed": true}
```

The harness also navigated the page, observed `stale-incarnation: binding is no
longer active` for the old target, closed/reopened the tab, and restarted Chromium.
Only extension installation identity survived full browser restart. Actual Flix
received three observations while awaiting the page action and resolved under
`flix-request-978e1a06-a732-435b-9814-a4119f487974`; its terminal record ID above
matched the independent inspector's retained result.

See [selected sanitized traces](../fixtures/observed/README.md), [protocol test
coverage](adversarial.md), [process coverage](process.md), and [Flix probes](flix.md).

## Operational limits

- Datalog's Live predicate denotes a currently online observed registration,
  not a claim that a child OS process is still executing. Exit metadata and the
  operation list express the owned process's actual post-exit capabilities.
- Group cleanup covers descendants that remain in the owned process group.
  Direct-child parent-death signaling is tested. Detached/escaped descendants
  require a cgroup scope or supervisor; this is not a second service manager.
- A disk-full/I/O failure can prevent even an emergency record from reaching
  storage; the companion stops and reports failure rather than claiming capture.
- Polling and deadlines are local operational mechanisms, not distributed leases.
  No non-idempotent operation is automatically replayed after uncertainty.
- Focus means Chromium accepted the focus request; headless execution does not
  establish a graphical compositor focus observation. No screen geometry inferred.

## Final checks

```text
modules/zygon/bin/zygon test
  Ran 54 tests in 4.918s — OK
  Passed: 4, Failed: 0. Skipped: 0. Elapsed: 81.6ms.
modules/zygon/bin/zygon check
  exit 0
JAVA_TOOL_OPTIONS=-Xmx1200m ./bin/nema test
  Passed: 42, Failed: 0. Skipped: 0. Elapsed: 830.7ms.
  Ran 11 tests in 1.578s — OK
JAVA_TOOL_OPTIONS=-Xmx1200m ./bin/nema check
  exit 0
systemd-analyze --user verify modules/zygon/ops/nema-zygon.service
  exit 0, no diagnostics; no service installed or enabled
PYTHONPATH=modules/zygon python -m unittest discover -s modules/zygon/flix -p test_live.py -v
  Ran 3 tests in 15.548s — OK (actual Flix JVM)
```

The native Python3.14 server shutdown closes client transports before awaiting
listener closure. A regression also proves a provider can close its own client
from a callback without trying to join itself. Both follow observed failures,
with executable tests retaining the behavior.

## Guest recovery

The first tmpfs worktree was lost on reboot and reconstructed into the requested
sibling, with committed checkpoints. The guest disk then entered ext4
`emergency_ro` following device write I/O errors, despite 79G free. Work was
salvaged into `/tmp/nema-zygon-recovery.elwh3D/checkout` and pushed to
`codex/zygons`; both combined demos above ran there. The guest disk requires
repair before the sibling checkout can advance. No disk repair or main-branch
reconciliation was attempted. Runtime databases/profiles remain uncommitted.
