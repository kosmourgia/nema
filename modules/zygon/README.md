# Zygons: make a host addressable

A small local companion registers an ordinary process or selected Chromium demo
page, its child surfaces, their current incarnations, and their actual operations.
This module runs independently of the Nema fork bench, Vivarium, and Aeron.

## Run the experiments

From this feature worktree:

```sh
modules/zygon/bin/zygon check
modules/zygon/bin/zygon test
modules/zygon/bin/zygon demo
modules/zygon/bin/zygon demo-browser --with-flix
```

Requirements: native Linux, Python 3.14, pinned Flix 0.75.3, the machine's default
JDK (tested Java 26), and Chromium for the browser demo. Python uses only its
standard library. The Flix launcher builds independently inside the module.
The browser test uses real unpacked MV3 messaging in a fresh private profile and
an inherited DevTools pipe for test orchestration; it exposes no debugging TCP
port. `demo --no-flix` isolates the process side for troubleshooting.

The process demo prints one host with two surfaces in a separate terminal client,
interleaves slow/fast/repeated requests with unsolicited output, checks exact
invalid UTF-8 bytes, runs the actual Flix effect/CSP/Datalog participant, and
demonstrates EOF, PTY resize/interrupt/stop, restart, and external attachment.
Both clients identify the same retained terminal record. It finally stops the
registry and checks deterministic replay.

## Manual process session

Use separate terminal sessions for the long-running commands. The default
runtime is this worktree's `.nema/zygon`; all sockets, databases and captures stay
there. Pass global `--runtime PATH` and/or `--socket PATH` before the command for
another private session. Keep Unix socket paths under the OS length limit.

```sh
# Session 1: local registry
modules/zygon/bin/zygon serve

# Session 2: owned demo child, with a separate cooperative control socket
modules/zygon/bin/zygon launch --name process-demo
# Or: one real PTY, with stdout/stderr merged by the terminal device
modules/zygon/bin/zygon launch --name pty-demo --mode pty
# Or: an ordinary program, advertising only the available process controls
modules/zygon/bin/zygon launch --name ordinary -- /usr/bin/cat

# Session 3: inspect, then use the exact /requests incarnation printed there
modules/zygon/bin/zygon inspect
modules/zygon/bin/zygon invoke process-demo/requests INCARNATION demo.delay \
  '{"text":"hello from the terminal","seconds":1}'
modules/zygon/bin/zygon watch --cursor 0
```

`INCARNATION` is an explicit placeholder: copy the current incarnation from
`inspect`. The CLI never resolves an old command to a new process by name or PID.
Each CLI invocation registers a new requester. Retrying a CLI command is a new
user request; the protocol client offers correlation-based receipt lookup for
reconnect. Operation `process.stdin.write` accepts `{"bytesBase64":"aGVsbG8K"}`;
PTY resize accepts `{"rows":31,"cols":101}`. Pipe EOF is `process.stdin.eof`;
PTY VEOF is the distinct `process.pty.eof`. `process.interrupt` reports signal
submission; `process.stop` reports the observed exit. Ctrl-C on the owned launcher
stops its child with bounded escalation and process-group cleanup.

## Attach to a host that already exists

```sh
PYTHONPATH=modules/zygon python -m zygon.demo_child \
  --listen .nema/external-demo/host.sock

# Another terminal, after the external host has started:
modules/zygon/bin/zygon attach .nema/external-demo/host.sock
modules/zygon/bin/zygon inspect
```

Invoke the attached `/requests` surface using its current incarnation as above.
Ctrl-C in the attachment terminal detaches; the external host keeps running.
Reattachment uses the private continuity tokens in the same attachment runtime.
The host endpoint exposes only `demo.echo`, `demo.delay`, and `demo.read`. There
is no claim to historical stdout, arbitrary-process control, or a terminal
emulator tab identity. Stop the external host separately when finished.

## Browser session

```sh
# With the registry already running:
modules/zygon/bin/zygon browser
```

This prints a local demo URL and a private unpacked-extension directory. Launch
Chromium with a fresh profile and those exact paths:

```sh
chromium --user-data-dir="$PWD/.nema/zygon/manual-browser-profile" \
  --disable-extensions-except="$PWD/.nema/zygon/browser/extension" \
  --load-extension="$PWD/.nema/zygon/browser/extension" \
  --no-first-run 'PRINTED_DEMO_URL'
```

Inspect to see extension → tab → document. Invoke the document with
`browser.annotate '{"value":"addressed from the terminal"}'`, `browser.read`, or
`browser.replace '{"value":"new region text"}'`. The tab advertises
`browser.focus`; the extension advertises `browser.enumerate`. Annotation updates
the region's annotation attribute and tooltip; replace updates its visible text.
Navigating changes the document binding; the prior incarnation fails. Closing
the page ends its binding. Reopening registers the new document. Stopping the
bridge detaches without terminating Chromium. See [browser details](docs/browser.md).

## Retention, replay and boundaries

```sh
modules/zygon/bin/zygon export --cursor 0
modules/zygon/bin/zygon --runtime .nema/zygon/demos/PRINTED_RUN replay
```

Export is NDJSON using `nema.lab.record/v1`; cursor is registry ingestion order,
while each record's sequence belongs only to its source/incarnation. Parents
name known causal records. Raw capture retains base64 bytes, per-stream sequence,
monotonic observation time, and native control data separately from registration
and invocation transitions. A PTY has one merged stream. Text decoding is a
consumer choice and never replaces the bytes. Replay checks retained transitions
against the materialized snapshot without executing any host operation.

SQLite uses FULL synchronous transactions. A committed accepted invocation
precedes dispatch. No command is silently resent after a disconnect, restart or
uncertain send. First terminal result wins, including cancellation races. A
deadline is an operational timeout, not a lease or proof of host death.

The process spool defaults to 64 MiB of encoded records with an explicit capture
failure reserve; on exhaustion an owned child is stopped. Observer catch-up pages,
protocol frames, connections, workers and browser command queues are bounded.
Retained journals use disk and need operator retention management for extended
runs; this module is a bounded experiment, not a service manager or distributed
object system. Disk I/O may apply ordinary OS backpressure. No stdout/stderr
cross-stream chronology or exactly-once external effect is claimed.

## systemd example

`systemd-analyze --user verify modules/zygon/ops/nema-zygon.service` validates the
example. It points at `%h/nema-zygon`; adjust the two checkout paths if installing
elsewhere. Explicitly copy it to your user-unit directory and start it if wanted.
The example uses `KillMode=control-group`, restart-on-failure and a 10-second stop
deadline. Nothing installs/enables it automatically. It supervises the registry;
separately launched companions need their own unit/scope for full cgroup cleanup.

See [protocol](PROTOCOL.md), [process details](docs/process.md),
[Flix details](docs/flix.md), [acceptance evidence](docs/acceptance.md),
[integration](INTEGRATION.md), and [progress](PROGRESS.md).
