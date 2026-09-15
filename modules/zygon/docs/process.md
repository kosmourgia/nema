# Process companions

An owned companion starts one process group and captures its bytes from birth.
An attached companion connects to a cooperative Unix endpoint that an external
host already exposes. The attached companion neither acquires the host's old
stdout nor receives an OS signal/kill capability.

## Run the experiment

From the checkout root, use the module's independent launcher:

```sh
modules/zygon/bin/zygon --runtime .nema/zygon-local serve
```

In a second terminal:

```sh
modules/zygon/bin/zygon --runtime .nema/zygon-local launch --name python-demo
```

The default is the small Python child in `zygon.demo_child`. It writes ordinary
stdout and stderr, including partial lines and invalid UTF-8. Requests and
results travel over a separate inherited socket pair. The companion prints
the registered host and two child surfaces: `python-demo/requests` and
`python-demo/output`.

In an independent terminal:

```sh
modules/zygon/bin/zygon --runtime .nema/zygon-local inspect
modules/zygon/bin/zygon --runtime .nema/zygon-local watch
```

Copy the requests surface's exact current incarnation from `inspect`. This is
an intentional stale-target check; the CLI never finds a replacement by PID.
Substitute it for `REQUEST_SURFACE_INCARNATION` below:

```sh
modules/zygon/bin/zygon --runtime .nema/zygon-local invoke \
  python-demo/requests REQUEST_SURFACE_INCARNATION demo.delay \
  '{"seconds":1,"text":"the original answer"}' --correlation first-request
```

While this waits, an independent client can invoke `demo.echo` on the same
surface. Unsolicited `unsolicited:tick` output continues, and both correlations
resolve separately. `demo.read` returns the child's last requested value.
These are explicit demo-native operations, not universal process operations.

Use the current **host** incarnation for the owned controls:

```sh
modules/zygon/bin/zygon --runtime .nema/zygon-local invoke \
  python-demo HOST_INCARNATION process.stdin.write '{"bytesBase64":"aGVsbG8K"}'
modules/zygon/bin/zygon --runtime .nema/zygon-local invoke \
  python-demo HOST_INCARNATION process.stdin.eof '{}'
```

The write above submits `hello\n`. Its receipt means bytes reached the OS write
path; `consumedByChild` stays unknown. Raw write intentions and observed write
completion are separate records. A failed write is never silently retried.

An ordinary command need not implement the cooperative socket protocol:

```sh
modules/zygon/bin/zygon --runtime .nema/zygon-local launch --name ordinary \
  -- python -c 'import sys; print("ordinary process"); sys.exit(7)'
```

Only justified process controls are advertised for this command; its requests
surface has no invented `demo.*` capabilities. The standalone owned adapter
also exposes exit status directly:

```sh
PYTHONPATH=modules/zygon python -m zygon.processes \
  --socket .nema/zygon-local/zygon.sock \
  --runtime-dir .nema/zygon-local/standalone --id standalone \
  -- python -c 'raise SystemExit(7)'
```

## PTY mode

```sh
modules/zygon/bin/zygon --runtime .nema/zygon-local launch --name pty-demo --mode pty
modules/zygon/bin/zygon --runtime .nema/zygon-local invoke \
  pty-demo PTY_HOST_INCARNATION process.pty.resize '{"rows":37,"cols":109}'
modules/zygon/bin/zygon --runtime .nema/zygon-local invoke \
  pty-demo PTY_HOST_INCARNATION process.interrupt '{}'
modules/zygon/bin/zygon --runtime .nema/zygon-local invoke \
  pty-demo PTY_HOST_INCARNATION process.stop '{}'
```

The companion opens a PTY, creates a new session and controlling terminal, and
resizes it with `TIOCSWINSZ`. Output is a single `pty` stream. Terminal line
discipline, echo and CRLF translation remain observable terminal behavior;
stdout/stderr separation cannot be recovered. No terminal-emulator tab,
window location or focus is inferred. `process.pty.eof` submits the conventional
VEOF byte (`0x04`); unlike closing a pipe, its effect depends on terminal mode
and buffered input. A PTY cannot independently close only its stdin stream.

Interrupt submits SIGINT to the owned process group. The demo child catches
it and emits an observation; another ordinary program may exit. Stop first
requests cooperative shutdown (or closes ordinary pipe stdin), then uses
SIGTERM and SIGKILL after bounded grace periods. The invocation reports the
observed exit code, not merely that a signal was sent.

## Attach and detach

Start an external cooperative host in its own terminal:

```sh
PYTHONPATH=modules/zygon python -m zygon.demo_child \
  --listen .nema/zygon-local/external/host.sock
```

Attach a separate companion:

```sh
modules/zygon/bin/zygon --runtime .nema/zygon-local attach \
  .nema/zygon-local/external/host.sock
```

Inspect and invoke its advertised `demo.echo`, `demo.delay` and `demo.read`
operations using the printed host/surface IDs. The host supplies its identity
and incarnation through an explicit `hello` exchange; the PID and Linux start
ticks are retained as native metadata. They are never invocation destinations.
The output surface means cooperative observations available since attachment,
not historical stdout capture.

Ctrl-C in the **attachment terminal** detaches. The host keeps running and
the registry retains active bindings with presence `unknown`. Repeat the
attach command to establish the same live host incarnation again. Explicit
`unregister` means a binding ended; it is deliberately not used for detach.
A lost cooperative endpoint also makes the host and both child surfaces
unknown, and pending invocations settle `outcome-unknown`.

Stop the external fixture in its original terminal with SIGTERM when finished.
It writes a private identity file next to its socket. Restarting that endpoint
with the same private state preserves the host ID and creates a fresh
incarnation. A different private state produces a different stable ID. The
companion keeps registry resume tokens in its own private runtime directory.

## Capture and retained state

`export --cursor 0` exports retained registry records through the same bounded
cursor path as `watch`. Process raw spools are additionally retained in the
companion runtime directory. Each SQLite `records` row contains a
`nema.lab.record/v1` object. Original byte chunks use base64 independently of
any UTF-8 or line projection. Raw control chunks also preserve duplicate
replies, malformed bytes and incomplete final frames.

`seq` is producer-local, `streamSeq` is local to one stream, and monotonic
timestamps are observations on this machine. Spool/registry cursor order is
ingestion order, not a total causal ordering between stdout and stderr.
Write-intention records are explicitly referenced by write-completion records;
native cooperative bytes preserve invocation IDs and native correlation.
Publisher records include their original `captureRecordId` and sequence.

Each append uses SQLite `synchronous=FULL`. Capture chunks are at most 16 KiB.
The default spool ceiling is 64 MiB of encoded records and 100,000 rows, plus
one reserved small failure record and SQLite page overhead. Capacity exhaustion
records the unavailable read, marks later bytes unknown, and stops the owned
process. An attached companion instead closes its cooperative connection and
records `detach-attached-companion`; the external host remains alive. If
storage itself fails, the failure remains visible in memory and
the standalone launcher reports it on stderr with exit status 74; persistent
storage cannot be promised when the filesystem rejects writes.

Publishing pages the spool independently of child reads and control. A stalled
observer has no in-memory capture backlog. Registry transport loss preserves
the local spool; `OwnedProcess.reconnect(new_dedicated_client)` re-registers
with saved tokens and publishes from its cursor, without resending native
commands. Capture IDs permit recognizing an observation whose receipt was
lost; publication is not an exactly-once transport promise.

## Cleanup and operating limits

The companion accounts for EOF, startup errors, nonzero exit, ordinary
descendants retaining pipes, cooperative shutdown, interrupt and escalation.
The direct Linux child has `PR_SET_PDEATHSIG=SIGKILL`, so an abrupt companion
death kills it. This flag does not propagate to every descendant. Normal
cleanup signals the owned process group; descendants that deliberately create
new sessions can escape that group. If those need lifetime containment, launch
the owned companion itself inside an explicitly chosen systemd user service
with `KillMode=control-group`. The registry service alone does not contain
companions launched from unrelated terminals. No service is enabled by this
module automatically.

This implementation targets the Linux guest. It is neither memory cloning nor
checkpoint migration, and cannot reconstruct a live process from retained
bytes. Observation disk writes are synchronous and can delay the event loop
under a slow filesystem; stalled network observers do not sit on that path.
Disk space should be budgeted across multiple companion incarnations because
old spools remain retained for inspection until an operator archives them.

## Verification

```sh
PYTHONPATH=modules/zygon python -m unittest discover \
  -s modules/zygon/tests -p test_process.py -v
```

The real-process suite covers pipe/PTY raw bytes, partial/invalid UTF-8,
interleaved requests, observations and cancellation, duplicate terminal replies,
partial native frames, EOF, exit 7, startup failure, group descendant cleanup,
SIGKILL escalation, direct-child cleanup after abrupt companion death, bounded
capture, stalled publication and recovery. Actual Unix registry/endpoint tests
cover detach/reattach, fresh incarnations on external restart, endpoint-loss
uncertainty and owned registry reconnect. The helper registry in the remaining
OS tests isolates process behavior; it does not substitute for those actual
socket integration tests.
