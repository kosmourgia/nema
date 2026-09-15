# Zygon progress

## Delivery boundary

Implemented and verified the bounded experiment, including actual Flix requests
to both an owned Python process and a real Chromium MV3 document. Both views
resolve from the same retained terminal record. Module-local launcher, schema,
fixtures, tests, operator documentation, replay/export, integration note and
verified systemd example are included. No root runtime/build/progress, Vivarium,
fork bench, or Aeron changes; no merge to main.

Final verification in writable recovery checkout:

```text
modules/zygon/bin/zygon check                  exit 0
modules/zygon/bin/zygon test                   54 Python + 4 Flix passed
modules/zygon/bin/zygon demo                   exit 0 (actual Flix, 9 observations during wait)
modules/zygon/bin/zygon demo-browser --with-flix exit 0 (real Chromium, actual Flix)
JAVA_TOOL_OPTIONS=-Xmx1200m ./bin/nema check   exit 0
JAVA_TOOL_OPTIONS=-Xmx1200m ./bin/nema test    42 Flix + 11 Python passed
systemd-analyze --user verify modules/zygon/ops/nema-zygon.service exit 0
```

The 54 Python tests comprise 24 protocol, 19 process, 9 browser protocol and
2 replay checks. Last combined test output: 54 tests in 4.918s, Flix4 in81.6ms.
Three additional real Flix JVM regression tests passed in 15.548s, covering
observation during a pending result, retained provider-loss uncertainty, and
stale-target rejection without a stranded CSP worker.
Test coverage includes first terminal wins, stale incarnation/PID reuse, crash
after accepted, owner authentication, provider close from its own callback,
bounded stalled consumers, partial/malformed native bytes, and attached capture
failure detaching without killing the host.

See `docs/acceptance.md` for selected actual output and terminal record IDs;
`fixtures/observed/` contains reviewed controlled-demo records. Runtime captures
and browser profiles are ignored. `docs/process.md`, `docs/browser.md` and
`docs/flix.md` describe concrete limitations and copyable commands.

Operational handoff: the latest source is on pushed `codex/zygons`. Guest storage
repair is needed before updating the sibling worktree, whose filesystem became
read-only. The recovery checkout is tmpfs and can disappear on reboot; the remote
feature branch is the durable delivery. Do not reset another session's checkout.

Worktree `/home/elkeegano.guest/nema-zygon`, branch `codex/zygons`, baseline ac48adf.
All changes module-local; no Aeron or Vivarium dependency. Root handoff, progress,
IPC, capture/process code and Flix probes read before implementation.

## Recovery — 15 September 2026

A guest reboot cleared the initial `/tmp/nema-zygon` worktree before the first
commit. Its code is being reconstructed from retained session/tool history in
this persistent sibling worktree. Commit checkpoints now accompany slices.

The sibling was restored and checkpointed through e828734, then `/dev/vda2`
entered `emergency_ro` after write I/O errors and an aborted ext4 journal.
Kernel observations: `I/O error, dev vda ... WRITE`, `Aborting journal on device
vda2-8`, `JBD2: I/O error when updating journal superblock`. Disk usage 17%, 79G
available. No filesystem repair/remount attempted. All readable source and
commits were salvaged to `/tmp/nema-zygon-recovery.elwh3D/checkout` (tmpfs) for a
remote checkpoint and remaining independent validation. The original checkout
and sibling have not been reset, switched, or overwritten.

At an intermediate reconstruction checkpoint: all 51 Python tests passed in 5.664s (23 adversarial,
17 process, 9 browser, 2 replay). Four actual Flix tests compiled and passed;
actual Flix process/page invocations had not yet completed combined validation.
Restored no-Flix process demo also passed with offline replay at cursor116.

Before reboot, observed: 22 protocol tests passing (0.785s), 14 process lifecycle
tests passing (3.023s), real Chromium 153.0.8010.36 MV3 demo passing extension/tab/
document registration, DOM action, navigation, closure/reopen, worker restart,
transport loss/reconnect, no pending disappearance. Process demo passed owned
pipes, invalid/partial byte capture, interleaved requests, PTY, restart, attach/
detach. These observations must be rerun against reconstructed source.

Existing repository verification before reboot: `./bin/nema check` exit 0;
`./bin/nema test` 42 Flix + 11 Python passed. Sandbox socket EPERM was resolved by
running integration tests with appropriate access. Chromium installation needed
a package-index refresh after old version 152 package returned HTTP 404.

Flix participant implementation initially existed but had not yet compiled.
The delivery boundary above records the subsequently completed implementation
and combined process/browser validation.
