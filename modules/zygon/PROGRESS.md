# Zygon progress

Worktree `/home/elkeegano.guest/nema-zygon`, branch `codex/zygons`, baseline ac48adf.
All changes module-local; no Aeron or Vivarium dependency. Root handoff, progress,
IPC, capture/process code and Flix probes read before implementation.

## Recovery — 15 September 2026

A guest reboot cleared the initial `/tmp/nema-zygon` worktree before the first
commit. Its code is being reconstructed from retained session/tool history in
this persistent sibling worktree. Commit checkpoints now accompany slices.

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

Flix participant implementation existed but had not yet compiled. Restore and
complete it, run combined process/browser demos, adversarial/replay checks,
systemd verification, documentation/evidence, then push feature branch only.
