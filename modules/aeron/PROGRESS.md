# Aeron laboratory progress

Starting SHA: `ac48adfeeff37953d9d1688b54d3339de217e69b`.
Branch: `codex/flix-aeron`; isolated worktree under `.nema/worktrees/aeron`.
Scope: `modules/aeron/**` only. Independent of Vivarium and zygon.

## In progress

- Read repository handoff, architecture, progress, configuration, and Flix probes.
- Runtime observed: Flix 0.75.3; default OpenJDK 26.0.2.1; native Arch ARM.
- Investigating released Aeron 1.53.1 source and building independent module.
- Parallel ownership: transport bridge, Archive bridge, main Flix integration.
- Sibling worktree relocated into writable ignored `.nema/worktrees/aeron`.

## Slice 1: actual Flix transport (15 September 2026)

`modules/aeron/bin/aeron test`: **5 passed, 0 failed**, including the same
effectful round-trip program under a deterministic fixture and a real IPC
handler, nominal offer outcomes, contiguous multi-sink retention and replay
pins, nonzero segment origin, and storage-cap stop policy. IPC scope closed
with zero tracked handles. Flix compilation runs from generated copies under
module `.nema/flix`; compiler source discovery does not follow directory
symlinks. External jars require `url:` manifest entries and `lib/external`.

The worktree and uncommitted source survived the reported session interruption;
verified the branch and worktree registration before resuming. No root files
or other workstreams changed here.

Parallel Java bridge suites have passed their initial live runs; independent
review found failure-path cleanup and premature replay completion issues.
Those fixes and final reruns are still in progress. Initial sink suite reports
95 assertions including a child halt after forced commit before notification.

Next: Flix Archive API, actual acknowledgment recovery/reclamation example,
CSP worker Stop protocol, independent-process Flix/Java UDP, final gates.
