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

## Slice 2: Archive, workers, protocol recovery (15 September 2026)

Implemented Flix Archive descriptors, typed recording keys/ranges, bounded replay,
PersistentSubscription operations, sink API, and the Flix handoff program. The
worker owns native polling inside a Flix region; Stop wakes it before exit.
Native scopes admit primitive effects only. Compiler probes retain the observed
abandoned-continuation behavior and verify rejected scope escape/coordinate mixup.

Independent native suites passed Transport IPC/UDP/fanout/fragmentation/slow
consumer/queue/cancellation/driver failure/repeated closure; Archive recording,
restart, IPC replay→live→fall-behind→replay→live, early replay termination failure,
padding, session isolation, pins, and front purge; sink 95 assertions including
actual child halt after forced commit. UDP Flix↔independent Java processes passed
both directions with 16 KiB opaque/UTF-8 data and two publisher sessions. MDC passed.

Review fixes include worker-failure cleanup, retaining mappings on failed worker
join, premature replay EOS rejection, Archive control ownership locks, missing
catalog/incarnation rejection, primitive-only scope callbacks, full JSON escaping,
fresh build staging, cap accounting serialized with recording, and a genuinely
new subscription for the late-ack waiter.

## Environmental blocker and rescue

During the disk-backed handoff, Flix published/received/recorded/committed 40
messages through byte position 183680 and executed the intended `halt(73)` before
notification. Recovery failed in native mapped-file synchronization:

```text
java.io.IOException: Input/output error (msync with parameter MS_SYNC failed)
/dev/vda2 on / type ext4 (rw,relatime,emergency_ro)
I/O error, dev vda ... (WRITE)
Aborting journal on device vda2-8.
JBD2: I/O error when updating journal superblock for vda2-8.
EXT4-fs (vda2): I/O error while writing superblock
```

79 GiB remained free. This is a concrete VM virtual-disk failure, not ordinary
backpressure. No remount, filesystem repair, disk deletion, or root workstream
mutation was attempted. All original-disk activity stopped.

Rescued ALL module source including untracked files and the existing Git history
to `/tmp/nema-aeron-rescue.Fjv3dL/repo`, on **the same `codex/flix-aeron` branch**.
The original hidden worktree remains at
`/home/elkeegano.guest/nema/.nema/worktrees/aeron`; its last commit is `7ab0ae7`.
The rescue clone's source is the newer integration. Git integrity checks passed.
`/tmp` is a separate tmpfs: its functional checks do not establish disk durability.

Rescue validation so far:

- Module Flix: **13 passed, 0 failed**, including actual retry cancellation under
  backpressure, cancellation after the first replay fragment, and sink waiter
  deadline/cancellation while a retained hole blocks progress.
- Existing root `./bin/nema test`: **42 Flix + 11 Python passed**. Earlier isolated
  subagent run had two Python socket EPERM failures; unrestricted rescue rerun
  resolves those. Root check and both offline fixture demonstrations also passed.
- Two Flix process handoff on tmpfs: committed end 174592; recovered/reemitted ack;
  fresh late subscriber satisfied; two front segments purged to start 131072;
  original bytes remain findable by durable sink locator. **Protocol proof only**
  on tmpfs, not a repaired disk-backed durability gate.
- Reviewed control/identity regression passes, including refusing a missing
  established catalog. Compiler capability probes pass with exact diagnostics.

Full final module verification is being recorded under module `.nema/evidence`.
Release remains **blocked for a final disk-backed rerun**. After the VM storage is
repaired, resume from the pushed branch in a dedicated worktree and run
`modules/aeron/scripts/verify.sh`, `modules/aeron/scripts/probes.sh`, and existing
`./bin/nema test`. Do not infer disk repair from tmpfs results. See
`docs/acceptance.md` for the final supported/unsupported/untested matrix.
