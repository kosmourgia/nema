# Acceptance and capability evidence — 15 September 2026

## Release status

**Final disk-backed validation is blocked by the VM's virtual-disk failure.**
The implementation and functional protocol checks are available on the dedicated
`codex/flix-aeron` branch. No main/Vivarium/zygon integration or merge occurred.

Start: `ac48adfeeff37953d9d1688b54d3339de217e69b`.
First runnable checkpoint: `7ab0ae7`.
Recovery integration checkpoint: `6f4c60e` (pushed).
Original worktree: `/home/elkeegano.guest/nema/.nema/worktrees/aeron`.
Rescue clone: `/tmp/nema-aeron-rescue.Fjv3dL/repo`, same branch and preserved history.
All implementation files are under `modules/aeron`.

The original ext4 run reached 40 received/recorded/sink-committed records at
position `183680` and executed `halt(73)` before publishing the acknowledgment.
The next process failed with:

```text
java.io.IOException: Input/output error (msync with parameter MS_SYNC failed)
/dev/vda2 on / type ext4 (rw,relatime,emergency_ro)
I/O error, dev vda ... op 0x1:(WRITE)
Aborting journal on device vda2-8.
JBD2: I/O error when updating journal superblock for vda2-8.
EXT4-fs (vda2): I/O error while writing superblock
```

Read-only `findmnt`, `df`, and `dmesg` confirmed the storage condition; 79 GiB was
free. Source, Git history, and the handoff's readable runtime data were copied to
separate tmpfs before further work. No remount, fsck, VM restart, or disk repair
was attempted. The final source must be rerun on repaired disk storage before
calling the durability release gate complete. A JVM `force` on tmpfs does not
establish disk durability, and earlier process-crash tests do not prove survival
of every storage/power failure.

## Versions and reproducibility

| Component | Observed/pinned value |
| --- | --- |
| Guest | Arch Linux ARM; Linux 7.2.4-1-aarch64-ARCH |
| Default Java | OpenJDK 26.0.2.1, build 26.0.2.1 |
| Flix | 0.75.3; no additional JDK |
| Flix jar SHA-256 | `bf123cdb6494d6e0cbff6399bf185314d332bbe97bfd776e4abc03a5d39dd954` |
| Aeron | released `io.aeron:aeron-all:1.53.1` |
| Aeron binary SHA-256 | `504ac8bc74ca63783a921da8e7ee96985b8422dd7cae003b9f629e75e97ae333` |
| Aeron source SHA-256 | `27af40c95139b945e3476b520eacc7cddb62f12917deccf6114529463e82e9a1` |

`scripts/inspect-release.sh` retrieves and verifies released source. Detailed
source/API evidence is in [transport-api.md](transport-api.md) and
[archive-api.md](archive-api.md), including the released PersistentSubscription
default that differs from wiki prose. [flix-0.75.3-diagnostics.txt](flix-0.75.3-diagnostics.txt)
contains exact compiler/probe output; `scripts/probes.sh` regenerates it.

## Capability matrix

| Capability | Status and evidence |
| --- | --- |
| Real Flix API, fixture/real effect interpretations | Supported; same `Flow.roundTrip` runs pure and live IPC. |
| Nominal coordinates and resource-scope effect limits | Supported; compiler rejects StreamId as Position and an unhandled custom effect crossing a native scope. Old `opaque type` syntax is rejected; private native constructors back nominal handles. |
| Client/driver ownership, repeated close, worker failures | Supported; borrowed driver survives client closure; 8 cycles return resource count to zero; failed worker joins retain mappings until retry. |
| IPC, UDP, fanout, binary, UTF-8, fragmentation, sessions | Supported; live native suite plus independent Java↔Flix JVMs, distinct drivers, dynamic UDP ports, 16 KiB binary/UTF-8 and two publisher sessions. |
| Bounded queue, slow consumer, controlled ABORT | Supported; capacity-one retries while full, exactly one later delivery; Flix worker capacity-two test checks ordered delivery of six messages. |
| Flix CSP region Stop and retained worker failure | Supported; worker-local handler, bounded demand/reply, idle/pending cancellation and failure propagation. |
| Offer outcomes and cancellable retry | Supported; -1/-2/-4 observed live, all -1…-5 mapped against released source; actual administrative/exhaustion native events not forced. Flix retry cancellation under actual backpressure passes. |
| Recording, list, metadata, recorded progress, sync policy | Supported; native and Flix handoff exercise operations; policy is exposed separately from progress. |
| Bounded replay, fragments, invalid/pruned range, early stop | Supported; frame-boundary checks, exact bytes/attribution, native stopAllReplays injection fails incomplete replay. Flix cancels after first fragment. |
| Archive restart and retained replay | Supported in original native checks; final functional repeats use tmpfs. Disk-backed release rerun blocked. |
| PersistentSubscription IPC and fall-behind recovery | Supported in 1.53.1; 104 exact messages, joins=2/leaves=1; multiple-session isolation. No double assembler. |
| Durable sink mechanism | Implemented forced journal + directory policy; 95 original assertions include exact retry/conflict/torn suffix/corruption/reopen/halt73. Final disk-backed gate blocked. |
| Contiguous ack, multiple obligations, replay pin | Supported pure model and real protocol example; another incarnation cannot fill a hole; minimum required-sink prefix limits reclamation. |
| Commit→crash→ack recovery→fresh late subscriber→purge→lookup | Full functional two-Flix-process pass on tmpfs. Original ext4 prepare passed; ext4 recovery blocked by disk errors. |
| Front segment reclaim, storage-cap stop | Functional pass; metadata determines boundaries, actual PAD gaps checked, explicit pin blocks purge. Single controlled publisher waits for recording before next quota observation. |
| MDC | Optional bounded native loopback probe passed; not required by default verify. |
| Multicast | Untested; no network support claim. MDC supplies the required optional transport probe. |
| Borrowed-buffer API, remote Archive adoption/range inspection | Unsupported by this wrapper; default messages are owned copies and range validation uses local retained files. |
| Replication, ReplayMerge, Cluster/consensus, global order | Not implemented or claimed. |
| Durable JVM/Flix continuation restoration | Unsupported; explicit retained state reconstructs a fresh protocol execution. |

## Commands and actual results

Original guest before/around the disk fault:

- `bin/aeron test`: initial 5 Flix API/model tests pass; first separate worker
  suite passes 5 tests.
- `bin/aeron java nema.aeron.TransportChecks mdc`: passes IPC/fanout/UDP,
  fragmentation, queue, ownership, cancellation, driver failure, MDC.
- `bin/aeron java nema.aeron.ArchiveChecks`: passes recording/replay,
  persistent fallback, range checks, six PAD gaps, six-segment purge, and a real
  child `halt(91)` followed by retained-storage restart.
- `bin/aeron java nema.aeron.SinkChecks`: 95 assertions pass, including child
  `halt(73)` immediately after forced commit and before notification.
- `scripts/udp-interop.sh`: passes; original run `interop.mvSkJe`.
- Root `./bin/nema check` and both offline fixture demonstrations pass. Initial
  root Python run had two failures caused by socket `EPERM` in the subagent's
  sandbox; its exact daemon diagnostic was retained locally.

Rescue functional checks (separate tmpfs, explicitly not disk durability):

- `bin/aeron test`: **13 passed, 0 failed**.
- Root `./bin/nema test`: **42 Flix and 11 Python passed** in unrestricted rescue.
- `scripts/probes.sh`: abandoned continuation prints `result=99; closeCalls=0`;
  all three expected compile rejections are verified.
- `scripts/persistence-handoff.sh`: 40 messages; committed/reemitted end
  `174592`; fresh late subscriber satisfied; two segments purged; new catalog
  start `131072`; old bytes resolved from external sink locator.
- `ReviewChecks`: second Archive sharing control streams rejected; missing
  established catalog rejected; restored identity/catalog pair reopens.

An initial combined Archive run failed a health assertion after intentional
cancellation. That assertion did not print the stored cause; an immediate rerun
passed, so its exact cause is not established. The assertion now includes the
cause. Native WARN is separately retained in a bounded diagnostic queue, while
ERROR/FATAL/unknown exceptions remain failures, following released Aeron semantics.
The warning regression initially assumed its injected warning was the queue's
first entry; it now inspects all retained diagnostics, allowing native teardown
warnings that arrived earlier. These failures are not hidden as passing runs.

Final combined `modules/aeron/scripts/verify.sh` completed with exit status 0
and `AERON_VERIFY_PASS`. Report directory: `.nema/evidence/verify.9euyXl`.
All eight stages passed: independent build, 13 Flix tests, TransportChecks,
ArchiveChecks, ReviewChecks, 95 SinkChecks assertions, independent UDP processes,
and the two-process persistence handoff. The handoff used `handoff.yeP9mB`;
a further recovery invocation against its already-purged storage also passed,
reconstructed end `174592`, and correctly purged zero additional segments while
preserving catalog start `131072` and the original sink locator.

Raw reports remain worktree-local and uncommitted. The final combined log is
`.nema/evidence/rescue-full-verify-complete.log`; repeat recovery is
`.nema/evidence/rescue-recovery-repeat.log`. These are tmpfs functional results,
not completion of the blocked disk-backed gate.

## Diagnostic measurements

These small runs are regression aids, not benchmark comparisons. Original native
samples showed about 0.020–0.040 of one CPU core over a 500 ms idle window and
5,000 × 256-byte messages in roughly 0.025–0.050 seconds. A rescue run while other
checks were active showed 0.080 cores and 0.056 seconds. The final combined run
measured 0.079 cores and 0.036 seconds, with queue high water 64/64 and 13
controlled ABORT observations. These scheduler-sensitive samples vary across runs.
The idle policy sleeps/parks about 1 ms. No NATS comparison, latency percentile,
production capacity, or unbounded-outage disk claim is made.

## Resume after disk repair

Create a dedicated worktree from the pushed `codex/flix-aeron`, then run:

```sh
modules/aeron/scripts/verify.sh
modules/aeron/scripts/probes.sh
./bin/nema test
```

Inspect retained module `.nema/evidence` reports and update module `PROGRESS.md`.
Do not merge or edit root integration files as part of this validation. The
`/tmp` rescue copy is volatile and may disappear on VM restart; source is pushed,
while runtime traces remain deliberately uncommitted.
