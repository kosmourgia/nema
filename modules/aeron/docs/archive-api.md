# Archive bridge: released API and observed behavior

Verified against the released `io.aeron:aeron-all:1.53.1` binary and source
artifact, using the guest's default OpenJDK 26.0.2.1. Source SHA-256:
`27af40c95139b945e3476b520eacc7cddb62f12917deccf6114529463e82e9a1`.
The module launcher pins and verifies the binary. Source URL:
[Maven Central released sources](https://repo.maven.apache.org/maven2/io/aeron/aeron-all/1.53.1/aeron-all-1.53.1-sources.jar).

`ArchiveBridge.java` supplies native contexts, ownership, descriptor callbacks,
retained frame validation, and buffer conversion. Flix callers own the application,
duty cycle, deadlines/cancellation, persistence obligations, and purge decisions.
These are opaque source bytes; no Nema ontology or clock is introduced here.

## Scope and coordinates

`openOwned(driverDirectory, storageDirectory, archiveId, segmentLength, syncLevel)`
owns an embedded Archive and its Archive clients. It **borrows** the existing
Media Driver. Closing the Archive does not close that driver. Use one laboratory
Archive per dedicated Media Driver: local IPC control streams 9100/9101 are scoped
to that driver's unique directory. A driver-directory ownership lock rejects a
second owned Archive before opening its control subscriptions, and the connected
client's native Archive ID must match the requested ID. Failed lock acquisitions
close their file descriptors, including overlapping locks inside the same JVM.
Recording/replay stream IDs remain caller
choices inside that directory. Replication is not exposed; its mandatory native
context URI uses loopback port zero rather than a fixed shared endpoint.

The Archive catalog and segments remain in `storageDirectory` after close. A
directory lock rejects concurrent owners. `nema-incarnation` stores a native
Archive ID plus a random UUID, forces the file, and forces its directory. Reopen
preserves the UUID. A preexisting catalog without this identity, or an identity
without its established catalog, is rejected. An interrupted initial creation
may therefore require explicit operator inspection; the wrapper never creates
new recording IDs under an old incarnation. Adoption is not silently invented. Recording identity is the pair
`(archive incarnation, recording ID)`, never the recording ID alone. Restore or
copy the identity file together with its catalog and segments. Copying a catalog
to create a *new independent* recording universe requires an explicit identity
policy outside this laboratory API.

Public operations:

| Operation | Meaning |
| --- | --- |
| `startRecording(channel, stream)` | Returns native recording subscription registration; a recording descriptor follows when a publication image exists. |
| `stopRecording(registration)` | Stops that recording subscription. |
| `findRecording(channelFragment, stream, session)` | Native last matching recording ID, or -1 while absent. |
| `descriptor(id)`, `list(firstId, count)` | Retained start/stop, initial term ID, term/segment/MTU sizes, native session/stream/channel/source. Listing has an explicit count bound. |
| `progress(id)` | `AeronArchive.getMaxRecordedPosition`: recorded byte coordinate for active or stopped recording. |
| `replay(id, start, end, replayStream, capacity)` | Finite half-open byte range, bounded staging queue, caller-driven polling. |
| `paddingOnly(id, from, to)` | Verifies an entire retained coordinate gap consists of complete PAD frames; DATA returns false. |
| `persistent(id, start, liveChannel, liveStream, replayStream, capacity)` | Released PersistentSubscription with explicit start and session-filtered live channel. |
| `safePurgePosition(id, satisfiedThrough, replayPin)` | Complete segment boundary no later than both supplied obligations and current recorded progress. |
| `purgeSegments(id, satisfiedThrough, replayPin)` | Front reclamation at that boundary; open child readers add conservative pins. |

Positions include Aeron frame headers, fragment occupancy, alignment, and term
padding. They are neither payload byte counts nor message counts. Replay ranges
must stay inside retained coordinates. The bridge walks source frame headers
from the segment's first retained position to reject offsets inside a frame or
inside a fragmented application message. Empty ranges are complete immediately.
Boundary inspection uses the observed 1.53.1 local `recordingId-basePosition.rec`
layout; the filename helper is package-private. Remote Archive range inspection
is not implemented.

`Replay.complete()` requires observing the requested end position and draining
staging. Premature EOS or disappearance of a previously connected replay sets
`failureReason()` and throws; it cannot masquerade as successful finite replay.
Any already received prefix remains a prefix, not a complete requested range.

When application ranges skip term padding, `paddingOnly` lets Flix establish
that the gap contains actual PAD frames. It validates the retained bounds and
real aligned frame boundaries, then refuses any DATA frame. A caller may cover
`[previousEnd, messageEnd)` only after satisfying the application's obligations
for the message and verifying `[previousEnd, messageStart)` this way. A missing
application message therefore cannot silently become acknowledged padding.

The bounded reader calls `ControlledFragmentAssembler` exactly once. Its callback
copies complete messages into owned `byte[]` values. A full queue returns ABORT
*before copying or enqueueing*. Aeron's assembler restores the message assembly
state for retry, so a later poll enqueues that message once. A consumer can drain
staging on another thread; one thread alone may poll each reader. No application
or UI callback runs on the Archive/driver conductor. `take()` is a nonblocking
queue read; Flix decides what to do when the downstream channel is full.

Replay uses a new replay publication session/stream. The bridge restores original
source session/stream/source identity from the retained descriptor. Coordinates,
term/header metadata, reserved value, and bytes come from the replay callback;
image correlation ID identifies the current replay image, not the historic image.
Per-fragment flags/header sequences and original image correlation IDs are not
reconstructed. The assembled message carries the available first-header metadata
and assembled end position. No original source timestamp is invented.

## PersistentSubscription: actual 1.53.1 source

The public [Persistent Subscriptions wiki](https://github.com/aeron-io/aeron/wiki/Persistent-Subscriptions)
describes IPC support and fallback from live into replay. The source artifact
confirms and the live checks exercise both. In released
`io/aeron/archive/client/PersistentSubscription.java`:

- Lines 60–76 describe the duty cycle, single-thread restriction, and assembly.
- Lines 92–96 instantiate its own image fragment assemblers.
- Lines 191–194 create an asynchronous instance; repeated polling progresses
  connection and replay/live state as well as delivering messages.
- Lines 240–250 show controlled polling. The supplied handler receives an
  assembled message, and its action applies to that entire message. The bridge
  passes a plain controlled handler; wrapping another assembler would be wrong.
- Lines 293–332 expose live/replay/failed states and failure reason. Listener
  events distinguish leaving live, joining live, and possibly recoverable errors.
- Line 1633 initializes `Context.startPosition` to `FROM_LIVE` (-2). This differs
  from the wiki's statement about a from-start default. The bridge always sets an
  explicit coordinate or `FROM_START` (-1)/`FROM_LIVE` (-2).
- The native live path chooses an image from its subscription. The bridge adds
  the recorded session ID to the live channel and verifies the live stream ID,
  preventing another publisher session from supplying wrongly attributed bytes.

The runnable IPC test first replays three historical records including a 7 KB
fragmented message, joins live, receives another record, then intentionally stops
polling an **untethered** live reader. One hundred 4 KB publications force it out
of its live window while the Archive keeps recording. Polling resumes, observes
`onLiveLeft`, replays the missing bytes, and rejoins live. All 104 application
messages match exactly and retain source session/stream attribution; no duplicates
appear. Observed counters: two live joins and one live leave. The short driver
untethered timeouts belong to this bounded failure-injection test, not an implicit
production delivery policy. For ordinary tethered IPC, a slow consumer instead
backpressures publication.

UDP PersistentSubscription and remote Archive operation are not tested by this
laboratory bridge. ReplayMerge is not substituted for unsupported cases.

## Progress, force policy, reclamation, and failure

`fileSyncLevel()` and `catalogSyncLevel()` report the configured native policy:
0 normal writes, 1 force data, 2 force data and metadata. Released
`RecordingWriter.onBlock` writes the block then invokes `FileChannel.force`
when the policy is nonzero, before its position advances. Catalog and directory
sync have their own native paths. `progress()` is still named recorded progress;
the wrapper does not relabel an observed counter as a universal `fsyncComplete`
or downstream sink acknowledgment.

`AeronArchive.segmentFileBasePosition(start, position, termLength, segmentLength)`
accounts for recordings beginning inside a term. The bridge uses it directly,
including metadata's effective segment length. It does not divide a position by
a guessed segment size. The caller supplies the highest contiguous satisfied
position and earliest external replay pin; an open bounded or persistent reader
also conservatively pins its entire initial range until closed. Only complete
front segments are purged. Native `truncateRecording` removes the other end and
is deliberately not used for front reclamation. See the general
[purging/truncation documentation](https://aeron.io/docs/aeron-archive/purging-and-truncation/).

The bridge has no durable sink obligations of its own: caller claims supplied to
purge must be backed by the application's durable state. It also cannot guarantee
bounded disk growth under indefinite sink failure. The Flix handoff example owns
the storage cap and stop/backpressure choice.

Control requests have a native three-second response timeout. Readers contain no
independent worker: `close()` cancels their native replay/subscription and clears
staging. The polling owner should be cooperatively stopped before scope teardown.
`openChildren()` and `leakedChildren()` expose child ownership; parent close
reclaims leaked readers. Native worker failures are retained in `failure()` and
subsequent operations fail with the original cause. Persistent transient error
text is distinct from its terminal `hasFailed()` state.

Archive close waits, with a three-second bound, for its native driver counter
removal before permitting immediate reopen with the same Archive ID. This fixed
an observed native asynchronous teardown race (`found existing archive for
archiveId=71001`). After *abrupt process death*, the native Archive mark-file
liveness window still applies; the crash test waits eleven seconds after proving
the child exited before opening retained storage with a fresh driver directory.

## Reproduction and evidence

```sh
modules/aeron/bin/aeron java nema.aeron.ArchiveChecks
```

All generated drivers, catalogs, segments, child logs, and recovery checkpoints
stay under the launcher's unique module `.nema/runs` directory. Aeron probes a
native UDP socket even for IPC, so the restricted tool sandbox required the
ordinary command approval for sockets; execution itself remains in the guest.

Observed 15 September 2026, exit 0:

```text
PASS same-storage/same-driver owner rejection; repeated lock failure has no fd leak
PASS recording/list/progress/binary/UTF-8/fragmentation/queue ABORT/ranges/cancel
PASS PersistentSubscription IPC replay/live/fall-behind/replay/live; joins=2 leaves=1
PASS multiple publication sessions retain distinct recordings and live attribution
PASS premature native replay stop reports failure rather than completion
PASS retained gap validation distinguishes PAD from missing DATA; gaps=6
PASS Archive restart/identity/retained replay/pins/front purge/pruned rejection; purged=6
PASS abrupt process halt(91)/Archive recovery/replay with syncLevel=2; power-loss claim excluded
PASS Archive checks
```

The producer fixture calls native Aeron directly. It does not publish through the
Flix or Transport publication wrapper. Replay is compared with its original
binary arrays. Recovery includes a real child JVM `Runtime.halt(91)` after observed
recorded progress, without close hooks; a new driver and Archive recover and
replay retained bytes and the same incarnation. This proves the tested process
crash/restart path, **not** every power-loss, filesystem, controller, or storage
failure. No remote durability, replication, consensus, or global ordering is
claimed.

The early-stop failure injection uses an independent native Archive control
client to stop the replay while its bounded queue is full; the reader subsequently
reports failure before the requested end. Lock testing performs forty rejected
same-storage/same-driver acquisitions and checks `/proc/self/fd` for growth. A
nonzero-start metadata check distinguishes the correct segment base (196608)
from naive byte division (131072). The full run also validates six actual term
padding gaps and rejects every application DATA range as padding.
An independent second publication session runs on the same stream during the
persistent test; its bytes replay from a distinct recording and never enter the
first session's replay/live reader. A second persistent reader is cancelled while
its initial replay staging queue is full.
That cancellation can leave a final native response with no listening client;
one run retained `ArchiveEvent: WARN - control response publication is not
connected` for a `ControlSession` already in `DONE`. The Archive preserved the
raw diagnostic in its worktree-local `archive-...-error.log` on reopen; the bounded
close/reopen/replay checks still passed. This warning is distinguished from a
terminal worker failure and is not discarded from retained runtime evidence.

One rerun during the interrupted guest session failed at native Media Driver
startup with `java.io.IOException: Input/output error (msync with parameter
MS_SYNC failed)` in `MediaDriver$Context.conclude`. The filesystem had 83 GB free;
the cause was not established. Subsequent full runs passed without a code change
to driver startup. This observation is retained as an environmental diagnostic,
not attributed to Archive persistence or presented as a passing test.
