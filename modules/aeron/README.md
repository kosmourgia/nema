# Flix Aeron laboratory

Independent Aeron Transport and Archive wrapper, with Flix effects, polling,
resource scopes, CSP workers, and an application-level persisted-through example.
Application bytes stay opaque. Aeron positions are byte coordinates, not Nema
identities, message counts, semantic ticks, or a total order across sessions.

**Release gate remains blocked by the guest disk.** During final validation,
`/dev/vda2` entered ext4 `emergency_ro` after write I/O errors and an aborted
journal. Source was rescued to a separate `/tmp` clone on `codex/flix-aeron`.
Transport and protocol checks run there, but tmpfs cannot establish disk
durability. See [acceptance.md](docs/acceptance.md) and [PROGRESS.md](PROGRESS.md).

## Run independently

From this branch's repository root:

```sh
modules/aeron/bin/aeron build
modules/aeron/bin/aeron test
modules/aeron/bin/aeron run --entrypoint Nema.Aeron.Examples.main
modules/aeron/scripts/udp-interop.sh
modules/aeron/scripts/persistence-handoff.sh
modules/aeron/scripts/verify.sh
# Optional bounded UDP multi-destination probe:
modules/aeron/bin/aeron java nema.aeron.TransportChecks mdc
```

The launcher uses native guest Java, `javac`, `jar`, `curl`, `sha256sum`, and
Flix 0.75.3 at `/usr/share/java/flix/flix.jar` (override `NEMA_FLIX_JAR` if needed).
It downloads and SHA-256-checks released `io.aeron:aeron-all:1.53.1`, builds the
Java bridge, and compiles actual Flix. Observed default runtime: OpenJDK 26.0.2.1,
Arch Linux ARM. No extra JDK is installed. Module `flix.toml` is independent of
the root manifest. Java 26's observed Agrona access flags are in the launcher.

Every invocation gets a fresh `.nema/builds/build.*` directory, so removed
sources/classes cannot survive in build staging. Runtime data goes under module
`.nema/runs/`; source jars and inspected released classes go under `.nema/deps/`
and `.nema/release-source/`. `NEMA_AERON_RUN` can select an explicit run directory.
IPC drivers and Archives use unique local directories. UDP fixtures ask the OS
for port zero and exchange the resolved endpoint through local readiness files.
The launcher retains evidence; repeated laboratory runs deliberately consume
space until an operator removes their no-longer-needed run directories.

## API walkthrough

Read [Aeron.flix](src/Nema/Aeron.flix) for the low-level API and
[Flow.flix](src/Nema/Aeron/Flow.flix) for the entire small effect example.
[Examples.flix](src/Nema/Aeron/Examples.flix) runs the same `Flow.roundTrip`
program under a scripted pure handler and a real IPC handler.

```flix
let cancel = Aeron.cancelToken();
let deadline = Aeron.deadlineAfterMillis(5000i64);
Flow.real(publication, subscription, cancel, deadline,
    _ -> Flow.roundTrip(String.toBytes("hello λ")))
```

Create the handles using nested `withDriver`, `withClient`, `withSubscription`,
and `withPublication` scopes, as in `Examples.ipcRoundTrip`. Low-level
`open*`/`close*`, `tryPublish`, `poll`, and `take` let a caller own its duty cycle.
`publish` and `receive` add cooperative cancellation and absolute deadlines.

`ChannelUri`, `StreamId`, `SessionId`, `Position`, `ArchiveId`, `RecordingId`,
`ArchiveIdentity`, `RecordingKey`, and `RecordingRange` are distinct nominal
types. A recording key includes its retained Archive identity/incarnation.
Integer coordinates cross Java interop only after explicit unwrapping.
Handles wrap Java objects with private acquisition constructors; they are not
integer table indexes. Flix does not enforce affine ownership: a returned handle
is closed after its enclosing scope, and subsequent operations report closure.

Publication outcomes remain distinct: `Accepted(Position)`, `Backpressured`,
`NotConnected`, `AdministrativeRetry`, `Closed`, `PositionExhausted`, and a
retained `UnknownNative(code)` for unexpected future codes. Accepted is a local
publication-log append, not an Archive or durable-sink acknowledgment. Native
errors preserve their class/message in `Failure.Native`; timeout and cooperative
Stop are `Failure.Deadline` and `Failure.Cancelled`.

`Message` contains an immutable vector of owned bytes and `NativeHeader`:
session, stream, start/end, source identity, term ID/offset, flags, reserved value,
and current image correlation ID. The Java callback copies a complete assembled
message before returning. No `DirectBuffer` or borrowed header escapes. There is
no public borrowed-buffer fast path. One Flix effect operation handles a whole
message; the retrying publisher prepares its byte array once per message.

## Ownership and backpressure

Closing a client never closes its borrowed Media Driver. Owned-driver scope
closes only the driver it launched. Explicit child closes are preferred; parent
close reclaims and reports unclosed children. Worker exceptions and interrupted
joins remain inspectable. A still-running worker keeps its native mappings until
a later successful join/close; cleanup does not unmap under it.

Native resource scope callbacks accept primitive effects only: `IO`, or the
explicit `*Csp` variants with `{IO, Chan, NonDet}`. Install custom handlers inside
the scope. A real Flix probe showed an outer handler can discard a continuation
and skip code after the body, including Java exception cleanup. The public scope
signatures reject that composition. See [probes](probes) and
`scripts/probes.sh`. No continuation or Java resource is serialized.

Each subscription has one polling owner. A full bounded staging queue returns
Aeron controlled `ABORT` before enqueueing, leaving the last message available
for exactly one later enqueue. No driver/conductor calls arbitrary Flix or UI
callbacks. [Workers.flix](src/Nema/Aeron/Workers.flix) provides a Flix region worker
with explicit Stop, bounded demand/reply CSP channels, and an in-worker effect
handler. Abandoning a consumer cannot strand a blocking send during region exit.
The default driver, conductor, and caller idle strategies park/sleep about 1 ms.
See [worker/interop notes](docs/workers-interop.md).

## Archive

[Archive.flix](src/Nema/Aeron/Archive.flix) exposes owned local Archive scopes,
start/stop/list, typed descriptors/progress, bounded replay and explicit replay
close, and released PersistentSubscription polling with live/replay/failure state.
The Archive borrows its driver; one wrapper Archive per driver is enforced to
isolate local control streams. Storage survives close. Identity/catalog mismatches
fail closed to prevent recording ID reuse under an old incarnation.

Replay validates retained frame boundaries, including fragmentation. It restores
the original source session/stream/source identity from the recording descriptor;
image correlation is specific to the new replay. Original per-fragment header
sequences and old image correlation IDs are not reconstructed. A premature replay
end is a failure, never successful completion.

Released PersistentSubscription supports IPC and live → replay fallback, and
already assembles fragments. It receives no second fragment assembler. Live
subscription filtering pins the recorded publisher session. Remote Archive
adoption, remote range inspection, replication, multicast, and ReplayMerge are
not exposed. MDC is an optional tested native probe. Details and source locations:
[Archive API](docs/archive-api.md), [transport API](docs/transport-api.md).

## Persisted-through application example

`persistence-handoff.sh` runs two real Flix processes:

1. Publish 40 source-local observation envelopes; separately check receipt and
   recorded progress. Replay exact bytes into a local forced journal, recording
   source range, record ID, durable locator, bytes, and checkpoint together.
2. Close owned scopes, then deliberately `halt(73)` before any notification.
3. Reopen the retained Archive and sink; reconstruct the contiguous checkpoint;
   publish its notification. Attach a fresh subscription after the notification
   was consumed and satisfy its waiter from retained state.
4. Prove a replay pin blocks front purge; release the pin, compute boundaries
   from recording metadata, and purge complete front segments. Resolve and check
   an old durable sink locator after Archive bytes have been removed.

The sink journal's documented policy is checksummed atomic logical records,
`FileChannel.force(true)`, and parent-directory force. Torn suffixes are quarantined
and ignored; complete corrupt records fail closed. Identical retries return the
same locator; conflicting identities/ranges are rejected. Its single writer can
serve local waiters using the same open sink handle. Cross-process sink subscription
service is not implemented. See [sink policy](docs/sink-policy.md).

The watermark is a contiguous prefix for a recording incarnation, never the
maximum received position. The example trusts an original base of zero established
before first publication and reuses it after purge, irrespective of mutable
catalog start. It verifies actual PAD frames before covering gaps between messages.
The pure retention model additionally tests two required sinks, a hole, a replay
pin, and a different incarnation with the same recording ID.

This single-producer laboratory stops at its configured Archive storage cap.
It reserves a segment before publication and waits for recording before the next
cap observation, preventing an unaccounted backlog. Multi-producer storage quotas
would require a shared reservation owner. The ack stream itself is only a hint;
locators/checkpoints live in the sink outside purged Archive storage.

`received`, `Archive-recorded`, `downstream-persisted`, and `consumer-applied`
appear separately in the trace. Sync policy is exposed separately from recording
progress. Neither forced writes nor process crash tests promise survival of every
power-loss or storage-controller failure.

## Integration without parallel workstream changes

Keep this independent module optional. A future host build can include its
`src/Nema/Aeron*.flix` tree plus the generated Java bridge and pinned Aeron jar;
reuse the host's existing empty `Nema` namespace instead of copying module
`src/Nema.flix` or `src/Main.flix`. Install real/fixture handlers at the caller's
scope and pass opaque payloads plus source-native metadata. Use explicit Stop
before closing any host region. Add a host command only after that integration
is chosen. Nothing here imports or edits Vivarium or zygon, root `flix.toml`,
`bin/nema`, root progress, or architecture.

The optional observation convention and sample are local to this module:
[schema](docs/record.schema.json), [example](examples/record.json).
