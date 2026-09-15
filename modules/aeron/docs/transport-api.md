# Transport API and release evidence

The public interface is real Flix in `src/Nema/Aeron.flix`. `Transport.java` is
the callback, byte buffer, driver/client and optional queue polling adapter. It
does not interpret application records or acknowledgments.

## Run

From the dedicated worktree:

```sh
modules/aeron/bin/aeron test
modules/aeron/bin/aeron run --entrypoint Nema.Aeron.Examples.main
modules/aeron/bin/aeron java nema.aeron.TransportChecks
modules/aeron/bin/aeron java nema.aeron.TransportChecks mdc
```

The launcher allocates a fresh `NEMA_AERON_RUN` below `modules/aeron/.nema/runs`.
Every Media Driver receives an explicit fresh directory there. Test UDP ports
are selected using `endpoint=127.0.0.1:0` and the actual resolved URI is passed
to the sender while the subscription keeps its socket bound. No fixed global
port or `/dev/shm/aeron-USER` default is used.

Default guest Java was OpenJDK 26.0.2.1; Flix is 0.75.3. The launcher passes
`--add-opens java.base/jdk.internal.misc=ALL-UNNAMED` for Agrona and uses the
guest's default Java. No second JDK is required.

## Small public surface

| Operation | Contract |
| --- | --- |
| `withDriver(path, body)` | Own a driver in a fresh directory; close on normal return, error result, or Java exception. |
| `withClient(path, body)` | Borrow the driver; own and close one Aeron client. |
| `withPublication(client, channel, stream, body)` | Own a distinct exclusive publication session. |
| `withSubscription(client, channel, stream, capacity, body)` | Own a subscription and bounded complete-message staging queue. |
| `tryPublish(publication, bytes)` | One offer; expose every native status. |
| `publish(publication, bytes, cancel, deadline)` | Flix retry loop for temporary statuses, cancellable and bounded by a monotonic deadline. |
| `poll(subscription, fragmentLimit)` | One explicit controlled poll by the subscription's polling owner. |
| `take(subscription)` | Remove an owned complete message from staging, or return `None`. |
| `receive(subscription, cancel, deadline)` | Flix polling/idle loop with cancellation and deadline. |
| `cancelToken`, `stop`, `idle` | Level-triggered cooperative cancellation; stopping unparks idle token users. |
| `resourceCount`, `resources`, `diagnostic` | Inspect remaining bridge handles and retained native/worker errors. |

Equivalent `open*`/`close*` functions support callers with their own lifetime
orchestration. Handles have separate Flix types. `ChannelUri`, `StreamId`,
`SessionId`, `Position`, `ArchiveIdentity`, `RecordingKey`, and `RecordingRange`
prevent accidental positional integer interchange. This is dynamic scope
ownership, not an affine or linear type proof: Flix 0.75.3 can retain a Java
handle after the scope, but the native resource is then closed. Interpret user
effects inside native resource scopes. A non-resuming or multi-shot effect
handler outside a resource scope can skip cleanup or duplicate use of a native
handle; primitive-effect scope bodies prevent this form of continuation escape.

The review reproduced that Flix 0.75.3 behavior with an isolated real program:

```flix
import java.util.concurrent.atomic.AtomicInteger
import java.lang.Throwable
pub eff AbortScope { def skip(): Unit }
def main(): Unit \ IO = {
    let closed = new AtomicInteger(0);
    let result = run {
        let value = try { AbortScope.skip(); 1 }
            catch { case _ex: Throwable => 0 };
        let _ = closed.incrementAndGet();
        value
    } with handler AbortScope {
        def skip(_, _resume) = 99
    };
    println("result=${result}; closeCalls=${closed.get()}")
}
```

Running this in a standalone Flix 0.75.3 project printed
`result=99; closeCalls=0`. The actual probe is retained locally in
`.nema/review-scope`; the small source above is the durable reproducer. Java
`try/catch` alone cannot close a resource when an algebraic continuation is
abandoned.

`Position` is an Aeron byte coordinate. A `RecordingKey` combines the Archive's
retained incarnation identity and its recording number. Neither is a message
count, application identity, semantic clock, or total ordering of sessions.

## Direct-style effect

`src/Nema/Aeron/Flow.flix` declares two actual Flix effect operations, `send`
and `next`, and interprets them with either live Aeron or an immutable fixture.
This exact small program runs under both handlers:

```flix
pub def roundTrip(bytes: Vector[Int8]): Result[Failure, Bool] \ Flow =
    match Flow.send(bytes) {
        case Err(e) => Err(e)
        case Ok(Offer.Accepted(_)) => match Flow.next() {
            case Err(e) => Err(e)
            case Ok(m) => Ok(Aeron.messageBytes(m) == bytes)
        }
        case Ok(other) => Err(Failure.Native("publication did not accept: ${other}"))
    }
```

`Flow.real(pub, sub, cancel, deadline, body)` translates each operation into
the Flix low-level API. `Flow.fixture(offerResult, incomingResult, body)` is
deterministic and creates no JVM resources. `Examples.flix` supplies the nested
resource scopes for the real IPC interpretation. There is one effect operation
per application message, never one continuation per byte or fragment.

## Publication outcomes

The pinned release's `io/aeron/Publication.java` defines:

| Native offer | Flix outcome | Retry behavior |
| --- | --- | --- |
| Positive position | `Accepted(Position)` | Done; appended to the local publication log. |
| `-1` | `NotConnected` | Temporary; retry until stopped/deadline. |
| `-2` | `Backpressured` | Temporary; retry until stopped/deadline. |
| `-3` | `AdministrativeRetry` | Temporary, e.g. term rotation. |
| `-4` | `Closed` | Terminal. |
| `-5` | `PositionExhausted` | Terminal; a new publication is needed. |
| Other negative value | `UnknownNative(value)` | Preserved rather than guessed. |

An accepted offer proves neither receiver delivery nor Archive durability nor
downstream sink commit. TransportTests observe real `-1`, `-2`, and `-4`;
the `-3` and `-5` mappings are verified against the released source and Flix
fixture tests. Exhausting the full native position space is not a live test.
The source calculates maximum position from term length times `2^31`, not
three term buffers.

## Callback lifetime, coordinates, and ordering

Normal subscriptions use exactly one `ControlledFragmentAssembler`. Its
delegate is a small Java callback which checks queue capacity, copies the
complete message into owned bytes, snapshots scalar header metadata, and
returns. No Flix/UI callback runs on the driver or client conductor.

The metadata includes session, stream, start and end byte positions, source
identity, term ID/offset, flags, reserved value, and image correlation ID.
`Header.position()` accounts for fragment headers and alignment. The start is
computed using `LogBufferDescriptor.computePosition` and the assembled header's
initial term ID, term ID, term offset, and position shift. For the 10,000-byte
IPC smoke test, the full aligned range was `[0, 10272)` and the header end
matched the offer result. The release's `BufferBuilder.captureHeader` retains
the first frame header; `completeHeader` fixes flags, frame length and complete
occupancy. This matters because `ControlledFragmentAssembler` has an older
Javadoc sentence referring to the last fragment.

Term-end padding can create space between successive application-message
ranges. A retention layer must account for actual padding/frame boundaries; it
cannot infer a missing application message solely from subtracting payload
lengths. Ordering is per publication session. Image/source attribution is
retained without pretending unrelated sessions have one total order.

No borrowed-buffer public Flix path is offered. The Java message owns its array;
Flix `take` converts it to an immutable byte vector. The helper `copyMessage`
is callback-only Java interop and never retains its `DirectBuffer` or `Header`.
The native Java handles are available to module fixtures as an advanced escape
hatch, not a supported way to bypass the single-owner contract.

## Queue pressure and worker lifetimes

The queue is bounded in complete messages. On a full queue the callback returns
`ABORT` before copying/enqueueing. The selected assembler restores its partial
buffer limit when the final fragment is aborted, and native polling retries
that fragment. After capacity is available, the same application message is
enqueued once. The capacity-one regression fills staging, retries an aborted
19KB fragmented message thirty times, drains capacity, and verifies exact bytes
and no duplicate. `queueFullCount` and queue high-water evidence make pressure
visible. Assembly buffers are freed on unavailable-session notifications by
the polling owner, and cleared on close.

The Java `startWorker` helper performs only controlled polling into staging.
Flix remains responsible for lifecycle and CSP delivery policy. A subscription
permits one polling thread; manual callers explicitly release ownership before
handing it over. Manual polling and a worker must not both poll one subscription.
Queue consumption does not claim polling ownership.

Default driver/client idle strategies sleep for 1ms. Token idle waits register
their thread before checking cancellation, preventing a lost stop wakeup.
Closing a subscription stops and joins its worker before native buffers close;
the join has a two-second bound. A joined worker's failure is propagated while
the subscription still closes. An unjoined worker remains reported as a leak
and native buffers are kept mapped for a later close attempt. Closing a client
reclaims any forgotten child resources and reports them. If a polling worker
has not joined, the client also remains open; retrying close after the worker
stops completes reclamation. The interrupted-join test verifies this behavior.
Driver handles are
separate: closing a borrowed client cannot stop its Media Driver.

## Independent native peer

`NativePeer.java` intentionally imports raw Aeron APIs and never calls the
bridge. Its bounded command contract is:

```text
send[-owned]    DRIVER_DIRECTORY CHANNEL STREAM HEX_PAYLOAD COUNT
receive[-owned] DRIVER_DIRECTORY CHANNEL STREAM HEX_PAYLOAD COUNT [READY_FILE]
```

Without `-owned`, a peer borrows an already running driver. With `-owned`, it
launches and closes its own driver in the supplied fresh directory. A receiver
checks the exact opaque bytes and count and can write its resolved channel URI
to a ready file. Using `endpoint=127.0.0.1:0` selects a port without a bind race;
the sender reads the resolved URI. `Transport.subscriptionChannel` provides the
same resolved URI for a bridge subscription (empty until ready). Sender
and receiver each have a ten-second deadline. `TransportChecks` launches this
fixture in independent JVM processes and independent Media Drivers for both
UDP directions, using 10KB fragmented binary messages. The Flix interoperability
example uses the same independent fixture.

## PersistentSubscription release verification

The published 1.53.1 sources include `io.aeron.archive.client.PersistentSubscription`.
Its actual implementation owns `ImageControlledFragmentAssembler` and
`ImageFragmentAssembler` instances; its controlled callback receives whole
messages. An Archive wrapper must pass a plain whole-message handler, not a
second assembler. Its context accepts IPC replay channels; source selection
supports replay, attempting a switch, live, and return to replay after live
loss. `isLive`, `isReplaying`, `hasFailed`, `failureReason`, and transition
listeners expose status. See the Archive checks for observed replay/live
behavior; source availability alone is not a passing failure-injection test.

The upstream [Persistent Subscriptions guide](https://github.com/aeron-io/aeron/wiki/Persistent-Subscriptions)
also documents IPC/spy support and fallback, while `ReplayMerge` lacks those
capabilities. The bridge selects the released persistent-subscription API,
not an emulation based on `ReplayMerge`.

## Observed transport results

The final transport command with optional MDC passed on the ARM guest:

```text
modules/aeron/bin/aeron java nema.aeron.TransportChecks mdc
PASS IPC binary + UTF8 + fragmentation + two sessions + two subscribers
PASS capacity=1; 30 controlled ABORT retries; no loss/duplicate
PASS single polling owner; 10s park cancelled in 0ms
PASS synthetic retained-worker failure propagates and cleanup completes
PASS native disconnected/backpressure/closed outcomes and cancellation
PASS eight open/close cycles; borrowed driver survives
PASS independent native JVM UDP producer and consumer
PASS real driver shutdown reports DriverTimeoutException
PASS optional manual UDP MDC, two loopback destinations
PASS zero remaining transport handles
```

A 500ms idle sample used approximately 0.060 CPU cores. A diagnostic run sent and
checked 5,000 messages of 256 bytes in 0.051s, queue high water 64/64 and 39 ABORTs.
These are rough regression observations including application work, not a
competitive latency/throughput benchmark. Multicast membership, non-loopback
networks and power-loss behavior were not tested by this suite. Loopback MDC is
an optional bounded probe, not a mandatory network capability.

An earlier strict sandbox denied even the driver's UDP capability-probe socket
with `Operation not permitted`; approved native execution passed. One interrupted
VM-era run reported `msync with parameter MS_SYNC failed`; fresh worktree-local
runtime runs passed after resume. Neither observation was hidden as a passing
test. Raw native peer output stays under the local run directory.

## Pinned primary source

Released [binary artifact](https://repo.maven.apache.org/maven2/io/aeron/aeron-all/1.53.1/aeron-all-1.53.1.jar)
and [source artifact](https://repo.maven.apache.org/maven2/io/aeron/aeron-all/1.53.1/aeron-all-1.53.1-sources.jar)
were fetched and inspected. Their SHA-256 values are:

```text
binary  504ac8bc74ca63783a921da8e7ee96985b8422dd7cae003b9f629e75e97ae333
source  27af40c95139b945e3476b520eacc7cddb62f12917deccf6114529463e82e9a1
```

Locally reproducible inspections:

```sh
unzip -p modules/aeron/.nema/deps/aeron-all-1.53.1-sources.jar io/aeron/Publication.java
unzip -p modules/aeron/.nema/deps/aeron-all-1.53.1-sources.jar io/aeron/ControlledFragmentAssembler.java
unzip -p modules/aeron/.nema/deps/aeron-all-1.53.1-sources.jar io/aeron/BufferBuilder.java
unzip -p modules/aeron/.nema/deps/aeron-all-1.53.1-sources.jar io/aeron/logbuffer/Header.java
unzip -p modules/aeron/.nema/deps/aeron-all-1.53.1-sources.jar io/aeron/archive/client/PersistentSubscription.java
```

The general [publication/subscription documentation](https://aeron.io/docs/aeron/publications-subscriptions/)
is useful background. The selected release's source and executed checks settle
the numeric, assembly, and lifecycle contracts above.
