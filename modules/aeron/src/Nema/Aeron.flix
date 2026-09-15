/// Source-native bytes and byte coordinates. No semantic clock or global order.
pub mod Nema.Aeron {
    use Nema.Aeron.Driver.Driver
    use Nema.Aeron.Client.Client
    use Nema.Aeron.Publication.Publication
    use Nema.Aeron.Subscription.Subscription
    use Nema.Aeron.Cancel.Cancel
    use Nema.Aeron.Deadline.Deadline
    use Nema.Aeron.ChannelUri.ChannelUri
    use Nema.Aeron.StreamId.StreamId
    use Nema.Aeron.SessionId.SessionId
    use Nema.Aeron.Position.Position
    use Nema.Aeron.ArchiveIdentity.ArchiveIdentity
    use Nema.Aeron.RecordingKey.RecordingKey
    use Nema.Aeron.RecordingId.RecordingId
    use Nema.Aeron.RecordingRange.RecordingRange
    use Nema.Aeron.NativeHeader.NativeHeader
    use Nema.Aeron.Message.Message
    import nema.aeron.Transport
    import nema.aeron.{Transport$Driver => JDriver, Transport$Client => JClient,
        Transport$Pub => JPub, Transport$Sub => JSub, Transport$Cancel => JCancel,
        Transport$Message => JMessage}
    import java.lang.Throwable

    /// Handles can only be obtained from the bridge's private constructors.
    /// Scopes close dynamically; Flix 0.75.3 does not provide affine Java handles.
    pub enum Driver(JDriver)
    pub enum Client(JClient)
    pub enum Publication(JPub)
    pub enum Subscription(JSub)
    pub enum Cancel(JCancel)
    pub enum Deadline(Int64)
    pub enum ChannelUri(String) with Eq, ToString
    pub enum StreamId(Int32) with Eq, ToString
    pub enum SessionId(Int32) with Eq, Order, ToString
    pub enum Position(Int64) with Eq, Order, ToString
    pub enum ArchiveIdentity(String) with Eq, ToString
    pub enum ArchiveId(Int64) with Eq, ToString
    pub enum RecordingId(Int64) with Eq, ToString
    pub enum RecordingKey(ArchiveIdentity, RecordingId) with Eq, ToString
    pub enum RecordingRange(RecordingKey, Position, Position) with Eq, ToString

    pub enum Offer with Eq, ToString {
        case Accepted(Position)
        case Backpressured
        case NotConnected
        case AdministrativeRetry
        case Closed
        case PositionExhausted
        case UnknownNative(Int64)
    }
    pub enum Failure with Eq, ToString {
        case Cancelled
        case Deadline
        case Native(String)
        case InvalidRange(String)
    }
    pub enum NativeHeader(SessionId, StreamId, Position, Position, String, Int32, Int32, Int32, Int64, Int64) with Eq, ToString
    pub enum Message(Vector[Int8], NativeHeader) with Eq, ToString

    pub def channel(s: String): ChannelUri = ChannelUri(s)
    pub def stream(n: Int32): StreamId = StreamId(n)
    pub def position(n: Int64): Position = Position(n)
    pub def positionBytes(p: Position): Int64 = let Position(n) = p; n
    pub def messageBytes(m: Message): Vector[Int8] = let Message(b, _) = m; b
    pub def messageStart(m: Message): Position = let Message(_, NativeHeader(_, _, p, _, _, _, _, _, _, _)) = m; p
    pub def messageEnd(m: Message): Position = let Message(_, NativeHeader(_, _, _, p, _, _, _, _, _, _)) = m; p
    pub def recordingSource(k: RecordingKey): String =
        let RecordingKey(ArchiveIdentity(a), RecordingId(id)) = k; "${a}/${id}"

    /// Codes verified against Publication.java in Aeron 1.53.1.
    pub def offerOutcome(n: Int64): Offer =
        if (n >= 0i64) Offer.Accepted(Position(n)) else match n {
            case -1i64 => Offer.NotConnected
            case -2i64 => Offer.Backpressured
            case -3i64 => Offer.AdministrativeRetry
            case -4i64 => Offer.Closed
            case -5i64 => Offer.PositionExhausted
            case x => Offer.UnknownNative(x)
        }

    pub def openDriver(path: String): Result[Failure, Driver] \ IO =
        attempt(_ -> Driver(Transport.ownedDriver(path)))
    pub def openClient(path: String): Result[Failure, Client] \ IO =
        attempt(_ -> Client(Transport.connect(path)))
    pub def openPublication(c: Client, uri: ChannelUri, sid: StreamId): Result[Failure, Publication] \ IO = {
        let Client(jc) = c; let ChannelUri(u) = uri; let StreamId(s) = sid;
        attempt(_ -> Publication(Transport.addExclusivePublication(jc, u, s)))
    }
    pub def openSubscription(c: Client, uri: ChannelUri, sid: StreamId, capacity: Int32): Result[Failure, Subscription] \ IO = {
        let Client(jc) = c; let ChannelUri(u) = uri; let StreamId(s) = sid;
        attempt(_ -> Subscription(Transport.addSubscription(jc, u, s, capacity)))
    }
    pub def closeDriver(d: Driver): Result[Failure, Unit] \ IO =
        let Driver(j) = d; attempt(_ -> Transport.closeDriver(j))
    pub def closeClient(c: Client): Result[Failure, Unit] \ IO =
        let Client(j) = c; attempt(_ -> Transport.closeClient(j))
    pub def closePublication(p: Publication): Result[Failure, Unit] \ IO =
        let Publication(j) = p; attempt(_ -> Transport.closePublication(j))
    pub def closeSubscription(s: Subscription): Result[Failure, Unit] \ IO =
        let Subscription(j) = s; attempt(_ -> Transport.closeSubscription(j))

    pub def attempt(f: Unit -> a \ ef): Result[Failure, a] \ ef + IO =
        try { Ok(f()) } catch { case ex: Throwable => Err(Failure.Native(ex.toString())) }

    /// Also catches Java exceptions from the body so closing is not skipped.
    def scopeImpl(open: Result[Failure, a], close: a -> Result[Failure, Unit] \ IO,
              body: a -> Result[Failure, b] \ ef): Result[Failure, b] \ ef + IO =
        match open {
            case Err(e) => Err(e)
            case Ok(resource) =>
                let result = match attempt(_ -> body(resource)) {
                    case Ok(r) => r
                    case Err(e) => Err(e)
                };
                match (result, close(resource)) {
                    case (r, Ok(_)) => r
                    case (Ok(_), Err(e)) => Err(e)
                    case (Err(e), Err(c)) => Err(Failure.Native("body: ${e}; close: ${c}"))
                }
        }
    /// Only primitive effects may cross a resource scope. Interpret custom
    /// effects inside it; an outer algebraic handler must not abandon cleanup.
    pub def scope(open: Result[Failure, a], close: a -> Result[Failure, Unit] \ IO,
        body: a -> Result[Failure, b] \ IO): Result[Failure, b] \ IO = scopeImpl(open, close, body)
    pub def scopeCsp(open: Result[Failure, a], close: a -> Result[Failure, Unit] \ IO,
        body: a -> Result[Failure, b] \ {IO, Chan, NonDet}): Result[Failure, b] \ {IO, Chan, NonDet} = scopeImpl(open, close, body)
    pub def withDriver(path: String, body: Driver -> Result[Failure, a] \ IO): Result[Failure, a] \ IO =
        scope(openDriver(path), closeDriver, body)
    /// Borrows the Media Driver. Closing this scope only closes its Aeron client.
    pub def withClient(path: String, body: Client -> Result[Failure, a] \ IO): Result[Failure, a] \ IO =
        scope(openClient(path), closeClient, body)
    pub def withPublication(c: Client, u: ChannelUri, s: StreamId, body: Publication -> Result[Failure, a] \ IO): Result[Failure, a] \ IO =
        scope(openPublication(c, u, s), closePublication, body)
    pub def withSubscription(c: Client, u: ChannelUri, s: StreamId, capacity: Int32, body: Subscription -> Result[Failure, a] \ IO): Result[Failure, a] \ IO =
        scope(openSubscription(c, u, s, capacity), closeSubscription, body)
    pub def withDriverCsp(path: String, body: Driver -> Result[Failure, a] \ {IO, Chan, NonDet}): Result[Failure, a] \ {IO, Chan, NonDet} =
        scopeCsp(openDriver(path), closeDriver, body)
    pub def withClientCsp(path: String, body: Client -> Result[Failure, a] \ {IO, Chan, NonDet}): Result[Failure, a] \ {IO, Chan, NonDet} =
        scopeCsp(openClient(path), closeClient, body)
    pub def withPublicationCsp(c: Client, u: ChannelUri, s: StreamId, body: Publication -> Result[Failure, a] \ {IO, Chan, NonDet}): Result[Failure, a] \ {IO, Chan, NonDet} =
        scopeCsp(openPublication(c, u, s), closePublication, body)
    pub def withSubscriptionCsp(c: Client, u: ChannelUri, s: StreamId, capacity: Int32, body: Subscription -> Result[Failure, a] \ {IO, Chan, NonDet}): Result[Failure, a] \ {IO, Chan, NonDet} =
        scopeCsp(openSubscription(c, u, s, capacity), closeSubscription, body)

    pub def cancelToken(): Cancel \ IO = Cancel(Transport.newCancel())
    pub def stop(c: Cancel): Unit \ IO = let Cancel(j) = c; Transport.cancel(j)
    pub def isStopped(c: Cancel): Bool \ IO = let Cancel(j) = c; Transport.cancelled(j)
    pub def idle(c: Cancel): Unit \ IO = let Cancel(j) = c; Transport.awaitIdle(j, 1_000_000i64)
    pub def deadlineAfterMillis(ms: Int64): Deadline \ IO = Deadline(Transport.nanoTime() + ms * 1_000_000i64)
    pub def expired(d: Deadline): Bool \ IO = let Deadline(n) = d; Transport.nanoTime() - n >= 0i64
    pub def publicationSession(p: Publication): SessionId \ IO = let Publication(j) = p; SessionId(Transport.publicationSession(j))
    pub def publicationPosition(p: Publication): Position \ IO = let Publication(j) = p; Position(Transport.publicationPosition(j))
    pub def resourceCount(): Int32 \ IO = Transport.openResources()
    pub def resources(): String \ IO = Transport.resourceReport()
    pub def diagnostic(): String \ IO = Transport.diagnostic()

    /// One offer. Accepted means appended to Aeron's publication log, not persisted.
    pub def tryPublish(p: Publication, bytes: Vector[Int8]): Result[Failure, Offer] \ IO = region rc {
        let Publication(j) = p;
        let owned = Vector.toArray(rc, bytes);
        attempt(_ -> offerOutcome(Transport.offer(j, owned)))
    }
    /// Cooperative retry lives in Flix; terminal native outcomes remain typed.
    pub def publish(p: Publication, bytes: Vector[Int8], cancel: Cancel, deadline: Deadline): Result[Failure, Offer] \ IO = region rc {
        let Publication(j) = p;
        let owned = Vector.toArray(rc, bytes);
        retryOffer(j, owned, cancel, deadline)
    }
    def retryOffer(j: JPub, bytes: Array[Int8, r], cancel: Cancel, deadline: Deadline): Result[Failure, Offer] \ {IO, r} =
        if (isStopped(cancel)) Err(Failure.Cancelled)
        else if (expired(deadline)) Err(Failure.Deadline)
        else match attempt(_ -> offerOutcome(Transport.offer(j, bytes))) {
            case Ok(Offer.Backpressured) => idle(cancel); retryOffer(j, bytes, cancel, deadline)
            case Ok(Offer.NotConnected) => idle(cancel); retryOffer(j, bytes, cancel, deadline)
            case Ok(Offer.AdministrativeRetry) => idle(cancel); retryOffer(j, bytes, cancel, deadline)
            case result => result
        }

    /// Single polling owner, callback copies once into a bounded staging queue.
    pub def poll(s: Subscription, fragmentLimit: Int32): Result[Failure, Int32] \ IO =
        let Subscription(j) = s; attempt(_ -> Transport.poll(j, fragmentLimit))
    pub def queued(s: Subscription): Int32 \ IO = let Subscription(j) = s; Transport.queued(j)
    pub def queueFullCount(s: Subscription): Int64 \ IO = let Subscription(j) = s; Transport.queueFullCount(j)
    pub def releasePollOwner(s: Subscription): Result[Failure, Unit] \ IO =
        let Subscription(j) = s; attempt(_ -> Transport.releasePollOwner(j))
    pub def subscriptionClosed(s: Subscription): Bool \ IO =
        let Subscription(j) = s; j.nativeSubscription.isClosed()
    pub def resolvedChannel(s: Subscription): String \ IO =
        let Subscription(j) = s; Transport.subscriptionChannel(j)
    pub def take(s: Subscription): Result[Failure, Option[Message]] \ IO = {
        let Subscription(j) = s;
        attempt(_ -> {
            let m = Transport.take(j);
            fromNative(m)
        })
    }
    /// Bridge seam: conversion consumes an owned callback copy, never a DirectBuffer.
    pub def fromNative(m: JMessage): Option[Message] \ IO =
        if (Transport.isNull(m)) None else
            Some(Message(Array.toVector(Transport.bytes(m)),
                    NativeHeader(SessionId(Transport.session(m)), StreamId(Transport.stream(m)),
                        Position(Transport.startPosition(m)), Position(Transport.endPosition(m)),
                        Transport.source(m), Transport.termId(m), Transport.termOffset(m),
                        Transport.flags(m), Transport.reservedValue(m), Transport.imageCorrelationId(m))))
    pub def receive(s: Subscription, cancel: Cancel, deadline: Deadline): Result[Failure, Message] \ IO =
        if (isStopped(cancel)) Err(Failure.Cancelled)
        else if (expired(deadline)) Err(Failure.Deadline)
        else match poll(s, 16) {
            case Err(e) => Err(e)
            case Ok(_) => match take(s) {
                case Err(e) => Err(e)
                case Ok(Some(m)) => Ok(m)
                case Ok(None) => idle(cancel); receive(s, cancel, deadline)
            }
        }
}
