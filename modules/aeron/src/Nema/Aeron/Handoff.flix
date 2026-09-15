/// Readable application-level persistence protocol above the opaque byte transport.
pub mod Nema.Aeron.Handoff {
    use Nema.Aeron
    use Nema.Aeron.{Failure, Offer, Publication, Subscription, Position, RecordingKey, Cancel, Deadline}
    use Nema.Aeron.Position.Position
    use Nema.Aeron.ChannelUri.ChannelUri
    use Nema.Aeron.StreamId.StreamId
    use Nema.Aeron.ArchiveId.ArchiveId
    use Nema.Aeron.RecordingId.RecordingId
    use Nema.Aeron.RecordingRange.RecordingRange
    use Nema.Aeron.Archive
    use Nema.Aeron.Sink
    use Nema.Aeron.Retention
    use Nema.Aeron.Retention.{Satisfied, ReplayPin, CapacityDecision}
    use Nema.Aeron.Retention.Satisfied.Satisfied
    use Nema.Aeron.Retention.ReplayPin.ReplayPin
    use Sys.Env
    import java.lang.RuntimeException
    import java.util.UUID
    import nema.aeron.LabFiles

    def value(r: Result[Failure, a]): a \ IO = match r {
        case Ok(x) => x
        case Err(e) => throw new RuntimeException("${e}")
    }
    def require(b: Bool, label: String): Unit \ IO =
        if (b) () else throw new RuntimeException(label)

    /// This example establishes start=0 before recording. Recovery ALWAYS uses
    /// this original base, even after front purge moves the catalog start.
    def originalBase(): Position = Position(0i64)
    def count(): Int32 = 40
    def channel(): Nema.Aeron.ChannelUri = ChannelUri("aeron:ipc?term-length=65536|mtu=1408")

    def payload(incarnation: String, seq: Int32): Vector[Int8] = {
        let text = String.repeat(4000, "x");
        String.toBytes("{\"schema\":\"nema.lab.record/v1\",\"id\":\"${incarnation}:${seq}\",\"source\":\"aeron-lab-producer\",\"incarnation\":\"${incarnation}\",\"seq\":\"${seq}\",\"kind\":\"sample\",\"parents\":[],\"body\":{\"text\":\"λ${text}\"}}")
    }

    /// Reserve a complete next Archive segment before offering. At cap this
    /// example stops; it makes no bounded-disk promise during an indefinite outage.
    def publishCapped(pub0: Publication, bytes: Vector[Int8], storage: String, cap: Int64,
        cancel: Cancel, deadline: Deadline): Result[Failure, Offer] \ IO =
        if (Retention.capacity(LabFiles.storageBytes(storage), 65536i64, cap) == CapacityDecision.StopAtCap)
            Err(Failure.Native("Archive storage cap reached; production stopped"))
        else Aeron.publish(pub0, bytes, cancel, deadline)

    def capture(a: Archive, pub0: Publication, sub: Subscription, incarnation: String, n: Int32,
        storage: String, cap: Int64, cancel: Cancel, deadline: Deadline): Position \ IO =
        if (n == count()) Aeron.publicationPosition(pub0) else {
            let bytes = payload(incarnation, n);
            let offered = value(publishCapped(pub0, bytes, storage, cap, cancel, deadline));
            require(match offered { case Offer.Accepted(_) => true case _ => false }, "offer did not accept");
            let received = value(Aeron.receive(sub, cancel, deadline));
            require(Aeron.messageBytes(received) == bytes, "received bytes differ");
            // Serialize cap accounting with recording: no unrecorded offer may
            // consume another segment after the next storage-size observation.
            let key = value(Archive.awaitRecording(a, StreamId(501), Aeron.publicationSession(pub0), cancel, deadline));
            let _recorded = value(Archive.awaitRecorded(a, key, Aeron.publicationPosition(pub0), cancel, deadline));
            capture(a, pub0, sub, incarnation, n + 1, storage, cap, cancel, deadline)
        }

    def persistReplay(a: Archive, replay: Archive.Replay, sink: Sink, key: RecordingKey,
        incarnation: String, previous: Position, n: Int32, cancel: Cancel, deadline: Deadline): Position \ IO =
        match value(Archive.nextReplay(replay, cancel, deadline)) {
            case None => require(n == count(), "replay message count differs"); previous
            case Some(message) =>
                require(Aeron.messageBytes(message) == payload(incarnation, n), "replay application bytes differ");
                let start = Aeron.messageStart(message);
                let end = Aeron.messageEnd(message);
                require(end > previous, "source coordinates did not advance");
                require(value(Archive.paddingOnly(a, key, previous, start)), "unexplained source hole");
                let _locator = value(Sink.commit(sink, key, "${incarnation}:${n}", previous, end, Aeron.messageBytes(message)));
                persistReplay(a, replay, sink, key, incarnation, end, n + 1, cancel, deadline)
        }

    def recordAndCommit(a: Archive, client: Aeron.Client, storage: String, sinkPath: String): Result[Failure, Position] \ IO = {
        let cancel = Aeron.cancelToken();
        let deadline = Aeron.deadlineAfterMillis(15000i64);
        let incarnation = UUID.randomUUID().toString();
        let registration = value(Archive.startRecording(a, channel(), StreamId(501)));
        let cap = LabFiles.storageBytes(storage) + 8i64 * 65536i64;
        Aeron.withSubscription(client, channel(), StreamId(501), 2, sub ->
            Aeron.withPublication(client, channel(), StreamId(501), pub0 -> {
                require(Aeron.publicationPosition(pub0) == originalBase(), "example requires a fresh source starting at zero");
                let end = capture(a, pub0, sub, incarnation, 0, storage, cap, cancel, deadline);
                println("received: ${count()} complete messages, end=\"${Aeron.positionBytes(end)}\"");
                let key = value(Archive.awaitRecording(a, StreamId(501), Aeron.publicationSession(pub0), cancel, deadline));
                let _progress = value(Archive.awaitRecorded(a, key, end, cancel, deadline));
                let policy = Archive.syncPolicy(a);
                println("Archive-recorded: end=\"${Aeron.positionBytes(end)}\", fileSync=${policy#fileSyncLevel}, catalogSync=${policy#catalogSyncLevel}");
                let _stop = value(Archive.stopRecording(a, registration));
                let beforeCap = Aeron.publicationPosition(pub0);
                let stopped = publishCapped(pub0, payload(incarnation, count()), storage,
                    LabFiles.storageBytes(storage), cancel, deadline);
                require(match stopped { case Err(_) => true case _ => false }, "cap failed to stop publication");
                require(Aeron.publicationPosition(pub0) == beforeCap, "cap wrote bytes after stopping");
                Sink.withSink(sinkPath, sink ->
                    Archive.withReplay(a, RecordingRange(key, originalBase(), end), StreamId(601), 2, replay -> {
                        let through0 = persistReplay(a, replay, sink, key, incarnation, originalBase(), 0, cancel, deadline);
                        require(through0 == end, "sink does not cover complete captured range");
                        require(Sink.ackPosition(value(Sink.through(sink, key, originalBase()))) == end, "checkpoint has a hole");
                        println("downstream-persisted: end=\"${Aeron.positionBytes(end)}\"; notification not yet published");
                        Ok(end)
                    }))
            }))
    }

    pub def prepare(path: String): Result[Failure, Position] \ IO = {
        let driver = "${path}/prepare-driver";
        let storage = "${path}/archive";
        Aeron.withDriver(driver, _ -> Archive.withArchive(driver, storage, ArchiveId(72001i64), 65536, 2,
            archive -> Aeron.withClient(driver, client -> recordAndCommit(archive, client, storage, "${path}/sink"))))
    }

    def notificationAndPurge(a: Archive, client: Aeron.Client, sink: Sink, info: Archive.Info): Result[Failure, Position] \ IO = {
        let key = info#key;
        let cancel = Aeron.cancelToken();
        let deadline = Aeron.deadlineAfterMillis(10000i64);
        // This locator and all sink bytes remain outside soon-to-be-purged storage.
        let firstLocator = value(Sink.locator(sink, key, originalBase()));
        let firstBytes = value(Sink.lookupLocator(sink, firstLocator));
        let ack = value(Sink.through(sink, key, originalBase()));
        let end = Sink.ackPosition(ack);
        require(end == value(Archive.progress(a, key)), "committed prefix does not cover Archive");
        Aeron.withSubscription(client, ChannelUri("aeron:ipc"), StreamId(701), 1, notifications ->
            Aeron.withPublication(client, ChannelUri("aeron:ipc"), StreamId(701), pub0 -> {
                let reemitted = value(Sink.notify(sink, key, originalBase(), pub0, cancel, deadline));
                let wire = value(Aeron.receive(notifications, cancel, deadline));
                require(Aeron.messageBytes(wire) == Sink.ackBytes(reemitted), "notification bytes differ");
                // Create a NEW subscription after the event was already consumed.
                Aeron.withSubscription(client, ChannelUri("aeron:ipc"), StreamId(701), 1, lateSub -> {
                    let late = value(Sink.await(sink, key, originalBase(), end, lateSub, cancel, deadline));
                    require(Sink.ackPosition(late) == end, "late subscriber missed retained acknowledgment");
                    println("persisted-through reemitted and late subscriber satisfied: end=\"${Aeron.positionBytes(end)}\"");
                    reclaim(a, sink, info, end, firstLocator, firstBytes, cancel, deadline)
                })
            }))
    }

    def reclaim(a: Archive, sink: Sink, info: Archive.Info, end: Position, firstLocator: Sink.Locator,
        firstBytes: Vector[Int8], cancel: Cancel, deadline: Deadline): Result[Failure, Position] \ IO = {
        let key = info#key;
        let satisfied = Satisfied("local-file-sink", key, originalBase(), end) :: Nil;
        let required = "local-file-sink" :: Nil;
        let pinned = Retention.reclaimable(key, originalBase(), required, satisfied, ReplayPin(key, originalBase()) :: Nil);
        require(value(Archive.purgeSegments(a, key, pinned, originalBase())) == 0i64, "replay pin did not protect front");
        let safe = Retention.reclaimable(key, originalBase(), required, satisfied, Nil);
        let expectedStart = Retention.segmentBoundary(info#start, safe, info#termLength, info#segmentLength);
        let purged = value(Archive.purgeSegments(a, key, safe, end));
        let after = value(Archive.descriptor(a, key));
        require((purged > 0i64 or info#start > originalBase()) and after#start == expectedStart,
            "segment reclamation did not match metadata");
        require(value(Sink.lookupLocator(sink, firstLocator)) == firstBytes, "purged content is not findable in sink");
        let old = Archive.replay(a, RecordingRange(key, originalBase(), end), StreamId(801), 1);
        require(match old { case Err(_) => true case Ok(_) => false }, "pruned range was accepted");
        // Use a complete-message boundary: segment start can be inside a fragmented message.
        println("front purged: ${purged} segments; retained start=\"${Aeron.positionBytes(after#start)}\"; old locator=${firstLocator}");
        let _cancel = cancel; let _deadline = deadline;
        println("consumer-applied: first retained sink record read and byte-checked after Archive purge");
        Ok(after#start)
    }

    pub def recover(path: String): Result[Failure, Position] \ IO = {
        let driver = "${path}/recover-driver-${UUID.randomUUID().toString()}";
        Aeron.withDriver(driver, _ -> Archive.withArchive(driver, "${path}/archive", ArchiveId(72001i64), 65536, 2,
            archive -> {
                let descriptors = value(Archive.list(archive, RecordingId(0i64), 10));
                let info = match descriptors {
                    case one :: Nil => one
                    case _ => throw new RuntimeException("expected one isolated recording")
                };
                Aeron.withClient(driver, client -> Sink.withSink("${path}/sink", sink -> notificationAndPurge(archive, client, sink, info)))
            }))
    }

    pub def main(): Unit \ {IO, Env} = {
        let path = Env.getVar("NEMA_AERON_RUN") |> Option.getWithDefault(".nema/handoff");
        let phase = Env.getVar("NEMA_AERON_PHASE") |> Option.getWithDefault("recover");
        if (phase == "prepare") {
            let _end = value(prepare(path));
            require(Aeron.resourceCount() == 0, "prepare leaked a transport handle");
            println("crash injection: halt(73) after forced sink commit, before notification");
            LabFiles.haltAfterCommit()
        } else {
            let _start = value(recover(path));
            require(Aeron.resourceCount() == 0, "recover leaked a transport handle");
            println("persistence handoff PASS")
        }
    }
}
