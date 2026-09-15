pub mod Nema.Aeron.Archive {
    use Nema.Aeron
    use Nema.Aeron.{ArchiveId, ArchiveIdentity, RecordingId, RecordingKey, RecordingRange,
        Position, SessionId, StreamId, ChannelUri, Message, Failure, Cancel, Deadline}
    use Nema.Aeron.ArchiveId.ArchiveId
    use Nema.Aeron.ArchiveIdentity.ArchiveIdentity
    use Nema.Aeron.RecordingId.RecordingId
    use Nema.Aeron.RecordingKey.RecordingKey
    use Nema.Aeron.RecordingRange.RecordingRange
    use Nema.Aeron.Position.Position
    use Nema.Aeron.SessionId.SessionId
    use Nema.Aeron.StreamId.StreamId
    use Nema.Aeron.ChannelUri.ChannelUri
    use Nema.Aeron.Archive.Archive
    use Nema.Aeron.Archive.Registration.Registration
    use Nema.Aeron.Archive.Replay.Replay
    use Nema.Aeron.Archive.Persistent.Persistent
    import nema.aeron.{ArchiveBridge => JArchive, ArchiveBridge$Descriptor => JDescriptor,
        ArchiveBridge$Replay => JReplay, ArchiveBridge$Persistent => JPersistent}

    pub enum Archive(JArchive)
    pub enum Registration(ArchiveIdentity, Int64)
    pub enum Replay(JReplay)
    pub enum Persistent(JPersistent)
    pub enum FollowState with Eq, ToString { case Replaying, case Live, case Connecting, case Failed(String) }
    pub type alias Info = { key = RecordingKey, start = Position, stop = Option[Position],
        termLength = Int32, segmentLength = Int32, mtuLength = Int32, initialTermId = Int32,
        session = SessionId, stream = StreamId, channel = ChannelUri, source = String }
    pub type alias SyncPolicy = { fileSyncLevel = Int32, catalogSyncLevel = Int32 }

    /// Owns Archive server+control client, borrows its Media Driver. Storage is retained.
    pub def open(driver: String, storage: String, id: ArchiveId, segmentLength: Int32, syncLevel: Int32): Result[Failure, Archive] \ IO = {
        let ArchiveId(n) = id;
        Aeron.attempt(_ -> Archive(JArchive.openOwned(driver, storage, n, segmentLength, syncLevel)))
    }
    pub def close(a: Archive): Result[Failure, Unit] \ IO = let Archive(j) = a; Aeron.attempt(_ -> j.close())
    pub def withArchive(driver: String, storage: String, id: ArchiveId, segment: Int32, sync: Int32,
        body: Archive -> Result[Failure, a] \ IO): Result[Failure, a] \ IO =
        Aeron.scope(open(driver, storage, id, segment, sync), close, body)

    pub def identity(a: Archive): ArchiveIdentity \ IO = let Archive(j) = a; ArchiveIdentity(j.incarnation())
    pub def syncPolicy(a: Archive): SyncPolicy \ IO = {
        let Archive(j) = a; { fileSyncLevel = j.fileSyncLevel(), catalogSyncLevel = j.catalogSyncLevel() }
    }
    pub def diagnostic(a: Archive): String \ IO = let Archive(j) = a; j.diagnostic()
    pub def droppedDiagnostics(a: Archive): Int64 \ IO = let Archive(j) = a; j.droppedDiagnostics()
    pub def startRecording(a: Archive, uri: ChannelUri, stream: StreamId): Result[Failure, Registration] \ IO = {
        let Archive(j) = a; let ChannelUri(u) = uri; let StreamId(s) = stream;
        Aeron.attempt(_ -> Registration(identity(a), j.startRecording(u, s)))
    }
    pub def stopRecording(a: Archive, registration: Registration): Result[Failure, Unit] \ IO = {
        let Archive(j) = a; let Registration(i, n) = registration;
        if (i != identity(a)) Err(Failure.InvalidRange("recording registration belongs to another Archive"))
        else Aeron.attempt(_ -> j.stopRecording(n))
    }
    pub def findRecording(a: Archive, stream: StreamId, session: SessionId): Result[Failure, Option[RecordingKey]] \ IO = {
        let Archive(j) = a; let StreamId(s) = stream; let SessionId(n) = session;
        Aeron.attempt(_ -> {
            let id = j.findRecording("", s, n);
            if (id < 0i64) None else Some(RecordingKey(identity(a), RecordingId(id)))
        })
    }
    pub def awaitRecording(a: Archive, stream: StreamId, session: SessionId, cancel: Cancel, deadline: Deadline): Result[Failure, RecordingKey] \ IO =
        if (Aeron.isStopped(cancel)) Err(Failure.Cancelled)
        else if (Aeron.expired(deadline)) Err(Failure.Deadline)
        else match findRecording(a, stream, session) {
            case Err(e) => Err(e)
            case Ok(Some(k)) => Ok(k)
            case Ok(None) => Aeron.idle(cancel); awaitRecording(a, stream, session, cancel, deadline)
        }
    def coordinate(a: Archive, key: RecordingKey): Result[Failure, Int64] \ IO = {
        let RecordingKey(i, RecordingId(n)) = key;
        if (i != identity(a)) Err(Failure.InvalidRange("recording belongs to another Archive incarnation")) else Ok(n)
    }
    def info(d: JDescriptor): Info \ IO = {
        key = RecordingKey(ArchiveIdentity(d.incarnation()), RecordingId(d.recordingId())),
        start = Position(d.startPosition()), stop = if (d.stopPosition() < 0i64) None else Some(Position(d.stopPosition())),
        termLength = d.termLength(), segmentLength = d.segmentLength(), mtuLength = d.mtuLength(),
        initialTermId = d.initialTermId(), session = SessionId(d.sessionId()), stream = StreamId(d.streamId()),
        channel = ChannelUri(d.channel()), source = d.sourceIdentity()
    }
    pub def descriptor(a: Archive, key: RecordingKey): Result[Failure, Info] \ IO = {
        let Archive(j) = a;
        match coordinate(a, key) { case Err(e) => Err(e) case Ok(n) => Aeron.attempt(_ -> info(j.descriptor(n))) }
    }
    pub def list(a: Archive, first: RecordingId, count: Int32): Result[Failure, List[Info]] \ IO = {
        let Archive(j) = a; let RecordingId(n) = first;
        Aeron.attempt(_ -> Array.toList(j.list(n, count)) |> List.map(info))
    }
    /// Observed Archive recording progress; NEVER a claim named fsyncComplete.
    pub def progress(a: Archive, key: RecordingKey): Result[Failure, Position] \ IO = {
        let Archive(j) = a;
        match coordinate(a, key) { case Err(e) => Err(e) case Ok(n) => Aeron.attempt(_ -> Position(j.progress(n))) }
    }
    pub def awaitRecorded(a: Archive, key: RecordingKey, end: Position, cancel: Cancel, deadline: Deadline): Result[Failure, Position] \ IO =
        if (Aeron.isStopped(cancel)) Err(Failure.Cancelled)
        else if (Aeron.expired(deadline)) Err(Failure.Deadline)
        else match progress(a, key) {
            case Err(e) => Err(e)
            case Ok(p) => if (p >= end) Ok(p) else {
                Aeron.idle(cancel); awaitRecorded(a, key, end, cancel, deadline)
            }
        }
    pub def replay(a: Archive, range: RecordingRange, stream: StreamId, capacity: Int32): Result[Failure, Replay] \ IO = {
        let Archive(j) = a; let RecordingRange(k, Position(start), Position(end)) = range; let StreamId(s) = stream;
        match coordinate(a, k) {
            case Err(e) => Err(e)
            case Ok(n) => Aeron.attempt(_ -> Replay(j.replay(n, start, end, s, capacity)))
        }
    }
    pub def closeReplay(r: Replay): Result[Failure, Unit] \ IO = let Replay(j) = r; Aeron.attempt(_ -> j.close())
    pub def withReplay(a: Archive, range: RecordingRange, stream: StreamId, capacity: Int32,
        body: Replay -> Result[Failure, b] \ IO): Result[Failure, b] \ IO =
        Aeron.scope(replay(a, range, stream, capacity), closeReplay, body)
    pub def pollReplay(r: Replay, limit: Int32): Result[Failure, Int32] \ IO = let Replay(j) = r; Aeron.attempt(_ -> j.poll(limit))
    pub def takeReplay(r: Replay): Result[Failure, Option[Message]] \ IO = let Replay(j) = r; Aeron.attempt(_ -> Aeron.fromNative(j.take()))
    pub def replayComplete(r: Replay): Bool \ IO = let Replay(j) = r; j.complete()
    pub def nextReplay(r: Replay, cancel: Cancel, deadline: Deadline): Result[Failure, Option[Message]] \ IO =
        if (Aeron.isStopped(cancel)) Err(Failure.Cancelled)
        else if (Aeron.expired(deadline)) Err(Failure.Deadline)
        else match pollReplay(r, 16) {
            case Err(e) => Err(e)
            case Ok(_) => match takeReplay(r) {
                case Err(e) => Err(e)
                case Ok(Some(m)) => Ok(Some(m))
                case Ok(None) => if (replayComplete(r)) Ok(None) else { Aeron.idle(cancel); nextReplay(r, cancel, deadline) }
            }
        }

    /// PersistentSubscription 1.53.1 supports IPC and internally assembles.
    pub def follow(a: Archive, key: RecordingKey, start: Position, live: ChannelUri, liveStream: StreamId,
        replayStream: StreamId, capacity: Int32): Result[Failure, Persistent] \ IO = {
        let Archive(j) = a; let Position(p) = start; let ChannelUri(u) = live;
        let StreamId(s) = liveStream; let StreamId(r) = replayStream;
        match coordinate(a, key) {
            case Err(e) => Err(e)
            case Ok(n) => Aeron.attempt(_ -> Persistent(j.persistent(n, p, u, s, r, capacity)))
        }
    }
    pub def closePersistent(p: Persistent): Result[Failure, Unit] \ IO = let Persistent(j) = p; Aeron.attempt(_ -> j.close())
    pub def pollPersistent(p: Persistent, limit: Int32): Result[Failure, Int32] \ IO = let Persistent(j) = p; Aeron.attempt(_ -> j.poll(limit))
    pub def takePersistent(p: Persistent): Result[Failure, Option[Message]] \ IO = let Persistent(j) = p; Aeron.attempt(_ -> Aeron.fromNative(j.take()))
    pub def followState(p: Persistent): FollowState \ IO = {
        let Persistent(j) = p;
        if (j.hasFailed()) FollowState.Failed(j.failureReason())
        else if (j.isLive()) FollowState.Live
        else if (j.isReplaying()) FollowState.Replaying else FollowState.Connecting
    }
    pub def paddingOnly(a: Archive, key: RecordingKey, start: Position, end: Position): Result[Failure, Bool] \ IO = {
        let Archive(j) = a; let Position(f) = start; let Position(t) = end;
        match coordinate(a, key) { case Err(e) => Err(e) case Ok(n) => Aeron.attempt(_ -> j.paddingOnly(n, f, t)) }
    }
    /// Application supplies satisfied prefix AND earliest explicit replay pin.
    pub def purgeSegments(a: Archive, key: RecordingKey, through0: Position, replayPin: Position): Result[Failure, Int64] \ IO = {
        let Archive(j) = a; let Position(p) = through0; let Position(pin) = replayPin;
        match coordinate(a, key) { case Err(e) => Err(e) case Ok(n) => Aeron.attempt(_ -> j.purgeSegments(n, p, pin)) }
    }
}
