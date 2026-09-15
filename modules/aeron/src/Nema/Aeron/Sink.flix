/// Example adapter above transport. Durable locators live outside Archive files.
pub mod Nema.Aeron.Sink {
    use Nema.Aeron
    use Nema.Aeron.{RecordingKey, Position, Failure, Publication, Subscription, Cancel, Deadline, Offer}
    use Nema.Aeron.Position.Position
    use Nema.Aeron.Sink.Sink
    use Nema.Aeron.Sink.Locator.Locator
    use Nema.Aeron.Sink.PersistedThrough.PersistedThrough
    import nema.aeron.{DurableSink => JSink}
    import nema.aeron.LabFiles

    pub enum Sink(JSink)
    pub enum Locator(String) with Eq, ToString
    pub enum PersistedThrough(RecordingKey, Position) with Eq, ToString

    pub def open(path: String): Result[Failure, Sink] \ IO = Aeron.attempt(_ -> Sink(JSink.open(path)))
    pub def close(s: Sink): Result[Failure, Unit] \ IO = let Sink(j) = s; Aeron.attempt(_ -> j.close())
    pub def withSink(path: String, body: Sink -> Result[Failure, a] \ IO): Result[Failure, a] \ IO =
        Aeron.scope(open(path), close, body)

    /// Commit requires verified coverage, not payload-length arithmetic. Mapping,
    /// bytes, identity and checkpoint candidate are one checksummed forced record.
    pub def commit(s: Sink, key: RecordingKey, identity: String, start: Position, end: Position,
                   bytes: Vector[Int8]): Result[Failure, Locator] \ IO = region rc {
        let Sink(j) = s; let Position(b) = start; let Position(e) = end;
        let data = Vector.toArray(rc, bytes);
        Aeron.attempt(_ -> Locator(j.commitRecord(Aeron.recordingSource(key), identity, b, e, data)))
    }
    pub def through(s: Sink, key: RecordingKey, base: Position): Result[Failure, PersistedThrough] \ IO = {
        let Sink(j) = s; let Position(b) = base;
        Aeron.attempt(_ -> PersistedThrough(key, Position(j.persistedThrough(Aeron.recordingSource(key), b))))
    }
    pub def ackPosition(a: PersistedThrough): Position = let PersistedThrough(_, p) = a; p
    pub def lookup(s: Sink, key: RecordingKey, start: Position): Result[Failure, Vector[Int8]] \ IO = {
        let Sink(j) = s; let Position(b) = start;
        Aeron.attempt(_ -> Array.toVector(j.lookup(Aeron.recordingSource(key), b)))
    }
    pub def locator(s: Sink, key: RecordingKey, start: Position): Result[Failure, Locator] \ IO = {
        let Sink(j) = s; let Position(b) = start;
        Aeron.attempt(_ -> Locator(j.locator(Aeron.recordingSource(key), b)))
    }
    pub def lookupLocator(s: Sink, locator0: Locator): Result[Failure, Vector[Int8]] \ IO = {
        let Sink(j) = s; let Locator(l) = locator0; Aeron.attempt(_ -> Array.toVector(j.lookupLocator(l)))
    }
    /// Notification carries strings for lossless 64-bit coordinates.
    pub def ackBytes(a: PersistedThrough): Vector[Int8] = {
        let PersistedThrough(key, Position(p)) = a;
        String.toBytes("{\"schema\":\"nema.lab.persisted-through/v1\",\"recording\":\"${jsonEscape(Aeron.recordingSource(key))}\",\"end\":\"${p}\"}")
    }
    def jsonEscape(s: String): String =
        unsafe IO { LabFiles.jsonEscape(s) }
    /// Reconstruct and reemit after a commit/notification crash. Safe to duplicate.
    pub def notify(s: Sink, key: RecordingKey, base: Position, pub0: Publication, cancel: Cancel,
        deadline: Deadline): Result[Failure, PersistedThrough] \ IO =
        match through(s, key, base) {
            case Err(e) => Err(e)
            case Ok(ack) => match Aeron.publish(pub0, ackBytes(ack), cancel, deadline) {
                case Err(e) => Err(e)
                case Ok(Offer.Accepted(_)) => Ok(ack)
                case Ok(other) => Err(Failure.Native("ack offer was not accepted: ${other}"))
            }
        }
    /// Subscribe before calling. Retained level is checked every iteration: a
    /// late waiter succeeds even when the notification has already passed.
    /// Notifications are hints only; retained committed state is the authority.
    pub def await(s: Sink, key: RecordingKey, base: Position, target: Position,
        notifications: Subscription, cancel: Cancel, deadline: Deadline): Result[Failure, PersistedThrough] \ IO =
        if (Aeron.isStopped(cancel)) Err(Failure.Cancelled)
        else match through(s, key, base) {
            case Err(e) => Err(e)
            case Ok(ack) => if (ackPosition(ack) >= target) Ok(ack)
                else if (Aeron.expired(deadline)) Err(Failure.Deadline)
                else match Aeron.poll(notifications, 8) {
                    case Err(e) => Err(e)
                    case Ok(_) => match Aeron.take(notifications) {
                        case Err(e) => Err(e)
                        case Ok(_) => Aeron.idle(cancel); await(s, key, base, target, notifications, cancel, deadline)
                    }
                }
        }
}
