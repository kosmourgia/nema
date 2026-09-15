/// Optional application policy above transport, not mandatory library semantics.
pub mod Nema.Aeron.Retention {
    use Nema.Aeron.Position.Position
    use Nema.Aeron.Retention.Satisfied.Satisfied
    use Nema.Aeron.Retention.ReplayPin.ReplayPin
    use Nema.Aeron.{RecordingKey, Position}

    pub enum Satisfied(String, RecordingKey, Position, Position) with Eq, ToString
    pub enum ReplayPin(RecordingKey, Position) with Eq, ToString
    pub enum CapacityDecision with Eq, ToString { case AllowPublish, case StopAtCap }

    /// Highest contiguous satisfied end for ONE sink and recording incarnation.
    /// Ranges cover complete application messages and verified transport padding.
    pub def through(sink: String, key: RecordingKey, base: Position, ranges: List[Satisfied]): Position = {
        let Position(b) = base;
        let n = List.foldLeft((acc, r) -> {
            let Satisfied(s, k, Position(start), Position(end)) = r;
            if (s == sink and k == key and start <= acc and end > acc) end else acc
        }, b, ranges);
        if (n == b) base else through(sink, key, Position(n), ranges)
    }

    /// Every required sink must satisfy the prefix. A replay pin can lower it.
    /// Empty obligation sets do not authorize purge.
    pub def reclaimable(key: RecordingKey, base: Position, required: List[String],
                        ranges: List[Satisfied], pins: List[ReplayPin]): Position = {
        let Position(b) = base;
        let satisfied = match required {
            case Nil => b
            case first :: rest =>
                let Position(p) = through(first, key, base, ranges);
                List.foldLeft((acc, sink) -> {
                    let Position(n) = through(sink, key, base, ranges); Int64.min(acc, n)
                }, p, rest)
        };
        Position(List.foldLeft((acc, pin) -> {
            let ReplayPin(k, Position(p)) = pin;
            if (k == key) Int64.max(b, Int64.min(acc, p)) else acc
        }, satisfied, pins))
    }

    /// Segment origin is the term base of recording start, not zero.
    /// Lengths must come from the descriptor; callers validate native metadata.
    pub def segmentBoundary(start: Position, through0: Position, termLength: Int32, segmentLength: Int32): Position = {
        let Position(s) = start; let Position(p) = through0;
        let term = Int32.toInt64(termLength); let segment = Int32.toInt64(segmentLength);
        let origin = s - Int64.remainder(s, term);
        Position(origin + ((p - origin) / segment) * segment)
    }

    /// Stop producing at the configured cap if a sink cannot keep up.
    pub def capacity(retainedBytes: Int64, nextReservation: Int64, cap: Int64): CapacityDecision =
        if (retainedBytes < 0i64 or nextReservation < 0i64 or cap <= 0i64 or
            retainedBytes > cap or nextReservation > cap - retainedBytes)
            CapacityDecision.StopAtCap else CapacityDecision.AllowPublish
}
