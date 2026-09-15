pub mod Nema.Aeron.Examples {
    use Nema.Aeron.ChannelUri.ChannelUri
    use Nema.Aeron.StreamId.StreamId
    use Nema.Aeron.SessionId.SessionId
    use Nema.Aeron.Position.Position
    use Nema.Aeron.NativeHeader.NativeHeader
    use Nema.Aeron.Message.Message
    use Nema.Aeron
    use Nema.Aeron.{ChannelUri, StreamId, SessionId, Position, NativeHeader, Message, Offer, Failure}
    use Nema.Aeron.Flow
    use Sys.Env

    pub def fixtureMessage(bytes: Vector[Int8]): Message =
        Message(bytes, NativeHeader(SessionId(1), StreamId(101), Position(0i64),
            Position(64i64), "fixture", 0, 0, 192, 0i64, 1i64))

    pub def fixtureRoundTrip(): Result[Failure, Bool] = {
        let bytes = String.toBytes("hello λ / opaque bytes");
        Flow.fixture(Ok(Offer.Accepted(Position(64i64))), Ok(fixtureMessage(bytes)),
            _ -> Flow.roundTrip(bytes))
    }

    pub def ipcRoundTrip(path: String): Result[Failure, Bool] \ IO = {
        let bytes = String.toBytes("hello λ / opaque bytes");
        Aeron.withDriver(path, _ ->
            Aeron.withClient(path, client ->
                Aeron.withSubscription(client, ChannelUri("aeron:ipc"), StreamId(101), 4, sub ->
                    Aeron.withPublication(client, ChannelUri("aeron:ipc"), StreamId(101), pub0 -> {
                        let cancel = Aeron.cancelToken();
                        let deadline = Aeron.deadlineAfterMillis(5000i64);
                        Flow.real(pub0, sub, cancel, deadline, _ -> Flow.roundTrip(bytes))
                    }))))
    }

    pub def main(): Unit \ {IO, Env} = {
        let runDir = Env.getVar("NEMA_AERON_RUN") |> Option.getWithDefault(".nema/demo");
        println("fixture: ${fixtureRoundTrip()}");
        println("IPC: ${ipcRoundTrip("${runDir}/ipc-driver")}");
        println("remaining handles: ${Aeron.resourceCount()}")
    }
}
