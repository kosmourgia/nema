/// Independent-process interoperability entrypoints. The peer is NativePeer,
/// which uses raw Aeron Java APIs and never calls the transport bridge.
pub mod Nema.Aeron.Interop {
    use Nema.Aeron
    use Nema.Aeron.{Client, Subscription, Cancel, Deadline, Failure, Offer, SessionId}
    use Nema.Aeron.NativeHeader.NativeHeader
    use Nema.Aeron.Message.Message
    use Nema.Aeron.Workers
    use Sys.Env
    import nema.aeron.InteropSupport

    def env(name: String): String \ Env = Env.getVar(name) |> Option.getWithDefault("")
    def payload(): Vector[Int8] \ IO = Array.toVector(InteropSupport.payload())

    def waitFile(path: String, cancel: Cancel, deadline: Deadline): Result[Failure, String] \ IO =
        if (Aeron.isStopped(cancel)) Err(Failure.Cancelled)
        else if (Aeron.expired(deadline)) Err(Failure.Deadline)
        else if (InteropSupport.exists(path)) Ok(InteropSupport.read(path))
        else { Aeron.idle(cancel); waitFile(path, cancel, deadline) }

    def sendCount(client: Client, channel: String, cancel: Cancel, deadline: Deadline): Result[Failure, Unit] \ IO =
        Aeron.withPublication(client, Aeron.channel(channel), Aeron.stream(7101), pub0 -> {
            def send(n) = if (n == 0) Ok(()) else match Aeron.publish(pub0, payload(), cancel, deadline) {
                case Ok(Offer.Accepted(_)) => send(n - 1)
                case Ok(other) => Err(Failure.Native("interop offer: ${other}"))
                case Err(e) => Err(e)
            };
            let result = send(3);
            // Keep the publication image alive while the independent consumer drains.
            let linger = Aeron.deadlineAfterMillis(250i64);
            def drain() = if (Aeron.expired(linger)) () else { Aeron.idle(cancel); drain() };
            drain();
            result
        })

    pub def sendMain(): Unit \ {IO, Env} = {
        let driver = env("NEMA_AERON_INTEROP_DRIVER");
        let ready = env("NEMA_AERON_INTEROP_READY");
        let peer = env("NEMA_AERON_INTEROP_PEER");
        let result = Aeron.withDriver(driver, _ -> Aeron.withClient(driver, client -> {
            let cancel = Aeron.cancelToken();
            let deadline = Aeron.deadlineAfterMillis(30_000i64);
            InteropSupport.ready(ready, "ready");
            match waitFile(peer, cancel, deadline) {
                case Err(e) => Err(e)
                case Ok(channel) => match sendCount(client, channel, cancel, deadline) {
                    case Err(e) => Err(e)
                    case Ok(_) => sendCount(client, channel, cancel, deadline)
                }
            }
        }));
        match result {
            case Err(e) => InteropSupport.fail("Flix sender failed: ${e}")
            case Ok(_) => println("FLIX_SENT count=6 sessions=2 bytes=16384")
        };
        if (Aeron.resourceCount() != 0) InteropSupport.fail(Aeron.resources()) else ()
    }

    def resolved(sub: Subscription, cancel: Cancel, deadline: Deadline): Result[Failure, String] \ IO =
        if (Aeron.expired(deadline)) Err(Failure.Deadline) else {
            let channel = Aeron.resolvedChannel(sub);
            if (channel == "") { Aeron.idle(cancel); resolved(sub, cancel, deadline) } else Ok(channel)
        }

    pub def receiveMain(): Unit \ {IO, Env, Chan, NonDet} = {
        let driver = env("NEMA_AERON_INTEROP_DRIVER");
        let ready = env("NEMA_AERON_INTEROP_READY");
        let result = Aeron.withDriverCsp(driver, _ -> Aeron.withClientCsp(driver, client ->
            Aeron.withSubscriptionCsp(client, Aeron.channel("aeron:udp?endpoint=127.0.0.1:0"), Aeron.stream(7101), 2, sub -> {
                let cancel = Aeron.cancelToken();
                let deadline = Aeron.deadlineAfterMillis(30_000i64);
                match resolved(sub, cancel, deadline) {
                    case Err(e) => Err(e)
                    case Ok(channel) => {
                        InteropSupport.ready(ready, channel);
                        Workers.withWorker(sub, cancel, worker -> {
                            def collect(n, sessions: List[SessionId]) =
                                if (n == 0) Ok(List.length(sessions) == 2) else match Workers.next(worker, deadline) {
                                    case Err(e) => Err(e)
                                    case Ok(m) => {
                                        let Message(_, NativeHeader(session, _, _, _, _, _, _, _, _, _)) = m;
                                        if (Aeron.messageBytes(m) != payload()) Err(Failure.Native("interop byte mismatch"))
                                        else collect(n - 1, if (List.exists(x -> x == session, sessions)) sessions else session :: sessions)
                                    }
                                };
                            match collect(6, Nil) {
                                case Ok(true) => match Workers.next(worker, Aeron.deadlineAfterMillis(150i64)) {
                                    case Err(Failure.Deadline) => Ok(true)
                                    case other => Err(Failure.Native("unexpected extra interop delivery: ${other}"))
                                }
                                case other => other
                            }
                        })
                    }
                }
            })));
        match result {
            case Ok(true) => println("FLIX_RECEIVED count=6 sessions=2 bytes=16384")
            case other => InteropSupport.fail("Flix receiver failed: ${other}")
        };
        if (Aeron.resourceCount() != 0) InteropSupport.fail(Aeron.resources()) else ()
    }
}
