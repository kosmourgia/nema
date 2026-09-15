use Nema.Aeron
use Nema.Aeron.{Failure, Offer, Publication, Cancel, Deadline}
use Nema.Aeron.ChannelUri.ChannelUri
use Nema.Aeron.StreamId.StreamId
use Nema.Aeron.Position.Position
use Nema.Aeron.ArchiveId.ArchiveId
use Nema.Aeron.ArchiveIdentity.ArchiveIdentity
use Nema.Aeron.RecordingId.RecordingId
use Nema.Aeron.RecordingKey.RecordingKey
use Nema.Aeron.RecordingRange.RecordingRange
use Nema.Aeron.Archive
use Nema.Aeron.Sink
use Sys.Env
import java.lang.Thread

def reachBackpressure(p: Publication, cancel: Cancel, deadline: Deadline): Bool \ IO =
    if (Aeron.expired(deadline)) false else match Aeron.tryPublish(p, String.toBytes(String.repeat(4096, "x"))) {
        case Ok(Offer.Backpressured) => true
        case Ok(Offer.Accepted(_)) => reachBackpressure(p, cancel, deadline)
        case Ok(Offer.AdministrativeRetry) => Aeron.idle(cancel); reachBackpressure(p, cancel, deadline)
        case Ok(Offer.NotConnected) => Aeron.idle(cancel); reachBackpressure(p, cancel, deadline)
        case _ => false
    }

@Test
def flixRetryCancellationDuringBackpressure(): Unit \ {Assert, IO, Env, Chan, NonDet} = {
    let runDir = Env.getVar("NEMA_AERON_RUN") |> Option.getWithDefault(".nema/test");
    let path = "${runDir}/flix-pressure-driver";
    let uri = ChannelUri("aeron:ipc?term-length=65536|mtu=1408");
    let result = Aeron.withDriverCsp(path, _ -> Aeron.withClientCsp(path, client ->
        Aeron.withSubscriptionCsp(client, uri, StreamId(901), 1, _sub ->
            Aeron.withPublicationCsp(client, uri, StreamId(901), p -> region rc {
                let cancel = Aeron.cancelToken();
                let deadline = Aeron.deadlineAfterMillis(3000i64);
                let pressured = reachBackpressure(p, cancel, deadline);
                let (tx, rx) = Channel.buffered(1);
                spawn { Thread.sleep(30i64); Aeron.stop(cancel); Channel.send((), tx) } @ rc;
                let outcome = Aeron.publish(p, String.toBytes(String.repeat(4096, "x")), cancel, deadline);
                Channel.recv(rx);
                Ok(pressured and outcome == Err(Failure.Cancelled))
            }))));
    Assert.assertEq(expected = Ok(true), result)
}

def awaitFirstReplayFragment(replay: Archive.Replay, cancel: Cancel, deadline: Deadline): Bool \ IO =
    if (Aeron.expired(deadline)) false else match Archive.pollReplay(replay, 1) {
        case Err(_) => false
        case Ok(n) => if (n > 0) true else { Aeron.idle(cancel); awaitFirstReplayFragment(replay, cancel, deadline) }
    }

@Test
def flixCancellationDuringFragmentedReplay(): Unit \ {Assert, IO, Env} = {
    let runDir = Env.getVar("NEMA_AERON_RUN") |> Option.getWithDefault(".nema/test");
    let path = "${runDir}/flix-replay-driver";
    let result = Aeron.withDriver(path, _ -> Archive.withArchive(path, "${runDir}/flix-replay-archive",
        ArchiveId(73001i64), 65536, 2, archive -> Aeron.withClient(path, client -> {
            let uri = ChannelUri("aeron:ipc?term-length=65536|mtu=1408");
            match Archive.startRecording(archive, uri, StreamId(902)) {
                case Err(e) => Err(e)
                case Ok(registration) => Aeron.withPublication(client, uri, StreamId(902), pub0 -> {
                    let cancel = Aeron.cancelToken();
                    let deadline = Aeron.deadlineAfterMillis(3000i64);
                    let sent = Aeron.publish(pub0, String.toBytes(String.repeat(7000, "z")), cancel, deadline);
                    match (sent, Archive.awaitRecording(archive, StreamId(902), Aeron.publicationSession(pub0), cancel, deadline)) {
                        case (Ok(Offer.Accepted(end)), Ok(key)) =>
                            match Archive.awaitRecorded(archive, key, end, cancel, deadline) {
                                case Err(e) => Err(e)
                                case Ok(_) => Archive.withReplay(archive, RecordingRange(key, Position(0i64), end), StreamId(903), 1, replay -> {
                                    let started = awaitFirstReplayFragment(replay, cancel, deadline);
                                    Aeron.stop(cancel);
                                    let result0 = Archive.nextReplay(replay, cancel, deadline);
                                    let stopped = Archive.stopRecording(archive, registration);
                                    Ok(started and result0 == Err(Failure.Cancelled) and stopped == Ok(()))
                                })
                            }
                        case _ => Err(Failure.Native("failed to establish replay source"))
                    }
                })
            }
        })));
    Assert.assertEq(expected = Ok(true), result)
}

@Test
def flixSinkHoleBlocksAckUntilCancelledOrDeadline(): Unit \ {Assert, IO, Env, Chan, NonDet} = {
    let runDir = Env.getVar("NEMA_AERON_RUN") |> Option.getWithDefault(".nema/test");
    let path = "${runDir}/flix-hole-driver";
    let result = Aeron.withDriverCsp(path, _ -> Aeron.withClientCsp(path, client ->
        Aeron.withSubscriptionCsp(client, ChannelUri("aeron:ipc"), StreamId(904), 1, sub ->
            Aeron.scopeCsp(Sink.open("${runDir}/flix-hole-sink"), Sink.close, sink -> region rc {
                let key = RecordingKey(ArchiveIdentity("hole-test"), RecordingId(1i64));
                let committed = Sink.commit(sink, key, "later", Position(128i64), Position(192i64), Vector#{0i8, -1i8});
                let deadlineResult = Sink.await(sink, key, Position(0i64), Position(192i64), sub,
                    Aeron.cancelToken(), Aeron.deadlineAfterMillis(20i64));
                let cancel = Aeron.cancelToken();
                let (tx, rx) = Channel.buffered(1);
                spawn { Thread.sleep(30i64); Aeron.stop(cancel); Channel.send((), tx) } @ rc;
                let cancelled = Sink.await(sink, key, Position(0i64), Position(192i64), sub,
                    cancel, Aeron.deadlineAfterMillis(3000i64));
                Channel.recv(rx);
                Ok(match committed { case Ok(_) => true case _ => false } and
                    deadlineResult == Err(Failure.Deadline) and cancelled == Err(Failure.Cancelled))
            }))));
    Assert.assertEq(expected = Ok(true), result)
}
