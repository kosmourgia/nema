use Nema.Aeron
use Nema.Aeron.{Failure, Offer}
use Nema.Aeron.Workers
use Sys.Env

def workerPath(name: String): String \ Env =
    let path = Env.getVar("NEMA_AERON_RUN") |> Option.getWithDefault(".nema/workers-test");
    "${path}/${name}"

@Test
def workerIdleStopAndOwnershipRelease(): Unit \ {Assert, IO, Env, Chan, NonDet} = {
    let path = workerPath("worker-idle");
    let result = Aeron.withDriverCsp(path, _ -> Aeron.withClientCsp(path, client ->
        Aeron.withSubscriptionCsp(client, Aeron.channel("aeron:ipc"), Aeron.stream(7201), 2, sub -> {
            let cancel = Aeron.cancelToken();
            match Workers.withWorker(sub, cancel, worker -> {
                let _ = Workers.next(worker, Aeron.deadlineAfterMillis(0i64));
                Ok(())
            }) {
                case Err(e) => Err(e)
                case Ok(_) => match Aeron.poll(sub, 1) {
                    case Err(e) => Err(e)
                    case Ok(_) => Ok(Aeron.isStopped(cancel))
                }
            }
        })));
    Assert.assertEq(expected = Ok(true), result)
}

@Test
def workerPendingReceiveCancellation(): Unit \ {Assert, IO, Env, Chan, NonDet} = {
    let path = workerPath("worker-cancel");
    let result = Aeron.withDriverCsp(path, _ -> Aeron.withClientCsp(path, client ->
        Aeron.withSubscriptionCsp(client, Aeron.channel("aeron:ipc"), Aeron.stream(7202), 2, sub -> {
            let cancel = Aeron.cancelToken();
            Workers.withWorker(sub, cancel, worker -> region rc {
                spawn {
                    let stopAt = Aeron.deadlineAfterMillis(30i64);
                    def wait() = if (Aeron.expired(stopAt)) Aeron.stop(cancel)
                        else { Aeron.idle(cancel); wait() };
                    wait()
                } @ rc;
                Ok(Workers.next(worker, Aeron.deadlineAfterMillis(5000i64)) == Err(Failure.Cancelled))
            })
        })));
    Assert.assertEq(expected = Ok(true), result)
}

@Test
def workerDeadlineKeepsScopeUsable(): Unit \ {Assert, IO, Env, Chan, NonDet} = {
    let path = workerPath("worker-deadline");
    let result = Aeron.withDriverCsp(path, _ -> Aeron.withClientCsp(path, client ->
        Aeron.withSubscriptionCsp(client, Aeron.channel("aeron:ipc"), Aeron.stream(7203), 2, sub -> {
            let cancel = Aeron.cancelToken();
            Workers.withWorker(sub, cancel, worker -> {
                let first = Workers.next(worker, Aeron.deadlineAfterMillis(20i64));
                let second = Workers.next(worker, Aeron.deadlineAfterMillis(20i64));
                Ok(first == Err(Failure.Deadline) and second == Err(Failure.Deadline))
            })
        })));
    Assert.assertEq(expected = Ok(true), result)
}

@Test
def workerSlowConsumerBoundedAndOrdered(): Unit \ {Assert, IO, Env, Chan, NonDet} = {
    let path = workerPath("worker-slow");
    let result = Aeron.withDriverCsp(path, _ -> Aeron.withClientCsp(path, client ->
        Aeron.withSubscriptionCsp(client, Aeron.channel("aeron:ipc"), Aeron.stream(7204), 2, sub ->
            Aeron.withPublicationCsp(client, Aeron.channel("aeron:ipc"), Aeron.stream(7204), pub0 -> {
                let cancel = Aeron.cancelToken();
                let deadline = Aeron.deadlineAfterMillis(5000i64);
                Workers.withWorker(sub, cancel, worker -> {
                    def send(n) = if (n == 6) Ok(()) else
                        match Aeron.publish(pub0, String.toBytes("sequence-${n}"), cancel, deadline) {
                            case Ok(Offer.Accepted(_)) => send(n + 1)
                            case Ok(other) => Err(Failure.Native("unexpected offer ${other}"))
                            case Err(e) => Err(e)
                        };
                    def full() = if (Aeron.queueFullCount(sub) > 0i64) Ok(Aeron.queued(sub) == 2)
                        else if (Aeron.expired(deadline)) Err(Failure.Deadline)
                        else { Aeron.idle(cancel); full() };
                    def collect(n) = if (n == 6) Ok(true) else match Workers.next(worker, deadline) {
                        case Err(e) => Err(e)
                        case Ok(m) => if (Aeron.messageBytes(m) == String.toBytes("sequence-${n}")) collect(n + 1)
                            else Ok(false)
                    };
                    match send(0) {
                        case Err(e) => Err(e)
                        case Ok(_) => match full() {
                            case Ok(true) => collect(0)
                            case other => other
                        }
                    }
                })
            }))));
    Assert.assertEq(expected = Ok(true), result)
}

@Test
def workerFailureIsReportedOnScopeExit(): Unit \ {Assert, IO, Env, Chan, NonDet} = {
    let path = workerPath("worker-failure");
    let result = Aeron.withDriverCsp(path, _ -> Aeron.withClientCsp(path, client ->
        Aeron.withSubscriptionCsp(client, Aeron.channel("aeron:ipc"), Aeron.stream(7205), 2, sub -> {
            // Deliberate ownership conflict injects a non-transient worker error.
            let _ = Aeron.poll(sub, 1);
            let cancel = Aeron.cancelToken();
            Workers.withWorker(sub, cancel, worker -> {
                let _ = Workers.next(worker, Aeron.deadlineAfterMillis(5000i64));
                Ok(())
            })
        })));
    Assert.assertTrue(match result { case Err(Failure.Native(_)) => true case _ => false })
}
