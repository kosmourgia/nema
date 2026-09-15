/// A scoped Flix CSP polling owner. Application callbacks never run on an
/// Aeron conductor thread; the only bridge callback copies into bounded staging.
pub mod Nema.Aeron.Workers {
    use Nema.Aeron
    use Nema.Aeron.{Subscription, Cancel, Deadline, Failure, Message}
    import java.lang.Throwable
    import java.util.concurrent.atomic.AtomicBoolean
    use Nema.Aeron.Workers.Pull.Pull
    use Nema.Aeron.Workers.Worker.Worker

    enum Pull(Deadline, Sender[Result[Failure, Message]])
    pub enum Worker(Sender[Pull], Cancel, AtomicBoolean)

    eff Source {
        def poll(): Result[Failure, Int32]
        def take(): Result[Failure, Option[Message]]
    }

    /// The body may issue one outstanding demand. Each demand owns a fresh
    /// capacity-one reply channel: cancelling/abandoning its receiver cannot
    /// block the worker's reply. Stop is level-triggered and wakes idle waits.
    pub def withWorker(sub: Subscription, cancel: Cancel,
                       body: Worker -> Result[Failure, a] \ {IO, Chan, NonDet}): Result[Failure, a] \ {IO, Chan, NonDet} = region rc {
        let (tx, rx) = Channel.buffered(1);
        let (doneTx, doneRx) = Channel.buffered(1);
        let pending = new AtomicBoolean(false);
        spawn {
            let result = try {
                run { loop(cancel, rx, None) } with handler Source {
                    def poll(_, resume) = resume(Aeron.poll(sub, 32))
                    def take(_, resume) = resume(Aeron.take(sub))
                }
            } catch { case ex: Throwable => Some(Failure.Native(ex.toString())) };
            let released = Aeron.releasePollOwner(sub);
            let finalResult = match (result, released) {
                case (Some(e), _) => Some(e)
                case (None, Err(e)) => Some(e)
                case (None, Ok(_)) => None
            };
            Aeron.stop(cancel);
            Channel.send(finalResult, doneTx)
        } @ rc;
        let result = try { body(Worker(tx, cancel, pending)) }
            catch { case ex: Throwable => Err(Failure.Native(ex.toString())) };
        Aeron.stop(cancel);
        let workerResult = Channel.recv(doneRx);
        match (result, workerResult) {
            case (r, None) => r
            case (Ok(_), Some(e)) => Err(e)
            case (Err(e), Some(w)) => Err(Failure.Native("body: ${e}; worker: ${w}"))
        }
    }

    /// Pull one owned message. Concurrent demands fail explicitly. Calls after
    /// scope Stop return Cancelled. No application data is discarded on timeout:
    /// a demand that times out before delivery leaves staging data untouched.
    pub def next(worker: Worker, deadline: Deadline): Result[Failure, Message] \ {IO, Chan, NonDet} = {
        let Worker(tx, cancel, pending) = worker;
        if (Aeron.isStopped(cancel)) Err(Failure.Cancelled)
        else if (Aeron.expired(deadline)) Err(Failure.Deadline)
        else if (not pending.compareAndSet(false, true)) Err(Failure.Native("worker already has an outstanding demand"))
        else {
            let (replyTx, replyRx) = Channel.buffered(1);
            Channel.send(Pull(deadline, replyTx), tx);
            let result = awaitReply(replyRx, cancel);
            pending.set(false);
            result
        }
    }

    def awaitReply(rx: Receiver[Result[Failure, Message]], cancel: Cancel): Result[Failure, Message] \ {IO, Chan, NonDet} =
        select {
            case result <- recv(rx) => result
            case _ => if (Aeron.isStopped(cancel)) Err(Failure.Cancelled)
                      else { Aeron.idle(cancel); awaitReply(rx, cancel) }
        }

    def loop(cancel: Cancel, requests: Receiver[Pull],
             failure: Option[Failure]): Option[Failure] \ {IO, Chan, NonDet, Source} =
        if (Aeron.isStopped(cancel)) failure else select {
            case request <- recv(requests) =>
                let Pull(deadline, reply) = request;
                let result = match failure {
                    case Some(e) => Err(e)
                    case None => receive(cancel, deadline)
                };
                Channel.send(result, reply);
                let nextFailure = match result {
                    case Err(Failure.Native(s)) => Some(Failure.Native(s))
                    case _ => failure
                };
                loop(cancel, requests, nextFailure)
            case _ =>
                let nextFailure = match failure {
                    case Some(e) => Some(e)
                    case None => match Source.poll() {
                        case Err(e) => Some(e)
                        case Ok(_) => None
                    }
                };
                Aeron.idle(cancel);
                loop(cancel, requests, nextFailure)
        }

    def receive(cancel: Cancel, deadline: Deadline): Result[Failure, Message] \ {IO, Source} =
        if (Aeron.isStopped(cancel)) Err(Failure.Cancelled)
        else if (Aeron.expired(deadline)) Err(Failure.Deadline)
        else match Source.take() {
            case Err(e) => Err(e)
            case Ok(Some(message)) => Ok(message)
            case Ok(None) => match Source.poll() {
                case Err(e) => Err(e)
                case Ok(_) => { Aeron.idle(cancel); receive(cancel, deadline) }
            }
        }
}
