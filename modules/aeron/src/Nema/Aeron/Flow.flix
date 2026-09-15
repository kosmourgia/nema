/// One effect operation per message, never per byte.
pub mod Nema.Aeron.Flow {
    use Nema.Aeron.{Failure, Offer, Message, Publication, Subscription, Cancel, Deadline}
    use Nema.Aeron

    pub eff Flow {
        def send(bytes: Vector[Int8]): Result[Failure, Offer]
        def next(): Result[Failure, Message]
    }

    pub def real(pub0: Publication, sub: Subscription, cancel: Cancel, deadline: Deadline,
                 body: Unit -> a \ ef + Flow): a \ ef + IO =
        run { body() } with handler Flow {
            def send(bytes, resume) = resume(Aeron.publish(pub0, bytes, cancel, deadline))
            def next(_, resume) = resume(Aeron.receive(sub, cancel, deadline))
        }

    /// Scripted immutable fixture; same operation contracts, no JVM resources.
    pub def fixture(offer: Result[Failure, Offer], incoming: Result[Failure, Message],
                    body: Unit -> a \ ef + Flow): a \ ef =
        run { body() } with handler Flow {
            def send(_, resume) = resume(offer)
            def next(_, resume) = resume(incoming)
        }

    /// This exact program runs under fixture and live IPC handlers.
    pub def roundTrip(bytes: Vector[Int8]): Result[Failure, Bool] \ Flow =
        match Flow.send(bytes) {
            case Err(e) => Err(e)
            case Ok(Offer.Accepted(_)) => match Flow.next() {
                case Err(e) => Err(e)
                case Ok(m) => Ok(Aeron.messageBytes(m) == bytes)
            }
            case Ok(other) => Err(Failure.Native("publication did not accept: ${other}"))
        }
}
