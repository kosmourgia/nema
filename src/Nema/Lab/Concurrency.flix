pub mod Nema.Lab.Concurrency {

    pub eff WorkerRequest {
        def value(): String
    }

    pub def channelRoundTrip(): Int32 \ {Chan, NonDet} = region rc {
        let (sender, receiver) = Channel.unbuffered();
        spawn Channel.send(42, sender) @ rc;
        Channel.recv(receiver)
    }

    /// The custom handler is installed inside the spawned worker. This is the
    /// required boundary: user-defined dynamic effect scope does not cross the
    /// spawn expression.
    pub def handledInsideWorker(): String \ {Chan, NonDet} = region rc {
        let (sender, receiver) = Channel.unbuffered();
        spawn run {
            Channel.send(WorkerRequest.value(), sender)
        } with handler WorkerRequest {
            def value(_, resume) = resume("handled-in-worker")
        } @ rc;
        Channel.recv(receiver)
    }
}
