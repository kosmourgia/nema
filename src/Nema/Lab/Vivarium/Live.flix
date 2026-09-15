pub mod Nema.Lab.Vivarium.Live {

    pub enum PulseSignal with Eq, ToString {
        case Beat(Int32)
        case Stop
    }

    pub enum BindingKind with Eq, Order, ToString {
        case Live
        case Durable(String)
        case Resolved(String)
        case Dropped
    }

    pub enum Invocation with Eq, ToString {
        case Notify(Int32, Int32, Int32, String)
        case Ask(Int32, Int32, Int32, Int32, String)
    }

    pub type alias Publication = {
        invocation = Invocation,
        closedRevision = Int32
    }

    pub enum Proposal with Eq, ToString {
        case None
        case Notify(String, Bool)
    }

    pub enum ResolutionCommand with Eq, ToString {
        case Rebind(Int32, BindingKind)
        case Resolve(Int32, String, Int32)
    }

    pub enum SinkSignal {
        case Deliver(Publication)
        case Stop
    }

    pub enum LiveEvent with Eq, ToString {
        case Born(Int32, Int32)
        case Representation(Int32, String)
        case Attached(Int32, Int32)
        case SinkWaiting(Int32)
        case Waiting(Int32, Int32)
        case Downbeat(Int32, Int32)
        case Awoke(Int32, Int32, Int32)
        case Systole(Int32, Int32, Int32)
        case StagedRevision(Int32)
        case StagedIntent(Int32, String)
        case Aborted(Int32)
        case Committed(Int32, Int32, String)
        case Diastole(Int32)
        case Delivery(Int32, Int32, Int32, String)
        case Settled(Int32, Int32)
        case AskPending(Int32, Int32)
        case BindingChanged(Int32, BindingKind)
        case Resumed(Int32, String, Int32)
        case Activity(Int32, Int32, String)
        case Detached(Int32, Int32)
        case MeasureGathering(Int32, Int32)
        case MeasureOpened(Int32, Int32, Int32)
        case ObligationCompleted(Int32, String)
        case MeasureClosed(Int32, Int32, Int32)
    }

    pub type alias RuntimeState = {
        object = Int32,
        revision = Int32,
        revisions = List[Int32],
        value = String,
        nextRevision = Int32,
        nextIntent = Int32,
        nextInvocation = Int32,
        invocations = List[Invocation],
        continuation = Option[(Int32, BindingKind)]
    }

    pub type alias LiveMeasure = {
        id = Int32,
        p3Beats = List[Int32],
        p4Beats = List[Int32],
        gatheringAfterFirst = Bool,
        opening = List[(Int32, Int32)],
        openBeforeAllObligations = Bool,
        closing = List[(Int32, Int32)],
        obligations = Set[String]
    }

    pub type alias Topology = {
        objects = Set[Int32],
        pulses = Set[Int32],
        drivenBy = Set[(Int32, String)],
        observers = Set[Int32],
        hasPulse = Set[(Int32, Int32)],
        waiting = Set[(Int32, Int32)],
        registrations = Set[(Int32, String, Int32)],
        continuations = Set[Int32],
        boundTo = Set[(Int32, String)],
        pending = Set[Int32],
        invocations = Set[Int32],
        origins = Set[(Int32, Int32)],
        measures = Set[Int32],
        participates = Set[(Int32, Int32)],
        ready = Set[(Int32, Int32)],
        measureOpen = Set[Int32],
        obligations = Set[(Int32, String)],
        complete = Set[(Int32, String)]
    }

    pub type alias QueryView = {
        liveObservers = Set[Int32],
        dormantObjects = Set[Int32],
        waitingOnPulse = Set[(Int32, Int32)],
        pendingContinuations = Set[Int32],
        reachableSubjects = Set[(Int32, Int32)],
        participantReady = Set[(Int32, Int32)],
        measuresReady = Set[Int32],
        measuresClosed = Set[Int32]
    }

    pub type alias LiveResult = {
        events = List[LiveEvent],
        trace = String,
        initialState = RuntimeState,
        afterAbort = RuntimeState,
        afterNotify = RuntimeState,
        afterAsk = RuntimeState,
        finalState = RuntimeState,
        notifyInvocation = Invocation,
        askInvocation = Invocation,
        liveInterpretations = Set[String],
        pureInterpretations = Set[String],
        bindingHistory = List[BindingKind],
        resumedBeat = Int32,
        attachedView = QueryView,
        pendingView = QueryView,
        finalView = QueryView,
        attachedTopology = Topology,
        pendingTopology = Topology,
        finalTopology = Topology,
        measure = LiveMeasure
    }

    type alias PrimaryDriver = {
        eventRx = Receiver[LiveEvent],
        pulseTx = Sender[PulseSignal],
        publicationRx = Receiver[Publication],
        dispatchAckTx = Sender[Unit],
        resolutionTx = Sender[ResolutionCommand],
        snapshotRx = Receiver[RuntimeState],
        finalRx = Receiver[RuntimeState],
        recorderTx = Sender[SinkSignal],
        witnessTx = Sender[SinkSignal]
    }

    type alias PrimaryResult = {
        events = List[LiveEvent],
        initialState = RuntimeState,
        afterAbort = RuntimeState,
        afterNotify = RuntimeState,
        afterAsk = RuntimeState,
        finalState = RuntimeState,
        notifyInvocation = Invocation,
        askInvocation = Invocation
    }

    type alias MeasureResult = {
        events = List[LiveEvent],
        measure = LiveMeasure
    }

    /// `next` captures the Observer's continuation. Its worker-local handler
    /// announces Waiting, blocks on a CSP channel, and resumes on a Beat.
    pub eff Pulse {
        def next(): PulseSignal
    }

    /// This language-level effect is reduced to an intent before commit.
    pub eff Stage {
        def notify(message: String, commit: Bool): Unit
    }

    /// The handler holds the real continuation while awaiting a Resolution.
    pub eff Ask {
        def ask(address: Int32, intent: Int32, invocation: Int32, origin: Int32, closedRevision: Int32, question: String): String
    }

    pub eff BeatContext {
        def current(): Int32
    }

    def initialState(): RuntimeState = {
        object = 17,
        revision = 40,
        revisions = List#{40},
        value = "asleep",
        nextRevision = 41,
        nextIntent = 8,
        nextInvocation = 8,
        invocations = Nil,
        continuation = None
    }

    def captureNotify(message: String, commit: Bool): Proposal =
        run {
            Stage.notify(message, commit);
            Proposal.None
        } with handler Stage {
            def notify(message0, commit0, resume) =
                let _ignored = resume();
                Proposal.Notify(message0, commit0)
        }

    def invocationId(invocation: Invocation): Int32 = match invocation {
        case Invocation.Notify(id, _, _, _) => id
        case Invocation.Ask(id, _, _, _, _) => id
    }

    def applyNotify(
        pulse: Int32,
        beat: Int32,
        proposal: Proposal,
        state: RuntimeState,
        eventTx: Sender[LiveEvent],
        publicationTx: Sender[Publication],
        dispatchAckRx: Receiver[Unit],
        snapshotTx: Sender[RuntimeState]
    ): RuntimeState \ {Chan, NonDet} = match proposal {
        case Proposal.None => state
        case Proposal.Notify(message, commit) => {
            let revision = state#nextRevision;
            let intent = state#nextIntent;
            Channel.send(LiveEvent.Systole(pulse, beat, state#revision), eventTx);
            Channel.send(LiveEvent.StagedRevision(revision), eventTx);
            Channel.send(LiveEvent.StagedIntent(intent, "Notify"), eventTx);
            if (not commit) {
                Channel.send(LiveEvent.Aborted(beat), eventTx);
                Channel.send(LiveEvent.Settled(pulse, beat), eventTx);
                Channel.send(state, snapshotTx);
                state
            } else {
                let iid = state#nextInvocation;
                let invocation = Invocation.Notify(iid, intent, state#object, message);
                let next = {
                    revision = revision,
                    revisions = revision :: state#revisions,
                    value = message,
                    nextRevision = revision + 1,
                    nextIntent = intent + 1,
                    nextInvocation = iid + 1,
                    invocations = invocation :: state#invocations
                    | state
                };
                Channel.send(LiveEvent.Committed(intent, iid, "Notify"), eventTx);
                Channel.send({invocation = invocation, closedRevision = next#revision}, publicationTx);
                Channel.send(LiveEvent.Diastole(iid), eventTx);
                Channel.recv(dispatchAckRx);
                Channel.send(LiveEvent.Settled(pulse, beat), eventTx);
                Channel.send(next, snapshotTx);
                next
            }
        }
    }

    def observerLoop(
        pulse: Int32,
        state: RuntimeState,
        eventTx: Sender[LiveEvent],
        publicationTx: Sender[Publication],
        dispatchAckRx: Receiver[Unit],
        snapshotTx: Sender[RuntimeState]
    ): RuntimeState \ {Ask, BeatContext, Chan, NonDet, Pulse} = match Pulse.next() {
        case PulseSignal.Stop =>
            Channel.send(LiveEvent.Detached(state#object, pulse), eventTx);
            state
        case PulseSignal.Beat(beat) if beat == 1 =>
            let next = applyNotify(pulse, beat, captureNotify("discard me", false), state, eventTx, publicationTx, dispatchAckRx, snapshotTx);
            observerLoop(pulse, next, eventTx, publicationTx, dispatchAckRx, snapshotTx)
        case PulseSignal.Beat(beat) if beat == 2 =>
            let next = applyNotify(pulse, beat, captureNotify("awake", true), state, eventTx, publicationTx, dispatchAckRx, snapshotTx);
            observerLoop(pulse, next, eventTx, publicationTx, dispatchAckRx, snapshotTx)
        case PulseSignal.Beat(beat) if beat == 3 => {
            Channel.send(LiveEvent.Systole(pulse, beat, state#revision), eventTx);
            let address = 17;
            let intent = state#nextIntent;
            let iid = state#nextInvocation;
            let question = "continue?";
            let answer = Ask.ask(address, intent, iid, state#object, state#revision, question);
            let resumedBeat = BeatContext.current();
            let invocation = Invocation.Ask(iid, intent, state#object, address, question);
            let next = {
                nextIntent = intent + 1,
                nextInvocation = iid + 1,
                invocations = invocation :: state#invocations,
                continuation = Some((address, BindingKind.Resolved(answer)))
                | state
            };
            Channel.send(LiveEvent.Resumed(address, answer, resumedBeat), eventTx);
            Channel.send(next, snapshotTx);
            observerLoop(pulse, next, eventTx, publicationTx, dispatchAckRx, snapshotTx)
        }
        case PulseSignal.Beat(beat) => {
            Channel.send(LiveEvent.Activity(pulse, beat, "observer heartbeat"), eventTx);
            Channel.send(LiveEvent.Settled(pulse, beat), eventTx);
            Channel.send(state, snapshotTx);
            observerLoop(pulse, state, eventTx, publicationTx, dispatchAckRx, snapshotTx)
        }
    }

    def runObserver(
        object: Int32,
        pulse: Int32,
        state: RuntimeState,
        pulseRx: Receiver[PulseSignal],
        eventTx: Sender[LiveEvent],
        publicationTx: Sender[Publication],
        dispatchAckRx: Receiver[Unit],
        resolutionRx: Receiver[ResolutionCommand],
        snapshotTx: Sender[RuntimeState]
    ): RuntimeState \ {Chan, NonDet} =
        run {
            run {
                run {
                    observerLoop(pulse, state, eventTx, publicationTx, dispatchAckRx, snapshotTx)
                } with handler Ask {
                    def ask(address, intent, iid, origin, closedRevision, question, resume) = {
                        let invocation = Invocation.Ask(iid, intent, origin, address, question);
                        Channel.send(LiveEvent.StagedIntent(intent, "Ask"), eventTx);
                        Channel.send(LiveEvent.Committed(intent, iid, "Ask"), eventTx);
                        Channel.send({invocation = invocation, closedRevision = closedRevision}, publicationTx);
                        Channel.send(LiveEvent.Diastole(iid), eventTx);
                        Channel.send(LiveEvent.AskPending(address, iid), eventTx);
                        match Channel.recv(resolutionRx) {
                            case ResolutionCommand.Rebind(address0, binding) =>
                                Channel.send(LiveEvent.BindingChanged(address0, binding), eventTx)
                            case ResolutionCommand.Resolve(_, _, _) => ()
                        };
                        match Channel.recv(resolutionRx) {
                            case ResolutionCommand.Rebind(address0, binding) =>
                                Channel.send(LiveEvent.BindingChanged(address0, binding), eventTx)
                            case ResolutionCommand.Resolve(_, _, _) => ()
                        };
                        match Channel.recv(resolutionRx) {
                            case ResolutionCommand.Resolve(address0, answer, laterBeat) => {
                                Channel.send(LiveEvent.BindingChanged(address0, BindingKind.Resolved(answer)), eventTx);
                                run { resume(answer) } with handler BeatContext {
                                    def current(_, k) = k(laterBeat)
                                }
                            }
                            case ResolutionCommand.Rebind(_, _) =>
                                run { resume("unresolved") } with handler BeatContext {
                                    def current(_, k) = k(-1)
                                }
                        }
                    }
                }
            } with handler BeatContext {
                def current(_, resume) = resume(-1)
            }
        } with handler Pulse {
            def next(_, resume) = {
                Channel.send(LiveEvent.Waiting(object, pulse), eventTx);
                let signal = Channel.recv(pulseRx);
                match signal {
                    case PulseSignal.Beat(beat) =>
                        Channel.send(LiveEvent.Awoke(object, pulse, beat), eventTx);
                        resume(signal)
                    case PulseSignal.Stop => resume(signal)
                }
            }
        }

    def sink(
        object: Int32,
        interpretation: String,
        signalRx: Receiver[SinkSignal],
        eventTx: Sender[LiveEvent]
    ): Unit \ {Chan, NonDet} = {
        Channel.send(LiveEvent.SinkWaiting(object), eventTx);
        match Channel.recv(signalRx) {
            case SinkSignal.Deliver(publication) => {
                let iid = invocationId(publication#invocation);
                let message = match publication#invocation {
                    case Invocation.Notify(_, _, _, value) => value
                    case Invocation.Ask(_, _, _, _, question) => question
                };
                Channel.send(LiveEvent.Delivery(iid, object, publication#closedRevision, "${interpretation}:${message}"), eventTx)
            }
            case SinkSignal.Stop => ()
        };
        match Channel.recv(signalRx) {
            case SinkSignal.Stop => Channel.send(LiveEvent.Detached(object, 3), eventTx)
            case SinkSignal.Deliver(_) => ()
        }
    }

    def measureLoop(object: Int32, pulse: Int32, eventTx: Sender[LiveEvent]): Unit \ {Chan, Pulse} =
        match Pulse.next() {
            case PulseSignal.Stop => Channel.send(LiveEvent.Detached(object, pulse), eventTx)
            case PulseSignal.Beat(beat) => {
                Channel.send(LiveEvent.Activity(pulse, beat, "measure participant"), eventTx);
                measureLoop(object, pulse, eventTx)
            }
        }

    def runMeasureParticipant(
        object: Int32,
        pulse: Int32,
        pulseRx: Receiver[PulseSignal],
        eventTx: Sender[LiveEvent]
    ): Unit \ {Chan, NonDet} =
        run {
            measureLoop(object, pulse, eventTx)
        } with handler Pulse {
            def next(_, resume) = {
                Channel.send(LiveEvent.Waiting(object, pulse), eventTx);
                let signal = Channel.recv(pulseRx);
                match signal {
                    case PulseSignal.Beat(beat) =>
                        Channel.send(LiveEvent.Awoke(object, pulse, beat), eventTx);
                        resume(signal)
                    case PulseSignal.Stop => resume(signal)
                }
            }
        }

    def receiveMany(n: Int32, rx: Receiver[LiveEvent]): List[LiveEvent] \ {Chan, NonDet} =
        if (n <= 0) Nil else Channel.recv(rx) :: receiveMany(n - 1, rx)

    def driveMeasureBeat(pulse: Int32, beat: Int32, tx: Sender[PulseSignal], rx: Receiver[LiveEvent]): List[LiveEvent] \ {Chan, NonDet} = {
        Channel.send(PulseSignal.Beat(beat), tx);
        LiveEvent.Downbeat(pulse, beat) :: receiveMany(3, rx)
    }

    /// Flix 0.75.3 waits for a region child blocked in `Channel.recv`; region
    /// exit is not an implicit cancellation signal. Shutdown is therefore
    /// cooperative: Stop resumes the waiter, it detaches, then scope closes.
    pub def scopeShutdownProbe(): (Int32, Int32, Bool) \ {Chan, NonDet} = {
        let detached = region rc {
            let (eventTx, eventRx) = Channel.unbuffered();
            let (pulseTx, pulseRx) = Channel.unbuffered();
            spawn runMeasureParticipant(17, 3, pulseRx, eventTx) @ rc;
            let waiting = match Channel.recv(eventRx) {
                case LiveEvent.Waiting(17, 3) => true
                case _ => false
            };
            Channel.send(PulseSignal.Stop, pulseTx);
            waiting and match Channel.recv(eventRx) {
                case LiveEvent.Detached(17, 3) => true
                case _ => false
            }
        };
        (17, 40, detached)
    }

    def bindingName(binding: BindingKind): String = match binding {
        case BindingKind.Live => "Live"
        case BindingKind.Durable(recipe) => "Durable:${recipe}"
        case BindingKind.Resolved(answer) => "Resolved:${answer}"
        case BindingKind.Dropped => "Dropped"
    }

    def topology(active: Bool, waiting: Bool, continuation: Option[BindingKind], measure: Option[LiveMeasure]): Topology = {
        let observerFacts = if (active) Set#{17, 18, 19} else Set.empty();
        let hasPulseFacts = if (active) Set#{(17, 3), (18, 3), (19, 3)} else Set.empty();
        let waitingFacts = if (waiting) Set#{(17, 3)} else Set.empty();
        let (continuationFacts, boundFacts, pendingFacts, invocationFacts, originFacts) = match continuation {
            case None => (Set.empty(), Set.empty(), Set.empty(), Set.empty(), Set.empty())
            case Some(binding) => (
                Set#{17},
                Set#{(17, bindingName(binding))},
                if (binding == BindingKind.Live or match binding { case BindingKind.Durable(_) => true case _ => false }) Set#{17} else Set.empty(),
                Set#{8, 9},
                Set#{(8, 17), (9, 17)}
            )
        };
        let (pulseFacts, driverFacts, measureFacts, participatesFacts, readyFacts, openFacts, obligationFacts, completeFacts) = match measure {
            case None => (Set#{3}, Set#{(3, "manual-p3")}, Set.empty(), Set.empty(), Set.empty(), Set.empty(), Set.empty(), Set.empty())
            case Some(_) => (
                Set#{3, 4}, Set#{(3, "manual-p3"), (4, "manual-p4")},
                Set#{2}, Set#{(2, 3), (2, 4)}, Set#{(2, 3), (2, 4)}, Set#{2},
                Set#{(2, "question"), (2, "answer")}, Set#{(2, "question"), (2, "answer")}
            )
        };
        {
            objects = Set#{17, 18, 19},
            pulses = pulseFacts,
            drivenBy = driverFacts,
            observers = observerFacts,
            hasPulse = hasPulseFacts,
            waiting = waitingFacts,
            registrations = Set#{(18, "specimen", 17), (19, "same specimen", 17)},
            continuations = continuationFacts,
            boundTo = boundFacts,
            pending = pendingFacts,
            invocations = invocationFacts,
            origins = originFacts,
            measures = measureFacts,
            participates = participatesFacts,
            ready = readyFacts,
            measureOpen = openFacts,
            obligations = obligationFacts,
            complete = completeFacts
        }
    }

    /// A pure projection. No rule can send a Beat, resume a continuation, or
    /// dispatch an invocation.
    pub def queryTopology(topology0: Topology): QueryView = {
        let rules = #{
            LiveObserver(o) :- Object(o), Observer(o), HasPulse(o, p), Pulse(p), DrivenBy(p, driver).
            DormantObject(o) :- Object(o), not Observer(o).
            PendingContinuation(c) :- Continuation(c), BoundTo(c, kind), Pending(c).
            ReachableSubject(o, s) :- Registration(o, name, s).
            ParticipantReady(m, p) :- Participates(m, p), Ready(m, p).
            HasParticipant(m) :- Participates(m, p).
            MissingReady(m) :- Participates(m, p), not Ready(m, p).
            MeasureReady(m) :- Measure(m), HasParticipant(m), not MissingReady(m).
            HasObligation(m) :- Obligation(m, x).
            Incomplete(m) :- Obligation(m, x), not Complete(m, x).
            MeasureClosed(m) :- Measure(m), MeasureOpen(m), HasObligation(m), not Incomplete(m).
        };
        let facts = inject
            topology0#objects, topology0#pulses, topology0#drivenBy,
            topology0#observers, topology0#hasPulse, topology0#waiting,
            topology0#registrations, topology0#continuations, topology0#boundTo,
            topology0#pending, topology0#invocations, topology0#origins,
            topology0#measures, topology0#participates, topology0#ready,
            topology0#measureOpen, topology0#obligations, topology0#complete
            into Object/1, Pulse/1, DrivenBy/2, Observer/1, HasPulse/2,
            WaitingOnPulse/2, Registration/3, Continuation/1, BoundTo/2,
            Pending/1, Invocation/1, Origin/2, Measure/1, Participates/2,
            Ready/2, MeasureOpen/1, Obligation/2, Complete/2;
        {
            liveObservers = query facts, rules select o from LiveObserver(o) |> Vector.toSet,
            dormantObjects = query facts, rules select o from DormantObject(o) |> Vector.toSet,
            waitingOnPulse = query facts, rules select (o, p) from WaitingOnPulse(o, p) |> Vector.toSet,
            pendingContinuations = query facts, rules select c from PendingContinuation(c) |> Vector.toSet,
            reachableSubjects = query facts, rules select (o, s) from ReachableSubject(o, s) |> Vector.toSet,
            participantReady = query facts, rules select (m, p) from ParticipantReady(m, p) |> Vector.toSet,
            measuresReady = query facts, rules select m from MeasureReady(m) |> Vector.toSet,
            measuresClosed = query facts, rules select m from MeasureClosed(m) |> Vector.toSet
        }
    }

    def pureReference(): Set[String] = {
        let (w1, subject) = Nema.Lab.Vivarium.birth("subject", Nema.Lab.Vivarium.Value.Text("asleep"), Nema.Lab.Vivarium.empty());
        let (w2, pulse) = Nema.Lab.Vivarium.newPulse("reference", w1);
        let w3 = Nema.Lab.Vivarium.animate(subject, pulse, Nema.Lab.Vivarium.Reaction.Notifier, w2);
        let (w4, recorder) = Nema.Lab.Vivarium.birth("recorder", Nema.Lab.Vivarium.Value.Text("empty"), w3);
        let (w5, witness) = Nema.Lab.Vivarium.birth("witness", Nema.Lab.Vivarium.Value.Text("empty"), w4);
        let w6 = Nema.Lab.Vivarium.animate(recorder, pulse, Nema.Lab.Vivarium.Reaction.Recorder, w5) |> Nema.Lab.Vivarium.register(recorder, subject, "specimen", pulse);
        let w7 = Nema.Lab.Vivarium.animate(witness, pulse, Nema.Lab.Vivarium.Reaction.Witness, w6) |> Nema.Lab.Vivarium.register(witness, subject, "same specimen", pulse);
        let (_, report) = Nema.Lab.Vivarium.tickNotify(pulse, subject, "awake", true, w7);
        Nema.Lab.Vivarium.deliveryInterpretations(report)
    }

    def interpretation(event: LiveEvent): Option[String] = match event {
        case LiveEvent.Delivery(_, _, _, value) => Some(value)
        case _ => None
    }

    pub def waitingCount(object: Int32, pulse: Int32, result: LiveResult): Int32 =
        result#events |> List.filter(e -> e == LiveEvent.Waiting(object, pulse)) |> List.length

    pub def eventIndex(event: LiveEvent, result: LiveResult): Option[Int32] =
        List.indexOf(event, result#events)

    pub def eventBefore(first: LiveEvent, second: LiveEvent, result: LiveResult): Bool =
        match result#events |> List.dropWhile(e -> e != first) {
            case Nil => false
            case _ :: rest => List.exists(e -> e == second, rest)
        }

    def renderEvent(event: LiveEvent): String = match event {
        case LiveEvent.Born(o, r) => "birth O${o} / R${r}                         inert"
        case LiveEvent.Representation(o, locator) => "represent O${o} -> ${locator}                    same identity"
        case LiveEvent.Attached(o, p) => "attach O${o} -> P${p} / spawn observer"
        case LiveEvent.SinkWaiting(o) => "O${o} WAIT invocation"
        case LiveEvent.Waiting(o, p) => "O${o} WAIT P${p}.next"
        case LiveEvent.Downbeat(p, b) => "driver P${p} / DOWNBEAT ${b}"
        case LiveEvent.Awoke(o, p, b) => "  resume O${o} on P${p}:${b}"
        case LiveEvent.Systole(p, b, r) => "P${p} / beat ${b} / SYSTOLE sees R${r}"
        case LiveEvent.StagedRevision(r) => "  stage R${r}"
        case LiveEvent.StagedIntent(e, kind) => "  stage E${e} ${kind}"
        case LiveEvent.Aborted(b) => "  ABORT beat ${b} / no invocation"
        case LiveEvent.Committed(e, i, kind) => "COMMIT E${e} -> I${i} ${kind}"
        case LiveEvent.Diastole(i) => "DIASTOLE I${i} visible"
        case LiveEvent.Delivery(i, o, r, value) => "  O${o} observes I${i} from closed R${r} as ${value}"
        case LiveEvent.Settled(p, b) => "P${p} / beat ${b} / SETTLED"
        case LiveEvent.AskPending(c, i) => "  I${i} unfinished / C${c} -> Live(local resumption)"
        case LiveEvent.BindingChanged(c, binding) => "  C${c} -> ${binding}"
        case LiveEvent.Resumed(c, answer, beat) => "  resume C${c}(\"${answer}\") under BeatContext P3:${beat}"
        case LiveEvent.Activity(p, b, activity) => "P${p}:${b} ${activity}"
        case LiveEvent.Detached(o, p) => "detach O${o} from P${p}"
        case LiveEvent.MeasureGathering(m, p) => "M${m} gathering / waiting P${p}"
        case LiveEvent.MeasureOpened(m, p3, p4) => "M${m} OPEN {P3:${p3}, P4:${p4}}"
        case LiveEvent.ObligationCompleted(m, name) => "M${m} obligation ${name} complete"
        case LiveEvent.MeasureClosed(m, p3, p4) => "M${m} CLOSE {P3:${p3}, P4:${p4}}"
    }

    def driveAbortedBeat(driver: PrimaryDriver): (List[LiveEvent], RuntimeState) \ {Chan, NonDet} = {
        Channel.send(PulseSignal.Beat(1), driver#pulseTx);
        let phase = receiveMany(6, driver#eventRx);
        let state = Channel.recv(driver#snapshotRx);
        let waiting = Channel.recv(driver#eventRx);
        (LiveEvent.Downbeat(3, 1) :: phase ::: List#{waiting}, state)
    }

    def driveNotifyBeat(driver: PrimaryDriver): (List[LiveEvent], RuntimeState, Invocation) \ {Chan, NonDet} = {
        Channel.send(PulseSignal.Beat(2), driver#pulseTx);
        let phase = receiveMany(5, driver#eventRx);
        let publication = Channel.recv(driver#publicationRx);
        let diastole = Channel.recv(driver#eventRx);
        Channel.send(SinkSignal.Deliver(publication), driver#recorderTx);
        let recorderDelivery = Channel.recv(driver#eventRx);
        Channel.send(SinkSignal.Deliver(publication), driver#witnessTx);
        let witnessDelivery = Channel.recv(driver#eventRx);
        Channel.send((), driver#dispatchAckTx);
        let settled = Channel.recv(driver#eventRx);
        let state = Channel.recv(driver#snapshotRx);
        let waiting = Channel.recv(driver#eventRx);
        let tail = List#{diastole, recorderDelivery, witnessDelivery, settled, waiting};
        (LiveEvent.Downbeat(3, 2) :: phase ::: tail, state, publication#invocation)
    }

    def driveAskBeat(driver: PrimaryDriver): (List[LiveEvent], RuntimeState, Invocation) \ {Chan, NonDet} = {
        Channel.send(PulseSignal.Beat(3), driver#pulseTx);
        let opening = receiveMany(4, driver#eventRx);
        let invocation = Channel.recv(driver#publicationRx)#invocation;
        let pending = receiveMany(2, driver#eventRx);
        Channel.send(ResolutionCommand.Rebind(17, BindingKind.Durable("recipe:continue")), driver#resolutionTx);
        let durable = Channel.recv(driver#eventRx);
        Channel.send(ResolutionCommand.Rebind(17, BindingKind.Live), driver#resolutionTx);
        let liveAgain = Channel.recv(driver#eventRx);
        Channel.send(ResolutionCommand.Resolve(17, "yes", 4), driver#resolutionTx);
        let resolved = receiveMany(2, driver#eventRx);
        let state = Channel.recv(driver#snapshotRx);
        let waiting = Channel.recv(driver#eventRx);
        let rebinding = List#{durable, liveAgain, LiveEvent.Downbeat(3, 4)};
        (LiveEvent.Downbeat(3, 3) :: opening ::: pending ::: rebinding ::: resolved ::: List#{waiting}, state, invocation)
    }

    def driveHeartbeatAndStop(driver: PrimaryDriver): (List[LiveEvent], RuntimeState) \ {Chan, NonDet} = {
        Channel.send(PulseSignal.Beat(5), driver#pulseTx);
        let heartbeat = receiveMany(3, driver#eventRx);
        let _state = Channel.recv(driver#snapshotRx);
        let waiting = Channel.recv(driver#eventRx);
        Channel.send(PulseSignal.Stop, driver#pulseTx);
        let primaryDetached = Channel.recv(driver#eventRx);
        let finalState = Channel.recv(driver#finalRx);
        Channel.send(SinkSignal.Stop, driver#recorderTx);
        let recorderDetached = Channel.recv(driver#eventRx);
        Channel.send(SinkSignal.Stop, driver#witnessTx);
        let witnessDetached = Channel.recv(driver#eventRx);
        let tail = List#{waiting, primaryDetached, recorderDetached, witnessDetached};
        (LiveEvent.Downbeat(3, 5) :: heartbeat ::: tail, finalState)
    }

    def primaryScenario(): PrimaryResult \ {Chan, NonDet} = region rc {
        let (eventTx, eventRx) = Channel.unbuffered();
        let (pulseTx, pulseRx) = Channel.unbuffered();
        let (publicationTx, publicationRx) = Channel.unbuffered();
        let (dispatchAckTx, dispatchAckRx) = Channel.unbuffered();
        let (resolutionTx, resolutionRx) = Channel.unbuffered();
        let (snapshotTx, snapshotRx) = Channel.unbuffered();
        let (finalTx, finalRx) = Channel.unbuffered();
        let (recorderTx, recorderRx) = Channel.unbuffered();
        let (witnessTx, witnessRx) = Channel.unbuffered();
        spawn sink(18, "recorded", recorderRx, eventTx) @ rc;
        let recorderWaiting = Channel.recv(eventRx);
        spawn sink(19, "witnessed", witnessRx, eventTx) @ rc;
        let witnessWaiting = Channel.recv(eventRx);
        let initial = initialState();
        spawn Channel.send(runObserver(17, 3, initial, pulseRx, eventTx, publicationTx, dispatchAckRx, resolutionRx, snapshotTx), finalTx) @ rc;
        let firstWait = Channel.recv(eventRx);
        let driver = {
            eventRx = eventRx, pulseTx = pulseTx, publicationRx = publicationRx,
            dispatchAckTx = dispatchAckTx, resolutionTx = resolutionTx,
            snapshotRx = snapshotRx, finalRx = finalRx,
            recorderTx = recorderTx, witnessTx = witnessTx
        };
        let prelude = List#{
            LiveEvent.Born(17, 40),
            LiveEvent.Representation(17, "Aeron(stream:17)"),
            LiveEvent.Representation(17, "CAS(sha256:17)"),
            LiveEvent.Attached(17, 3),
            recorderWaiting,
            witnessWaiting,
            firstWait
        };
        let (abortEvents, afterAbort) = driveAbortedBeat(driver);
        let (notifyEvents, afterNotify, notifyInvocation) = driveNotifyBeat(driver);
        let (askEvents, afterAsk, askInvocation) = driveAskBeat(driver);
        let (finalEvents, finalState) = driveHeartbeatAndStop(driver);
        {
            events = prelude ::: abortEvents ::: notifyEvents ::: askEvents ::: finalEvents,
            initialState = initial,
            afterAbort = afterAbort,
            afterNotify = afterNotify,
            afterAsk = afterAsk,
            finalState = finalState,
            notifyInvocation = notifyInvocation,
            askInvocation = askInvocation
        }
    }

    def driveLiveMeasure(
        p3Tx: Sender[PulseSignal],
        p4Tx: Sender[PulseSignal],
        eventRx: Receiver[LiveEvent],
        p3Waiting: LiveEvent,
        p4Waiting: LiveEvent
    ): MeasureResult \ {Chan, NonDet} = {
        let p3Beat6 = driveMeasureBeat(3, 6, p3Tx, eventRx);
        let gathering = LiveEvent.MeasureGathering(2, 4);
        let p3Beat7 = driveMeasureBeat(3, 7, p3Tx, eventRx);
        let p4Beat9 = driveMeasureBeat(4, 9, p4Tx, eventRx);
        let opened = LiveEvent.MeasureOpened(2, 7, 9);
        let p4Beat10 = driveMeasureBeat(4, 10, p4Tx, eventRx);
        let p3Beat8 = driveMeasureBeat(3, 8, p3Tx, eventRx);
        let questionComplete = LiveEvent.ObligationCompleted(2, "question");
        let p3Beat9 = driveMeasureBeat(3, 9, p3Tx, eventRx);
        let p4Beat11 = driveMeasureBeat(4, 11, p4Tx, eventRx);
        let answerComplete = LiveEvent.ObligationCompleted(2, "answer");
        let closed = LiveEvent.MeasureClosed(2, 9, 11);
        Channel.send(PulseSignal.Stop, p3Tx);
        let p3Detached = Channel.recv(eventRx);
        Channel.send(PulseSignal.Stop, p4Tx);
        let p4Detached = Channel.recv(eventRx);
        let measure = {
            id = 2,
            p3Beats = List#{6, 7, 8, 9},
            p4Beats = List#{9, 10, 11},
            gatheringAfterFirst = true,
            opening = List#{(3, 7), (4, 9)},
            openBeforeAllObligations = true,
            closing = List#{(3, 9), (4, 11)},
            obligations = Set#{"answer", "question"}
        };
        let events = List#{p3Waiting, p4Waiting} ::: p3Beat6 ::: (gathering :: p3Beat7) ::: p4Beat9 ::: (opened :: p4Beat10) ::: p3Beat8 ::: (questionComplete :: p3Beat9) ::: p4Beat11 ::: List#{answerComplete, closed, p3Detached, p4Detached};
        {events = events, measure = measure}
    }

    def measureScenario(): MeasureResult \ {Chan, NonDet} = region rc {
        let (eventTx, eventRx) = Channel.unbuffered();
        let (p3Tx, p3Rx) = Channel.unbuffered();
        let (p4Tx, p4Rx) = Channel.unbuffered();
        spawn runMeasureParticipant(17, 3, p3Rx, eventTx) @ rc;
        let p3Waiting = Channel.recv(eventRx);
        spawn runMeasureParticipant(19, 4, p4Rx, eventTx) @ rc;
        let p4Waiting = Channel.recv(eventRx);
        driveLiveMeasure(p3Tx, p4Tx, eventRx, p3Waiting, p4Waiting)
    }

    /// A deterministic driver around genuinely blocking workers. Every
    /// receive is paired with a protocol event, so tests require no sleeps.
    pub def scenario(): LiveResult \ {Chan, NonDet} = {
        let primary = primaryScenario();
        let measured = measureScenario();
        let events = primary#events ::: measured#events;
        let liveInterpretations = events |> List.filterMap(interpretation) |> List.toSet;
        let attachedTopology = topology(true, true, None, None);
        let pendingTopology = topology(true, false, Some(BindingKind.Live), None);
        let finalTopology = topology(false, false, Some(BindingKind.Resolved("yes")), Some(measured#measure));
        {
            events = events,
            trace = events |> List.map(renderEvent) |> String.intercalate("\n"),
            initialState = primary#initialState,
            afterAbort = primary#afterAbort,
            afterNotify = primary#afterNotify,
            afterAsk = primary#afterAsk,
            finalState = primary#finalState,
            notifyInvocation = primary#notifyInvocation,
            askInvocation = primary#askInvocation,
            liveInterpretations = liveInterpretations,
            pureInterpretations = pureReference(),
            bindingHistory = List#{BindingKind.Live, BindingKind.Durable("recipe:continue"), BindingKind.Live, BindingKind.Resolved("yes")},
            resumedBeat = 4,
            attachedView = queryTopology(attachedTopology),
            pendingView = queryTopology(pendingTopology),
            finalView = queryTopology(finalTopology),
            attachedTopology = attachedTopology,
            pendingTopology = pendingTopology,
            finalTopology = finalTopology,
            measure = measured#measure
        }
    }

    pub def main(): Unit \ {Chan, IO, NonDet} = scenario() |> r -> println(r#trace)
}
