pub mod Nema.Lab.Vivarium {

    /// A Value is extensional: equal contents are the same value.
    pub enum Value with Eq, ToString {
        case Text(String)
        case Number(Int32)
    }

    /// These wrappers keep the several kinds of identity from being confused.
    pub enum ObjectId with Eq, Order, ToString { case ObjectId(Int32) }
    pub enum RevisionId with Eq, Order, ToString { case RevisionId(Int32) }
    pub enum PayloadId with Eq, Order, ToString { case PayloadId(Int32) }
    pub enum PulseId with Eq, Order, ToString { case PulseId(Int32) }
    pub enum IntentId with Eq, Order, ToString { case IntentId(Int32) }
    pub enum InvocationId with Eq, Order, ToString { case InvocationId(Int32) }
    pub enum ContinuationAddress with Eq, Order, ToString { case ContinuationAddress(Int32) }
    pub enum MeasureId with Eq, Order, ToString { case MeasureId(Int32) }

    pub enum Reaction with Eq, ToString {
        case Notifier
        case Recorder
        case Witness
    }

    pub enum EffectIntent with Eq, ToString {
        case Notify(IntentId, ObjectId, String)
        case Ask(IntentId, ObjectId, ContinuationAddress, String)
    }

    pub enum EffectInvocation with Eq, ToString {
        case Notify(InvocationId, IntentId, ObjectId, String)
        case Ask(InvocationId, IntentId, ObjectId, ContinuationAddress, String)
    }

    /// This is the inspectable graph binding, never a serialized JVM stack.
    pub enum Binding with Eq, ToString {
        case Live
        case Durable(String)
        case Resolved(String)
        case Dropped
    }

    pub enum MeasureState with Eq, ToString {
        case Gathering
        case Open
        case Closed
    }

    pub type alias Genesis = {
        name = String,
        bornAs = Value,
        physicalLocator = String
    }

    pub type alias Revision = {
        id = RevisionId,
        object = ObjectId,
        parent = Option[RevisionId],
        payload = PayloadId
    }

    pub type alias Payload = {
        id = PayloadId,
        value = Value
    }

    pub type alias ObjectRecord = {
        id = ObjectId,
        genesis = Genesis,
        head = RevisionId,
        history = List[Revision]
    }

    pub type alias Pulse = {
        id = PulseId,
        driver = String,
        beat = Int32
    }

    pub type alias Observer = {
        object = ObjectId,
        pulse = PulseId,
        reaction = Reaction
    }

    /// Registrations are explicitly relative to an observer and the beat at
    /// which that observer made the registration.
    pub type alias Registration = {
        observer = ObjectId,
        subject = ObjectId,
        name = String,
        pulse = PulseId,
        beat = Int32
    }

    pub type alias Delivery = {
        invocation = InvocationId,
        observer = ObjectId,
        interpretation = String
    }

    pub type alias ContinuationRow = {
        address = ContinuationAddress,
        origin = ObjectId,
        binding = Binding,
        invocation = Option[InvocationId]
    }

    pub type alias Participant = {
        measure = MeasureId,
        pulse = PulseId,
        role = String,
        activity = String,
        ready = Bool,
        openingBeat = Option[Int32],
        closingBeat = Option[Int32]
    }

    pub type alias Obligation = {
        measure = MeasureId,
        name = String,
        complete = Bool
    }

    pub type alias Measure = {
        id = MeasureId,
        name = String,
        state = MeasureState,
        participants = List[Participant],
        obligations = List[Obligation]
    }

    pub type alias World = {
        nextObject = Int32,
        nextRevision = Int32,
        nextPayload = Int32,
        nextPulse = Int32,
        nextIntent = Int32,
        nextInvocation = Int32,
        nextContinuation = Int32,
        nextMeasure = Int32,
        objects = List[ObjectRecord],
        payloads = List[Payload],
        pulses = List[Pulse],
        observers = List[Observer],
        registrations = List[Registration],
        invocations = List[EffectInvocation],
        deliveries = List[Delivery],
        continuations = List[ContinuationRow],
        measures = List[Measure]
    }

    pub type alias BeatReport = {
        pulse = PulseId,
        beat = Int32,
        priorRevision = Option[RevisionId],
        stagedRevision = Option[RevisionId],
        stagedIntents = List[EffectIntent],
        committedInvocations = List[EffectInvocation],
        deliveries = List[Delivery],
        committed = Bool,
        trace = List[String]
    }

    pub eff AskEffect {
        def ask(question: String): String
    }

    pub eff BeatContext {
        def beatLabel(): String
    }

    /// A live resumption is intentionally kept outside World. World stores
    /// only its stable address and inspectable binding state.
    pub enum AskSession {
        case Waiting(ContinuationAddress, String, String -> AskSession \ BeatContext)
        case Finished(ContinuationAddress, String, String)
    }

    pub def empty(): World = {
        nextObject = 17,
        nextRevision = 40,
        nextPayload = 70,
        nextPulse = 3,
        nextIntent = 8,
        nextInvocation = 8,
        nextContinuation = 17,
        nextMeasure = 2,
        objects = Nil,
        payloads = Nil,
        pulses = Nil,
        observers = Nil,
        registrations = Nil,
        invocations = Nil,
        deliveries = Nil,
        continuations = Nil,
        measures = Nil
    }

    pub def birth(name: String, value: Value, physicalLocator: String, world: World): (World, ObjectId) = {
        let oid = ObjectId.ObjectId(world#nextObject);
        let rid = RevisionId.RevisionId(world#nextRevision);
        let payloadId = PayloadId.PayloadId(world#nextPayload);
        let payload = {id = payloadId, value = value};
        let revision = {id = rid, object = oid, parent = None, payload = payloadId};
        let object = {
            id = oid,
            genesis = {name = name, bornAs = value, physicalLocator = physicalLocator},
            head = rid,
            history = revision :: Nil
        };
        ({
            nextObject = world#nextObject + 1,
            nextRevision = world#nextRevision + 1,
            nextPayload = world#nextPayload + 1,
            objects = object :: world#objects,
            payloads = payload :: world#payloads
            | world
        }, oid)
    }

    pub def newPulse(driver: String, world: World): (World, PulseId) = {
        let pid = PulseId.PulseId(world#nextPulse);
        let pulse = {id = pid, driver = driver, beat = 0};
        ({nextPulse = world#nextPulse + 1, pulses = pulse :: world#pulses | world}, pid)
    }

    pub def animate(object: ObjectId, pulse: PulseId, reaction: Reaction, world: World): World = {
        let withoutOld = List.filter(o -> o#object != object or o#pulse != pulse, world#observers);
        {observers = {object = object, pulse = pulse, reaction = reaction} :: withoutOld | world}
    }

    pub def dormant(object: ObjectId, pulse: PulseId, world: World): World =
        {observers = List.filter(o -> o#object != object or o#pulse != pulse, world#observers) | world}

    pub def register(observer: ObjectId, subject: ObjectId, name: String, pulse: PulseId, world: World): World = {
        let beat = pulseBeat(pulse, world);
        let registration = {observer = observer, subject = subject, name = name, pulse = pulse, beat = beat};
        {registrations = registration :: world#registrations | world}
    }

    pub def pulseBeat(pulse: PulseId, world: World): Int32 =
        List.find(p -> p#id == pulse, world#pulses)
        |> Option.map(p -> p#beat)
        |> Option.getWithDefault(-1)

    pub def objectHead(object: ObjectId, world: World): Option[RevisionId] =
        List.find(o -> o#id == object, world#objects) |> Option.map(o -> o#head)

    pub def objectPayload(object: ObjectId, world: World): Option[Value] =
        List.find(o -> o#id == object, world#objects)
        |> Option.flatMap(o -> List.find(r -> r#id == o#head, o#history))
        |> Option.flatMap(r -> List.find(p -> p#id == r#payload, world#payloads))
        |> Option.map(p -> p#value)

    pub def objectPayloadId(object: ObjectId, world: World): Option[PayloadId] =
        List.find(o -> o#id == object, world#objects)
        |> Option.flatMap(o -> List.find(r -> r#id == o#head, o#history))
        |> Option.map(r -> r#payload)

    pub def isObserver(object: ObjectId, pulse: PulseId, world: World): Bool =
        List.exists(o -> o#object == object and o#pulse == pulse, world#observers)
        and List.exists(p -> p#id == pulse, world#pulses)

    pub def observerNames(subject: ObjectId, world: World): Set[String] =
        world#registrations
        |> List.filter(r -> r#subject == subject)
        |> List.map(r -> r#name)
        |> List.toSet

    pub def revisionCount(object: ObjectId, world: World): Int32 =
        List.find(o -> o#id == object, world#objects)
        |> Option.map(o -> List.length(o#history))
        |> Option.getWithDefault(0)

    def updatePulse(pulseId: PulseId, f: Pulse -> Pulse, pulses: List[Pulse]): List[Pulse] =
        List.map(p -> if (p#id == pulseId) f(p) else p, pulses)

    def updateObject(objectId: ObjectId, f: ObjectRecord -> ObjectRecord, objects: List[ObjectRecord]): List[ObjectRecord] =
        List.map(o -> if (o#id == objectId) f(o) else o, objects)

    def idObject(id: ObjectId): Int32 = match id {
        case ObjectId.ObjectId(n) => n
    }

    def idRevision(id: RevisionId): Int32 = match id {
        case RevisionId.RevisionId(n) => n
    }

    def idPayload(id: PayloadId): Int32 = match id {
        case PayloadId.PayloadId(n) => n
    }

    def idPulse(id: PulseId): Int32 = match id {
        case PulseId.PulseId(n) => n
    }

    def idIntent(id: IntentId): Int32 = match id {
        case IntentId.IntentId(n) => n
    }

    def idInvocation(id: InvocationId): Int32 = match id {
        case InvocationId.InvocationId(n) => n
    }

    def idContinuation(id: ContinuationAddress): Int32 = match id {
        case ContinuationAddress.ContinuationAddress(n) => n
    }

    def idMeasure(id: MeasureId): Int32 = match id {
        case MeasureId.MeasureId(n) => n
    }

    pub def showObject(id: ObjectId): String = "O${idObject(id)}"
    pub def showRevision(id: RevisionId): String = "R${idRevision(id)}"
    pub def showPayload(id: PayloadId): String = "V${idPayload(id)}"
    pub def showPulse(id: PulseId): String = "P${idPulse(id)}"
    pub def showIntent(id: IntentId): String = "E${idIntent(id)}"
    pub def showInvocation(id: InvocationId): String = "I${idInvocation(id)}"
    pub def showContinuation(id: ContinuationAddress): String = "C${idContinuation(id)}"
    pub def showMeasure(id: MeasureId): String = "M${idMeasure(id)}"

    def revise(objectId: ObjectId, value: Value, world: World): (World, RevisionId) = {
        let rid = RevisionId.RevisionId(world#nextRevision);
        let payloadId = PayloadId.PayloadId(world#nextPayload);
        let payload = {id = payloadId, value = value};
        let objects = updateObject(objectId, o -> {
            let revision = {id = rid, object = objectId, parent = Some(o#head), payload = payloadId};
            {head = rid, history = revision :: o#history | o}
        }, world#objects);
        ({
            nextRevision = world#nextRevision + 1,
            nextPayload = world#nextPayload + 1,
            objects = objects,
            payloads = payload :: world#payloads
            | world
        }, rid)
    }

    def advancePulse(pulse: PulseId, world: World): (World, Int32) = {
        let beat = pulseBeat(pulse, world) + 1;
        ({pulses = updatePulse(pulse, p -> {beat = beat | p}, world#pulses) | world}, beat)
    }

    def commitIntent(intent: EffectIntent, iid: InvocationId): EffectInvocation = match intent {
        case EffectIntent.Notify(eid, origin, message) => EffectInvocation.Notify(iid, eid, origin, message)
        case EffectIntent.Ask(eid, origin, address, question) => EffectInvocation.Ask(iid, eid, origin, address, question)
    }

    def invocationId(invocation: EffectInvocation): InvocationId = match invocation {
        case EffectInvocation.Notify(id, _, _, _) => id
        case EffectInvocation.Ask(id, _, _, _, _) => id
    }

    def interestedObservers(origin: ObjectId, pulse: PulseId, world: World): List[Observer] =
        world#observers
        |> List.filter(o -> o#pulse == pulse and o#object != origin)
        |> List.filter(o -> List.exists(r -> r#observer == o#object and r#subject == origin, world#registrations))

    def interpretationFor(observer: Observer, invocation: EffectInvocation): String = match (observer#reaction, invocation) {
        case (Reaction.Recorder, EffectInvocation.Notify(_, _, _, message)) => "recorded:${message}"
        case (Reaction.Witness, EffectInvocation.Notify(_, _, _, message)) => "witnessed:${message}"
        case (Reaction.Recorder, EffectInvocation.Ask(_, _, _, _, question)) => "recorded-question:${question}"
        case (Reaction.Witness, EffectInvocation.Ask(_, _, _, _, question)) => "witnessed-question:${question}"
        case (_, _) => "noticed"
    }

    def dispatch(invocation: EffectInvocation, pulse: PulseId, world: World): List[Delivery] = {
        let origin = match invocation {
            case EffectInvocation.Notify(_, _, o, _) => o
            case EffectInvocation.Ask(_, _, o, _, _) => o
        };
        interestedObservers(origin, pulse, world)
        |> List.map(o -> {
            invocation = invocationId(invocation),
            observer = o#object,
            interpretation = interpretationFor(o, invocation)
        })
    }

    /// One complete Beat. Systole sees the prior head and stages an immutable
    /// revision plus intent. Abort discards both. Commit publishes them;
    /// diastole then dispatches the authoritative invocation.
    def tickNotifyObserver(pulse: PulseId, origin: ObjectId, message: String, commit: Bool, world0: World): (World, BeatReport) = {
        let (world1, beat) = advancePulse(pulse, world0);
        let prior = objectHead(origin, world1);
        let rid = RevisionId.RevisionId(world1#nextRevision);
        let eid = IntentId.IntentId(world1#nextIntent);
        let intent = EffectIntent.Notify(eid, origin, message);
        let opening = "${showPulse(pulse)} Beat ${beat} SYSTOLE (visible ${Option.map(showRevision, prior) |> Option.getWithDefault("none")})";
        if (not commit) {
            let trace = List#{opening, "  stage ${showRevision(rid)} + ${showIntent(eid)} Notify", "  ABORT (staged work discarded)", "${showPulse(pulse)} Beat ${beat} SETTLED"};
            (world1, {
                pulse = pulse, beat = beat, priorRevision = prior, stagedRevision = Some(rid),
                stagedIntents = intent :: Nil, committedInvocations = Nil, deliveries = Nil,
                committed = false, trace = trace
            })
        } else {
            let (world2, actualRid) = revise(origin, Value.Text(message), world1);
            let iid = InvocationId.InvocationId(world2#nextInvocation);
            let invocation = commitIntent(intent, iid);
            let stagedWorld = {
                nextIntent = world2#nextIntent + 1,
                nextInvocation = world2#nextInvocation + 1,
                invocations = invocation :: world2#invocations
                | world2
            };
            let deliveries = dispatch(invocation, pulse, stagedWorld);
            let world3 = {deliveries = deliveries ::: stagedWorld#deliveries | stagedWorld};
            let trace = List#{
                opening,
                "  stage ${showRevision(actualRid)} + ${showIntent(eid)} Notify",
                "  COMMIT ${showIntent(eid)} -> ${showInvocation(iid)}",
                "  DIASTOLE dispatch ${showInvocation(iid)} to ${List.length(deliveries)} observer(s)",
                "${showPulse(pulse)} Beat ${beat} SETTLED"
            };
            (world3, {
                pulse = pulse, beat = beat, priorRevision = prior, stagedRevision = Some(actualRid),
                stagedIntents = intent :: Nil, committedInvocations = invocation :: Nil,
                deliveries = deliveries, committed = true, trace = trace
            })
        }
    }

    pub def tickNotify(pulse: PulseId, origin: ObjectId, message: String, commit: Bool, world0: World): (World, BeatReport) =
        if (isObserver(origin, pulse, world0))
            tickNotifyObserver(pulse, origin, message, commit, world0)
        else {
            let (world1, beat) = advancePulse(pulse, world0);
            let trace = List#{
                "${showPulse(pulse)} Beat ${beat} SYSTOLE (no binding for ${showObject(origin)})",
                "  no observation; no staged work",
                "${showPulse(pulse)} Beat ${beat} SETTLED"
            };
            (world1, {
                pulse = pulse, beat = beat, priorRevision = objectHead(origin, world0), stagedRevision = None,
                stagedIntents = Nil, committedInvocations = Nil, deliveries = Nil,
                committed = false, trace = trace
            })
        }

    pub def tick(pulse: PulseId, origin: ObjectId, message: String, world: World): (World, BeatReport) =
        tickNotify(pulse, origin, message, true, world)

    pub def invocationCount(world: World): Int32 = List.length(world#invocations)
    pub def deliveryCount(world: World): Int32 = List.length(world#deliveries)
    pub def reportTrace(report: BeatReport): String = String.intercalate("\n", report#trace)

    def renderDelivery(delivery: Delivery): String =
        "${showObject(delivery#observer)}=>${delivery#interpretation}"

    pub def renderDeliveries(report: BeatReport): String =
        report#deliveries |> List.map(renderDelivery) |> String.intercalate(", ")

    pub def deliveryInterpretations(report: BeatReport): Set[String] =
        report#deliveries |> List.map(d -> d#interpretation) |> List.toSet

    def capturedAsk(address: ContinuationAddress, question: String): String -> AskSession \ BeatContext =
        run {
            let answer = AskEffect.ask(question);
            _ -> AskSession.Finished(address, answer, BeatContext.beatLabel())
        } with handler AskEffect {
            def ask(_, resume) = answer -> resume(answer)(answer)
        }

    /// Stages and commits a result-bearing invocation while returning the
    /// actual local resumption separately from the inspectable World.
    pub def ask(pulse: PulseId, origin: ObjectId, question: String, world0: World): (World, AskSession, BeatReport) = {
        let (world1, beat) = advancePulse(pulse, world0);
        let prior = objectHead(origin, world1);
        let eid = IntentId.IntentId(world1#nextIntent);
        let iid = InvocationId.InvocationId(world1#nextInvocation);
        let address = ContinuationAddress.ContinuationAddress(world1#nextContinuation);
        let intent = EffectIntent.Ask(eid, origin, address, question);
        let invocation = commitIntent(intent, iid);
        let session = AskSession.Waiting(address, question, capturedAsk(address, question));
        let row = {address = address, origin = origin, binding = Binding.Live, invocation = Some(iid)};
        let stagedWorld = {
            nextIntent = world1#nextIntent + 1,
            nextInvocation = world1#nextInvocation + 1,
            nextContinuation = world1#nextContinuation + 1,
            invocations = invocation :: world1#invocations,
            continuations = row :: world1#continuations
            | world1
        };
        let deliveries = dispatch(invocation, pulse, stagedWorld);
        let world2 = {deliveries = deliveries ::: stagedWorld#deliveries | stagedWorld};
        let trace = List#{
            "${showPulse(pulse)} Beat ${beat} SYSTOLE (capture ${showContinuation(address)})",
            "  stage ${showIntent(eid)} Ask \"${question}\"",
            "  COMMIT ${showIntent(eid)} -> ${showInvocation(iid)} origin ${showObject(origin)}",
            "  DIASTOLE dispatch; ${showContinuation(address)} remains Live",
            "${showPulse(pulse)} Beat ${beat} SETTLED"
        };
        (world2, session, {
            pulse = pulse, beat = beat, priorRevision = prior, stagedRevision = None,
            stagedIntents = intent :: Nil, committedInvocations = invocation :: Nil,
            deliveries = deliveries, committed = true, trace = trace
        })
    }

    pub def continuationBinding(address: ContinuationAddress, world: World): Option[Binding] =
        List.find(c -> c#address == address, world#continuations) |> Option.map(c -> c#binding)

    pub def rebind(address: ContinuationAddress, binding: Binding, world: World): World =
        {continuations = List.map(c -> if (c#address == address) {binding = binding | c} else c, world#continuations) | world}

    pub def sessionAddress(session: AskSession): ContinuationAddress = match session {
        case AskSession.Waiting(address, _, _) => address
        case AskSession.Finished(address, _, _) => address
    }

    /// Resolution happens on a later Beat and installs a fresh dynamic
    /// BeatContext around the escaped resumption.
    pub def resolveAsk(pulse: PulseId, answer: String, session: AskSession, world0: World): (World, AskSession, String) = {
        let address = sessionAddress(session);
        let (world1, beat) = advancePulse(pulse, world0);
        let label = "${showPulse(pulse)} Beat ${beat}";
        let resumed = match session {
            case AskSession.Waiting(_, _, continuation) =>
                run { continuation(answer) } with handler BeatContext {
                    def beatLabel(_, resume) = resume(label)
                }
            case AskSession.Finished(_, _, _) => session
        };
        let world2 = rebind(address, Binding.Resolved(answer), world1);
        (world2, resumed, "${label} DIASTOLE resolve ${showContinuation(address)} = \"${answer}\"")
    }

    pub def finishedAt(session: AskSession): Option[String] = match session {
        case AskSession.Waiting(_, _, _) => None
        case AskSession.Finished(_, _, beat) => Some(beat)
    }

    pub def newMeasure(name: String, roles: List[(PulseId, String)], obligationNames: List[String], world: World): (World, MeasureId) = {
        let mid = MeasureId.MeasureId(world#nextMeasure);
        let participants = List.map(role -> match role {
            case (pulse, name0) => {
                measure = mid, pulse = pulse, role = name0, activity = "waiting",
                ready = false, openingBeat = None, closingBeat = None
            }
        }, roles);
        let obligations = List.map(name0 -> {measure = mid, name = name0, complete = false}, obligationNames);
        let measure = {id = mid, name = name, state = MeasureState.Gathering, participants = participants, obligations = obligations};
        ({nextMeasure = world#nextMeasure + 1, measures = measure :: world#measures | world}, mid)
    }

    def allReady(participants: List[Participant]): Bool =
        not List.isEmpty(participants) and List.forAll(p -> p#ready, participants)

    def allComplete(obligations: List[Obligation]): Bool =
        List.forAll(o -> o#complete, obligations)

    def pulseBeatIn(world: World, participant: Participant): Participant =
        {activity = "coordinating", openingBeat = Some(pulseBeat(participant#pulse, world)) | participant}

    pub def ready(measureId: MeasureId, pulseId: PulseId, world: World): World = {
        let measures = List.map(m -> if (m#id != measureId or m#state != MeasureState.Gathering) m else {
            let participants = List.map(p -> if (p#pulse == pulseId) {ready = true | p} else p, m#participants);
            if (allReady(participants))
                {state = MeasureState.Open, participants = List.map(p -> pulseBeatIn(world, p), participants) | m}
            else
                {participants = participants | m}
        }, world#measures);
        {measures = measures | world}
    }

    pub def fulfill(measureId: MeasureId, obligationName: String, world: World): World = {
        let measures = List.map(m -> if (m#id != measureId or m#state != MeasureState.Open) m else {
            let obligations = List.map(o -> if (o#name == obligationName) {complete = true | o} else o, m#obligations);
            if (allComplete(obligations)) {
                let participants = List.map(p -> {
                    activity = "settled", closingBeat = Some(pulseBeat(p#pulse, world)) | p
                }, m#participants);
                {state = MeasureState.Closed, participants = participants, obligations = obligations | m}
            } else
                {obligations = obligations | m}
        }, world#measures);
        {measures = measures | world}
    }

    pub def measureState(measureId: MeasureId, world: World): Option[MeasureState] =
        List.find(m -> m#id == measureId, world#measures) |> Option.map(m -> m#state)

    pub def measureFrontier(measureId: MeasureId, world: World): List[(PulseId, Option[Int32], Option[Int32])] =
        List.find(m -> m#id == measureId, world#measures)
        |> Option.map(m -> List.map(p -> (p#pulse, p#openingBeat, p#closingBeat), m#participants))
        |> Option.getWithDefault(Nil)

    pub def describeMeasure(measureId: MeasureId, world: World): String =
        match List.find(m -> m#id == measureId, world#measures) {
            case None => "${showMeasure(measureId)} missing"
            case Some(m) =>
                let participants = m#participants |> List.map(p ->
                    "${showPulse(p#pulse)} role=${p#role} activity=${p#activity} ready=${p#ready} open=${p#openingBeat} close=${p#closingBeat}"
                ) |> String.intercalate("; ");
                let obligations = m#obligations |> List.map(o -> "${o#name}=${o#complete}") |> String.intercalate(", ");
                "${showMeasure(m#id)} ${m#state} [${participants}] obligations=[${obligations}]"
        }

    /// Relational projection only: none of these queries performs a transition.
    pub def queryLiveObservers(world: World): Set[Int32] = {
        let objectFacts = world#objects |> List.map(o -> idObject(o#id)) |> List.toSet;
        let observerFacts = world#observers |> List.map(o -> idObject(o#object)) |> List.toSet;
        let pulseFacts = world#pulses |> List.map(p -> idPulse(p#id)) |> List.toSet;
        let hasPulseFacts = world#observers |> List.map(o -> (idObject(o#object), idPulse(o#pulse))) |> List.toSet;
        let drivenFacts = world#pulses |> List.map(p -> (idPulse(p#id), p#driver)) |> List.toSet;
        let rules = #{
            LiveObserver(o) :- Object(o), Observer(o), HasPulse(o, p), Pulse(p), DrivenBy(p, driver).
        };
        let facts = inject objectFacts, observerFacts, hasPulseFacts, pulseFacts, drivenFacts into Object/1, Observer/1, HasPulse/2, Pulse/1, DrivenBy/2;
        query facts, rules select o from LiveObserver(o) |> Vector.toSet
    }

    pub def queryPendingContinuations(world: World): Set[Int32] = {
        let continuationFacts = world#continuations |> List.map(c -> idContinuation(c#address)) |> List.toSet;
        let boundFacts = world#continuations |> List.map(c -> (idContinuation(c#address), "binding")) |> List.toSet;
        let pendingFacts = world#continuations |> List.filter(c -> c#binding == Binding.Live or match c#binding { case Binding.Durable(_) => true case _ => false }) |> List.map(c -> idContinuation(c#address)) |> List.toSet;
        let rules = #{
            PendingContinuation(c) :- Continuation(c), BoundTo(c, binding), Pending(c).
        };
        let facts = inject continuationFacts, boundFacts, pendingFacts into Continuation/1, BoundTo/2, Pending/1;
        query facts, rules select c from PendingContinuation(c) |> Vector.toSet
    }

    pub def queryMeasureReady(world: World): Set[Int32] = {
        let measureFacts = world#measures |> List.map(m -> idMeasure(m#id)) |> List.toSet;
        let participatesFacts = world#measures |> List.flatMap(m -> List.map(p -> (idMeasure(m#id), idPulse(p#pulse)), m#participants)) |> List.toSet;
        let readyFacts = world#measures |> List.flatMap(m -> m#participants |> List.filter(p -> p#ready) |> List.map(p -> (idMeasure(m#id), idPulse(p#pulse)))) |> List.toSet;
        let unreadyFacts = world#measures |> List.flatMap(m -> m#participants |> List.filter(p -> not p#ready) |> List.map(p -> (idMeasure(m#id), idPulse(p#pulse)))) |> List.toSet;
        let rules = #{
            HasParticipant(m) :- Participates(m, p).
            ParticipantReady(m, p) :- Participates(m, p), Ready(m, p).
            Blocked(m) :- Participates(m, p), Unready(m, p).
            MeasureReady(m) :- Measure(m), HasParticipant(m), not Blocked(m).
        };
        let facts = inject measureFacts, participatesFacts, readyFacts, unreadyFacts into Measure/1, Participates/2, Ready/2, Unready/2;
        query facts, rules select m from MeasureReady(m) |> Vector.toSet
    }

    pub def queryParticipantReady(world: World): Set[(Int32, Int32)] = {
        let participatesFacts = world#measures |> List.flatMap(m -> List.map(p -> (idMeasure(m#id), idPulse(p#pulse)), m#participants)) |> List.toSet;
        let readyFacts = world#measures |> List.flatMap(m -> m#participants |> List.filter(p -> p#ready) |> List.map(p -> (idMeasure(m#id), idPulse(p#pulse)))) |> List.toSet;
        let rules = #{
            ParticipantReady(m, p) :- Participates(m, p), Ready(m, p).
        };
        let facts = inject participatesFacts, readyFacts into Participates/2, Ready/2;
        query facts, rules select (m, p) from ParticipantReady(m, p) |> Vector.toSet
    }

    pub def queryMeasureClosed(world: World): Set[Int32] = {
        let measureFacts = world#measures |> List.map(m -> idMeasure(m#id)) |> List.toSet;
        let openFacts = world#measures |> List.filter(m -> m#state == MeasureState.Open or m#state == MeasureState.Closed) |> List.map(m -> idMeasure(m#id)) |> List.toSet;
        let incompleteFacts = world#measures |> List.flatMap(m -> m#obligations |> List.filter(o -> not o#complete) |> List.map(_ -> idMeasure(m#id))) |> List.toSet;
        let rules = #{
            MeasureClosed(m) :- Measure(m), MeasureOpen(m), not Incomplete(m).
        };
        let facts = inject measureFacts, openFacts, incompleteFacts into Measure/1, MeasureOpen/1, Incomplete/1;
        query facts, rules select m from MeasureClosed(m) |> Vector.toSet
    }

    pub def queryReachableSubjects(world: World): Set[(Int32, Int32)] = {
        let registrations = world#registrations |> List.map(r -> (idObject(r#observer), idObject(r#subject))) |> List.toSet;
        let rules = #{
            Reachable(o, s) :- Registration(o, s).
            Reachable(o, s) :- Registration(o, via), Reachable(via, s).
        };
        let facts = inject registrations into Registration/2;
        query facts, rules select (o, s) from Reachable(o, s) |> Vector.toSet
    }

    pub def queryInvocationOrigins(world: World): Set[(Int32, Int32)] = {
        let invocationFacts = world#invocations |> List.map(i -> idInvocation(invocationId(i))) |> List.toSet;
        let originFacts = world#invocations |> List.map(i -> match i {
            case EffectInvocation.Notify(iid, _, origin, _) => (idInvocation(iid), idObject(origin))
            case EffectInvocation.Ask(iid, _, origin, _, _) => (idInvocation(iid), idObject(origin))
        }) |> List.toSet;
        let rules = #{
            ObservableOrigin(i, o) :- Invocation(i), Origin(i, o).
        };
        let facts = inject invocationFacts, originFacts into Invocation/1, Origin/2;
        query facts, rules select (i, o) from ObservableOrigin(i, o) |> Vector.toSet
    }

    pub def describe(world: World): String = {
        let objectLines = world#objects |> List.reverse |> List.map(o ->
            let payload = objectPayloadId(o#id, world) |> Option.map(showPayload) |> Option.getWithDefault("none");
            "${showObject(o#id)} object genesis=\"${o#genesis#name}\" locator=${o#genesis#physicalLocator} head=${showRevision(o#head)} payload=${payload} revisions=${List.length(o#history)}"
        );
        let pulseLines = world#pulses |> List.reverse |> List.map(p ->
            "${showPulse(p#id)} pulse beat=${p#beat} driven-by=\"${p#driver}\""
        );
        let observerLines = world#observers |> List.reverse |> List.map(o ->
            "${showObject(o#object)} observer has-pulse=${showPulse(o#pulse)} reaction=${o#reaction}"
        );
        String.intercalate("\n", objectLines ::: pulseLines ::: observerLines)
    }

    pub def demo(): String = {
        let (w1, seed) = birth("seed", Value.Text("asleep"), "urn:nema:seed", empty());
        let inert = "birth ${showObject(seed)} (no Pulse; unrelated ticks leave it untouched)";
        let (w2, pulse) = newPulse("hand crank", w1);
        let w3 = animate(seed, pulse, Reaction.Notifier, w2);
        let (w4, recorder) = birth("recorder", Value.Text("empty"), "urn:nema:recorder", w3);
        let (w5, witness) = birth("witness", Value.Text("empty"), "urn:nema:witness", w4);
        let w6 = animate(recorder, pulse, Reaction.Recorder, w5) |> register(recorder, seed, "specimen", pulse);
        let w7 = animate(witness, pulse, Reaction.Witness, w6) |> register(witness, seed, "same specimen", pulse);
        let (w8, aborted) = tickNotify(pulse, seed, "discard me", false, w7);
        let (w9, committed) = tickNotify(pulse, seed, "awake", true, w8);
        let (w10, session, asked) = ask(pulse, seed, "continue?", w9);
        let address = sessionAddress(session);
        let durable = rebind(address, Binding.Durable("recipe:continue"), w10);
        let liveAgain = rebind(address, Binding.Live, durable);
        let pending = "Datalog before resolution PendingContinuation=${queryPendingContinuations(liveAgain)}";
        let (w11, finished, resolution) = resolveAsk(pulse, "yes", session, liveAgain);
        let (w12, secondPulse) = newPulse("metronome", w11);
        let (w13, measure) = newMeasure("duet", List#{(pulse, "caller"), (secondPulse, "answerer")}, List#{"question", "answer"}, w12);
        let w14 = ready(measure, pulse, w13);
        let beforeOpen = "${showMeasure(measure)} gathering after only ${showPulse(pulse)} is ready";
        let w15 = ready(measure, secondPulse, w14);
        let opened = describeMeasure(measure, w15);
        let w16 = fulfill(measure, "question", w15);
        let w17 = fulfill(measure, "answer", w16);
        let closed = describeMeasure(measure, w17);
        let datalog = "Datalog LiveObserver=${queryLiveObservers(w17)} PendingContinuation=${queryPendingContinuations(w17)} MeasureClosed=${queryMeasureClosed(w17)} Origin=${queryInvocationOrigins(w17)}";
        let resumedAt = finishedAt(finished) |> Option.getWithDefault("not resumed");
        let committedId = committed#committedInvocations |> List.head |> Option.map(invocationId) |> Option.map(showInvocation) |> Option.getWithDefault("none");
        String.intercalate("\n", List#{
            "Nema.Lab.Vivarium",
            inert,
            "bind ${showObject(seed)} HasPulse ${showPulse(pulse)} DrivenBy hand-crank => LiveObserver",
            reportTrace(aborted),
            reportTrace(committed),
            "  one ${committedId} projected independently: ${renderDeliveries(committed)}",
            reportTrace(asked),
            "  ${showContinuation(address)} binding Live -> Durable(recipe) -> Live",
            pending,
            resolution,
            "  resumed computation observed dynamic context ${resumedAt}",
            beforeOpen,
            opened,
            closed,
            datalog
        })
    }

    pub def main(): Unit \ IO = demo() |> println
}
