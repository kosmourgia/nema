mod TestVivarium {

    use Nema.Lab.Vivarium.{Binding, MeasureState, Reaction, Value}

    def inertSeed(): (Nema.Lab.Vivarium.World, Nema.Lab.Vivarium.ObjectId, Nema.Lab.Vivarium.PulseId) = {
        let (w1, object) = Nema.Lab.Vivarium.birth("seed", Value.Text("asleep"), "urn:test:seed", Nema.Lab.Vivarium.empty());
        let (w2, pulse) = Nema.Lab.Vivarium.newPulse("test-driver", w1);
        (w2, object, pulse)
    }

    def liveSeed(): (Nema.Lab.Vivarium.World, Nema.Lab.Vivarium.ObjectId, Nema.Lab.Vivarium.PulseId) = {
        let (world, object, pulse) = inertSeed();
        (Nema.Lab.Vivarium.animate(object, pulse, Reaction.Notifier, world), object, pulse)
    }

    @Test
    pub def inertObjectDoesNotReactToUnrelatedTick(): Unit \ Assert = {
        let (w1, object, pulse) = inertSeed();
        let before = Nema.Lab.Vivarium.objectHead(object, w1);
        let (w2, report) = Nema.Lab.Vivarium.tickNotify(pulse, object, "should not land", true, w1);
        Assert.assertEq(expected = before, Nema.Lab.Vivarium.objectHead(object, w2));
        Assert.assertEq(expected = 1, Nema.Lab.Vivarium.revisionCount(object, w2));
        Assert.assertEq(expected = 0, List.length(report#stagedIntents));
        Assert.assertEq(expected = 0, Nema.Lab.Vivarium.invocationCount(w2))
    }

    @Test
    pub def pulseBindingCreatesObserverBehavior(): Unit \ Assert = {
        let (w1, object, pulse) = liveSeed();
        let (w2, _) = Nema.Lab.Vivarium.tickNotify(pulse, object, "awake", true, w1);
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.isObserver(object, pulse, w1));
        Assert.assertEq(expected = 2, Nema.Lab.Vivarium.revisionCount(object, w2));
        Assert.assertEq(expected = Some(Value.Text("awake")), Nema.Lab.Vivarium.objectPayload(object, w2))
    }

    @Test
    pub def observersCanNameTheSameObjectDifferently(): Unit \ Assert = {
        let (w1, subject, pulse) = liveSeed();
        let (w2, first) = Nema.Lab.Vivarium.birth("first", Value.Text("empty"), "urn:test:first", w1);
        let (w3, second) = Nema.Lab.Vivarium.birth("second", Value.Text("empty"), "urn:test:second", w2);
        let w4 = Nema.Lab.Vivarium.animate(first, pulse, Reaction.Recorder, w3);
        let w5 = Nema.Lab.Vivarium.register(first, subject, "specimen", pulse, w4);
        let w6 = Nema.Lab.Vivarium.animate(second, pulse, Reaction.Witness, w5);
        let w7 = Nema.Lab.Vivarium.register(second, subject, "mystery", pulse, w6);
        Assert.assertEq(expected = Set#{"mystery", "specimen"}, Nema.Lab.Vivarium.observerNames(subject, w7))
    }

    @Test
    pub def systoleSeesPriorCommittedRevision(): Unit \ Assert = {
        let (w1, object, pulse) = liveSeed();
        let prior = Nema.Lab.Vivarium.objectHead(object, w1);
        let (w2, report) = Nema.Lab.Vivarium.tickNotify(pulse, object, "next", true, w1);
        Assert.assertEq(expected = prior, report#priorRevision);
        Assert.assertEq(expected = report#stagedRevision, Nema.Lab.Vivarium.objectHead(object, w2));
        Assert.assertEq(expected = false, report#priorRevision == report#stagedRevision)
    }

    @Test
    pub def abortedSystolePublishesNothing(): Unit \ Assert = {
        let (w1, object, pulse) = liveSeed();
        let (w2, report) = Nema.Lab.Vivarium.tickNotify(pulse, object, "discard", false, w1);
        Assert.assertEq(expected = false, report#committed);
        Assert.assertEq(expected = 0, Nema.Lab.Vivarium.invocationCount(w2));
        Assert.assertEq(expected = 1, Nema.Lab.Vivarium.revisionCount(object, w2));
        Assert.assertEq(expected = 1, List.length(report#stagedIntents));
        Assert.assertEq(expected = 0, List.length(report#committedInvocations))
    }

    @Test
    pub def commitConvertsIntentToInvocation(): Unit \ Assert = {
        let (w1, object, pulse) = liveSeed();
        let (w2, report) = Nema.Lab.Vivarium.tickNotify(pulse, object, "publish", true, w1);
        Assert.assertEq(expected = 1, List.length(report#stagedIntents));
        Assert.assertEq(expected = 1, List.length(report#committedInvocations));
        Assert.assertEq(expected = 1, Nema.Lab.Vivarium.invocationCount(w2));
        Assert.assertEq(expected = Set#{(8, 17)}, Nema.Lab.Vivarium.queryInvocationOrigins(w2))
    }

    @Test
    pub def oneInvocationHasIndependentObserverProjections(): Unit \ Assert = {
        let (w1, subject, pulse) = liveSeed();
        let (w2, recorder) = Nema.Lab.Vivarium.birth("recorder", Value.Text("empty"), "urn:test:recorder", w1);
        let (w3, witness) = Nema.Lab.Vivarium.birth("witness", Value.Text("empty"), "urn:test:witness", w2);
        let w4 = Nema.Lab.Vivarium.animate(recorder, pulse, Reaction.Recorder, w3);
        let w5 = Nema.Lab.Vivarium.register(recorder, subject, "subject", pulse, w4);
        let w6 = Nema.Lab.Vivarium.animate(witness, pulse, Reaction.Witness, w5);
        let w7 = Nema.Lab.Vivarium.register(witness, subject, "subject", pulse, w6);
        let (_, report) = Nema.Lab.Vivarium.tickNotify(pulse, subject, "hello", true, w7);
        Assert.assertEq(expected = 2, List.length(report#deliveries));
        Assert.assertEq(
            expected = Set#{"recorded:hello", "witnessed:hello"},
            Nema.Lab.Vivarium.deliveryInterpretations(report)
        )
    }

    @Test
    pub def resultInvocationResumesOnLaterBeat(): Unit \ Assert = {
        let (w1, object, pulse) = liveSeed();
        let (w2, session, _) = Nema.Lab.Vivarium.ask(pulse, object, "continue?", w1);
        let address = Nema.Lab.Vivarium.sessionAddress(session);
        let (w3, finished, _) = Nema.Lab.Vivarium.resolveAsk(pulse, "yes", session, w2);
        Assert.assertEq(expected = Some("P3 Beat 2"), Nema.Lab.Vivarium.finishedAt(finished));
        Assert.assertEq(expected = Some(Binding.Resolved("yes")), Nema.Lab.Vivarium.continuationBinding(address, w3))
    }

    @Test
    pub def stableAddressSurvivesBindingChanges(): Unit \ Assert = {
        let (w1, object, pulse) = liveSeed();
        let (w2, session, _) = Nema.Lab.Vivarium.ask(pulse, object, "continue?", w1);
        let address = Nema.Lab.Vivarium.sessionAddress(session);
        let w3 = Nema.Lab.Vivarium.rebind(address, Binding.Durable("recipe:test"), w2);
        let w4 = Nema.Lab.Vivarium.rebind(address, Binding.Live, w3);
        Assert.assertEq(expected = Some(Binding.Durable("recipe:test")), Nema.Lab.Vivarium.continuationBinding(address, w3));
        Assert.assertEq(expected = Some(Binding.Live), Nema.Lab.Vivarium.continuationBinding(address, w4));
        Assert.assertEq(expected = address, Nema.Lab.Vivarium.sessionAddress(session))
    }

    @Test
    pub def measureWaitsForEveryRequiredPulse(): Unit \ Assert = {
        let (w1, first) = Nema.Lab.Vivarium.newPulse("first", Nema.Lab.Vivarium.empty());
        let (w2, second) = Nema.Lab.Vivarium.newPulse("second", w1);
        let (w3, measure) = Nema.Lab.Vivarium.newMeasure("pair", List#{(first, "left"), (second, "right")}, List#{"done"}, w2);
        let w4 = Nema.Lab.Vivarium.ready(measure, first, w3);
        let w5 = Nema.Lab.Vivarium.ready(measure, second, w4);
        Assert.assertEq(expected = Some(MeasureState.Gathering), Nema.Lab.Vivarium.measureState(measure, w4));
        Assert.assertEq(expected = Set#{}, Nema.Lab.Vivarium.queryMeasureReady(w4));
        Assert.assertEq(expected = Set#{(2, 3)}, Nema.Lab.Vivarium.queryParticipantReady(w4));
        Assert.assertEq(expected = Some(MeasureState.Open), Nema.Lab.Vivarium.measureState(measure, w5));
        Assert.assertEq(expected = Set#{2}, Nema.Lab.Vivarium.queryMeasureReady(w5));
        Assert.assertEq(expected = Set#{(2, 3), (2, 4)}, Nema.Lab.Vivarium.queryParticipantReady(w5))
    }

    @Test
    pub def measureWaitsForEveryObligationBeforeClosing(): Unit \ Assert = {
        let (w1, first) = Nema.Lab.Vivarium.newPulse("first", Nema.Lab.Vivarium.empty());
        let (w2, second) = Nema.Lab.Vivarium.newPulse("second", w1);
        let (w3, measure) = Nema.Lab.Vivarium.newMeasure("pair", List#{(first, "left"), (second, "right")}, List#{"question", "answer"}, w2);
        let w4 = Nema.Lab.Vivarium.ready(measure, first, w3) |> Nema.Lab.Vivarium.ready(measure, second);
        let w5 = Nema.Lab.Vivarium.fulfill(measure, "question", w4);
        let w6 = Nema.Lab.Vivarium.fulfill(measure, "answer", w5);
        Assert.assertEq(expected = Some(MeasureState.Open), Nema.Lab.Vivarium.measureState(measure, w5));
        Assert.assertEq(expected = Set#{}, Nema.Lab.Vivarium.queryMeasureClosed(w5));
        Assert.assertEq(expected = Some(MeasureState.Closed), Nema.Lab.Vivarium.measureState(measure, w6));
        Assert.assertEq(expected = Set#{2}, Nema.Lab.Vivarium.queryMeasureClosed(w6))
    }

    @Test
    pub def datalogReconstructsTopologyWithoutDrivingIt(): Unit \ Assert = {
        let (w1, object, pulse) = liveSeed();
        let (w2, session, _) = Nema.Lab.Vivarium.ask(pulse, object, "pending?", w1);
        let before = Nema.Lab.Vivarium.describe(w2);
        let live = Nema.Lab.Vivarium.queryLiveObservers(w2);
        let pending = Nema.Lab.Vivarium.queryPendingContinuations(w2);
        let reachable = Nema.Lab.Vivarium.queryReachableSubjects(w2);
        Assert.assertEq(expected = Set#{17}, live);
        Assert.assertEq(expected = Set#{17}, pending);
        Assert.assertEq(expected = Set#{}, reachable);
        Assert.assertEq(expected = before, Nema.Lab.Vivarium.describe(w2));
        Assert.assertEq(expected = 1, Nema.Lab.Vivarium.pulseBeat(pulse, w2));
        Assert.assertEq(expected = Nema.Lab.Vivarium.sessionAddress(session), Nema.Lab.Vivarium.sessionAddress(session))
    }
}
