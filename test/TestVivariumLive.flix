mod TestVivariumLive {

    use Nema.Lab.Vivarium.Live.{BindingKind, Invocation, LiveEvent}

    @Test
    pub def inertObjectDoesNotReactBeforeAttachment(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = 40, result#initialState#revision);
        Assert.assertEq(expected = 0, List.length(result#initialState#invocations));
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.Attached(17, 3), LiveEvent.Downbeat(3, 1), result
        ))
    }

    @Test
    pub def liveObserverSuspendsAwaitingPulse(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.Waiting(17, 3), LiveEvent.Downbeat(3, 1), result
        ));
        Assert.assertEq(expected = Set#{(17, 3)}, result#attachedView#waitingOnPulse)
    }

    @Test
    pub def driverAdvancementResumesObserver(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.Downbeat(3, 1), LiveEvent.Awoke(17, 3, 1), result
        ))
    }

    @Test
    pub def observerReturnsToWaitingAfterReaction(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.Settled(3, 1), LiveEvent.Waiting(17, 3), result
        ));
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.Resumed(17, "yes", 4), LiveEvent.Waiting(17, 3), result
        ))
    }

    @Test
    pub def regionShutdownRemovesEmbodimentNotIdentity(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        let shutdown = Nema.Lab.Vivarium.Live.scopeShutdownProbe();
        Assert.assertEq(expected = (17, 40, true), shutdown);
        Assert.assertEq(expected = 17, result#finalState#object);
        Assert.assertEq(expected = List#{41, 40}, result#finalState#revisions);
        Assert.assertEq(expected = Set#{17, 18, 19}, result#finalView#dormantObjects);
        Assert.assertEq(expected = Set#{}, result#finalView#liveObservers);
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventIndex(LiveEvent.Detached(17, 3), result) != None)
    }

    @Test
    pub def abortedSystoleLeaksNoInvocation(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = 40, result#afterAbort#revision);
        Assert.assertEq(expected = List#{40}, result#afterAbort#revisions);
        Assert.assertEq(expected = 0, List.length(result#afterAbort#invocations));
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventIndex(LiveEvent.Aborted(1), result) != None)
    }

    @Test
    pub def invocationPublishesBeforeDiastolicDelivery(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = Invocation.Notify(8, 8, 17, "awake"), result#notifyInvocation);
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.Committed(8, 8, "Notify"), LiveEvent.Diastole(8), result
        ));
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.Diastole(8), LiveEvent.Delivery(8, 18, 41, "recorded:awake"), result
        ));
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventIndex(
            LiveEvent.Delivery(8, 19, 41, "witnessed:awake"), result
        ) != None)
    }

    @Test
    pub def oneBroadcastHasTwoLiveInterpretations(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = Set#{"recorded:awake", "witnessed:awake"}, result#liveInterpretations);
        Assert.assertEq(expected = result#pureInterpretations, result#liveInterpretations)
    }

    @Test
    pub def askingComputationRemainsPending(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = Invocation.Ask(9, 9, 17, 17, "continue?"), result#askInvocation);
        Assert.assertEq(expected = Set#{17}, result#pendingView#pendingContinuations);
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.AskPending(17, 9), LiveEvent.BindingChanged(17, BindingKind.Resolved("yes")), result
        ))
    }

    @Test
    pub def laterBeatResumesUnderNewContextThenWaitsAgain(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = 4, result#resumedBeat);
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.Downbeat(3, 4), LiveEvent.Resumed(17, "yes", 4), result
        ));
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.Resumed(17, "yes", 4), LiveEvent.Downbeat(3, 5), result
        ))
    }

    @Test
    pub def continuationAddressSurvivesEveryBinding(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(
            expected = List#{BindingKind.Live, BindingKind.Durable("recipe:continue"), BindingKind.Live, BindingKind.Resolved("yes")},
            result#bindingHistory
        );
        Assert.assertEq(expected = Some((17, BindingKind.Resolved("yes"))), result#finalState#continuation)
    }

    @Test
    pub def measurePulsesAdvanceIndependently(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = List#{6, 7, 8, 9}, result#measure#p3Beats);
        Assert.assertEq(expected = List#{9, 10, 11}, result#measure#p4Beats);
        Assert.assertEq(expected = false, result#measure#p3Beats == result#measure#p4Beats)
    }

    @Test
    pub def measureOpeningWaitsForBothFrontiers(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = true, result#measure#gatheringAfterFirst);
        Assert.assertEq(expected = List#{(3, 7), (4, 9)}, result#measure#opening);
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.MeasureGathering(2, 4), LiveEvent.MeasureOpened(2, 7, 9), result
        ))
    }

    @Test
    pub def measureClosingWaitsForObligations(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        Assert.assertEq(expected = true, result#measure#openBeforeAllObligations);
        Assert.assertEq(expected = Set#{"answer", "question"}, result#measure#obligations);
        Assert.assertEq(expected = List#{(3, 9), (4, 11)}, result#measure#closing);
        Assert.assertEq(expected = true, Nema.Lab.Vivarium.Live.eventBefore(
            LiveEvent.ObligationCompleted(2, "answer"), LiveEvent.MeasureClosed(2, 9, 11), result
        ))
    }

    @Test
    pub def datalogDescribesTopologyWithoutTransitions(): Unit \ {Assert, Chan, NonDet} = {
        let result = Nema.Lab.Vivarium.Live.scenario();
        let first = Nema.Lab.Vivarium.Live.queryTopology(result#finalTopology);
        let second = Nema.Lab.Vivarium.Live.queryTopology(result#finalTopology);
        Assert.assertEq(expected = result#finalView#dormantObjects, first#dormantObjects);
        Assert.assertEq(expected = first#dormantObjects, second#dormantObjects);
        Assert.assertEq(expected = Set#{2}, first#measuresReady);
        Assert.assertEq(expected = Set#{2}, first#measuresClosed);
        Assert.assertEq(expected = Set#{(18, 17), (19, 17)}, result#attachedView#reachableSubjects);
        Assert.assertEq(expected = Set#{17, 18, 19}, result#attachedView#liveObservers);
        Assert.assertEq(expected = Set#{3}, result#attachedTopology#pulses);
        Assert.assertEq(expected = Set#{}, result#attachedTopology#invocations);
        Assert.assertEq(expected = Set#{8, 9}, result#pendingTopology#invocations);
        Assert.assertEq(expected = Set#{3, 4}, result#finalTopology#pulses)
    }
}
