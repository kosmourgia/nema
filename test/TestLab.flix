mod TestLab {

    @Test
    pub def multiResumptionBuildsIndependentRecipes(): Unit \ Assert = {
        let actual = Nema.Lab.Continuation.explore("checkpoint-1")
            |> List.map(Nema.Lab.Continuation.render);
        let expected = List#{
            "checkpoint-1|streaming-parser|compact protocol context|read-only",
            "checkpoint-1|state-machine-parser|full raw-frame context|read-only"
        };
        Assert.assertEq(expected = expected, actual)
    }

    @Test
    pub def handlerPostActionFollowsResumedBody(): Unit \ Assert =
        Assert.assertEq(
            expected = List#{"handler:before", "body:true", "handler:after"},
            Nema.Lab.Continuation.orderSensitive()
        )

    @Test
    pub def handlerLivesInsideSpawnedWorker(): Unit \ {Assert, Chan, NonDet} =
        Assert.assertEq(expected = "handled-in-worker", Nema.Lab.Concurrency.handledInsideWorker())

    @Test
    pub def channelRoundTripWorks(): Unit \ {Assert, Chan, NonDet} =
        Assert.assertEq(expected = 42, Nema.Lab.Concurrency.channelRoundTrip())

    @Test
    pub def higherKindedAssociatedTypeAndEffectCompile(): Unit \ Assert = {
        let backend = Nema.Lab.Associated.FixtureBackend.FixtureBackend;
        let actual = Nema.Lab.Associated.loadTwice(backend, "artifact-1");
        Assert.assertEq(expected = (List#{"artifact-1"}, List#{"artifact-1"}), actual)
    }

    @Test
    pub def escapedResumptionRunsUnderLaterHandler(): Unit \ Assert =
        Assert.assertEq(
            expected = "later-handler",
            Nema.Lab.EscapedResumption.invokeUnder("later-handler")
        )

    @Test
    pub def datalogDerivesAndRetractsAffordance(): Unit \ Assert = {
        let candidates = Set#{("selection-1", "artifact-a")};
        let outputs = Set#{"artifact-a"};
        Assert.assertEq(
            expected = true,
            Nema.Lab.Affordance.isAnswerable("selection-1", candidates, outputs, Set.empty())
        );
        Assert.assertEq(
            expected = false,
            Nema.Lab.Affordance.isAnswerable("selection-1", candidates, outputs, Set#{"selection-1"})
        )
    }
}
