mod TestInteraction {

    @Test
    pub def firstAnswerWins(): Unit \ Assert = {
        let pending = Nema.Interaction.State.Pending("selection-1", 4i64);
        let expected = Nema.Interaction.AnswerResult.Accepted(Nema.Interaction.State.Resolved("selection-1", 4i64, "branch-a"));
        Assert.assertEq(expected = expected, Nema.Interaction.answer(pending, 4i64, "branch-a"))
    }

    @Test
    pub def identicalRetryGetsReceipt(): Unit \ Assert = {
        let state = Nema.Interaction.State.Resolved("selection-1", 4i64, "branch-a");
        Assert.assertEq(
            expected = Nema.Interaction.AnswerResult.IdenticalRetry(state),
            Nema.Interaction.answer(state, 4i64, "branch-a")
        )
    }

    @Test
    pub def conflictingAnswerIsRejected(): Unit \ Assert = {
        let state = Nema.Interaction.State.Resolved("selection-1", 4i64, "branch-a");
        Assert.assertEq(
            expected = Nema.Interaction.AnswerResult.Conflict(state),
            Nema.Interaction.answer(state, 4i64, "branch-b")
        )
    }

    @Test
    pub def relevantRevisionRejectsStaleAnswer(): Unit \ Assert = {
        let pending = Nema.Interaction.State.Pending("selection-1", 4i64);
        Assert.assertEq(
            expected = Nema.Interaction.AnswerResult.Stale(pending),
            Nema.Interaction.answer(pending, 3i64, "branch-a")
        )
    }
}
