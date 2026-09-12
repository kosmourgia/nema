pub mod Nema.Lab.Affordance {

    /// A bounded Datalog projection: a selection is answerable only when it
    /// has a candidate whose output exists and the selection is unresolved.
    pub def isAnswerable(
        selection: String,
        candidates: Set[(String, String)],
        completedOutputs: Set[String],
        resolvedSelections: Set[String]
    ): Bool = {
        let rules = #{
            Answerable(s) :- Candidate(s, artifact), Output(artifact), not Resolved(s).
        };
        let facts = inject candidates, completedOutputs, resolvedSelections into Candidate/2, Output/1, Resolved/1;
        let answers = query facts, rules select true from Answerable(selection);
        not Vector.isEmpty(answers)
    }
}
