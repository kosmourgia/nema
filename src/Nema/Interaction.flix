pub mod Nema.Interaction {

    pub enum State with Eq, ToString {
        case Pending(String, Int64),
        case Resolved(String, Int64, String),
        case Cancelled(String, Int64),
        case Disconnected(String, Int64)
    }

    pub enum AnswerResult with Eq, ToString {
        case Accepted(State),
        case IdenticalRetry(State),
        case Conflict(State),
        case Stale(State),
        case Unavailable(State)
    }

    /// Applies one answer against the interaction's relevant input revision.
    /// An unrelated journal append is deliberately not part of this contract.
    pub def answer(state: State, relevantRevision: Int64, value: String): AnswerResult = match state {
        case State.Pending(id, expectedRevision) =>
            if (relevantRevision == expectedRevision)
                AnswerResult.Accepted(State.Resolved(id, expectedRevision, value))
            else
                AnswerResult.Stale(state)
        case State.Resolved(_, _, accepted) =>
            if (accepted == value)
                AnswerResult.IdenticalRetry(state)
            else
                AnswerResult.Conflict(state)
        case State.Cancelled(_, _) => AnswerResult.Unavailable(state)
        case State.Disconnected(_, _) => AnswerResult.Unavailable(state)
    }
}
