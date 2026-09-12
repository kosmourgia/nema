pub mod Nema.Capture {

    /// The peer that originated an observable protocol frame.
    pub enum Peer with Eq, ToString {
        case Codex,
        case NemaClient
    }

    /// JSON-RPC identifiers retain their JSON type.
    pub enum RpcId with Eq, ToString {
        case IntegerId(Int64),
        case StringId(String)
    }

    /// A correlation key is scoped to one connection epoch and initiator.
    pub enum CorrelationKey with Eq, ToString {
        case CorrelationKey(String, Int64, Peer, RpcId)
    }

    /// Classification is derived from, and never replaces, the raw frame.
    pub enum FrameKind with Eq, ToString {
        case Request(String, RpcId),
        case Response(RpcId),
        case Notification(String),
        case Unknown,
        case Malformed(String)
    }

    /// The smallest useful retained observation. `availableBytes` is the exact
    /// text made available by the line-oriented transport, including fields the
    /// current decoder does not understand.
    pub enum RawRecord with Eq, ToString {
        case RawRecord(String, Int64, Peer, Int64, String, FrameKind)
    }

    pub def correlationKey(connectionId: String, epoch: Int64, initiator: Peer, id: RpcId): CorrelationKey =
        CorrelationKey.CorrelationKey(connectionId, epoch, initiator, id)
}
