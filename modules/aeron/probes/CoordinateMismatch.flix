use Nema.Aeron
use Nema.Aeron.StreamId.StreamId

/// Deliberately rejected: stream IDs are not publication byte positions.
def forbiddenCoordinate(): Int64 = Aeron.positionBytes(StreamId(7))
