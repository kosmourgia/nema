use Nema.Aeron.{ArchiveIdentity, RecordingKey, Position}
use Nema.Aeron.Retention
use Nema.Aeron.ArchiveIdentity.ArchiveIdentity
use Nema.Aeron.RecordingKey.RecordingKey
use Nema.Aeron.Position.Position
use Nema.Aeron.Retention.Satisfied.Satisfied
use Nema.Aeron.Retention.ReplayPin.ReplayPin
use Nema.Aeron.Retention.{Satisfied, ReplayPin, CapacityDecision}

@Test
def contiguousObligationsAndPins(): Unit \ Assert = {
    let key = RecordingKey(ArchiveIdentity("archive-a/incarnation-a"), 7i64);
    let other = RecordingKey(ArchiveIdentity("archive-a/incarnation-b"), 7i64);
    let ranges = Satisfied("a", key, Position(128i64), Position(192i64)) ::
        Satisfied("a", key, Position(0i64), Position(64i64)) ::
        Satisfied("a", other, Position(64i64), Position(128i64)) ::
        Satisfied("b", key, Position(0i64), Position(128i64)) :: Nil;
    Assert.assertEq(expected = Position(64i64), Retention.through("a", key, Position(0i64), ranges));
    let filled = Satisfied("a", key, Position(64i64), Position(128i64)) :: ranges;
    Assert.assertEq(expected = Position(192i64), Retention.through("a", key, Position(0i64), filled));
    Assert.assertEq(expected = Position(128i64), Retention.reclaimable(key, Position(0i64), "a" :: "b" :: Nil, filled, Nil));
    Assert.assertEq(expected = Position(64i64), Retention.reclaimable(key, Position(0i64), "a" :: "b" :: Nil,
        filled, ReplayPin(key, Position(64i64)) :: Nil));
    Assert.assertEq(expected = Position(0i64), Retention.reclaimable(key, Position(0i64), Nil, filled, Nil))
}

@Test
def segmentOriginAndStorageCap(): Unit \ Assert = {
    Assert.assertEq(expected = Position(196608i64), Retention.segmentBoundary(Position(65568i64), Position(210000i64), 65536, 131072));
    Assert.assertEq(expected = CapacityDecision.AllowPublish, Retention.capacity(90i64, 10i64, 100i64));
    Assert.assertEq(expected = CapacityDecision.StopAtCap, Retention.capacity(90i64, 11i64, 100i64))
}
