use Nema.Aeron
use Nema.Aeron.Position.Position
use Nema.Aeron.{Offer, Position}
use Nema.Aeron.Examples
use Sys.Env

@Test
def offerCodes(): Unit \ Assert = {
    Assert.assertEq(expected = Offer.NotConnected, Aeron.offerOutcome(-1i64));
    Assert.assertEq(expected = Offer.Backpressured, Aeron.offerOutcome(-2i64));
    Assert.assertEq(expected = Offer.AdministrativeRetry, Aeron.offerOutcome(-3i64));
    Assert.assertEq(expected = Offer.Closed, Aeron.offerOutcome(-4i64));
    Assert.assertEq(expected = Offer.PositionExhausted, Aeron.offerOutcome(-5i64));
    Assert.assertEq(expected = Offer.Accepted(Position(64i64)), Aeron.offerOutcome(64i64))
}

@Test
def fixtureEffect(): Unit \ Assert = Assert.assertEq(expected = Ok(true), Examples.fixtureRoundTrip())

@Test
def liveEffectIpc(): Unit \ {Assert, IO, Env} = {
    let path = Env.getVar("NEMA_AERON_RUN") |> Option.getWithDefault(".nema/test");
    Assert.assertEq(expected = Ok(true), Examples.ipcRoundTrip("${path}/effect-driver"));
    Assert.assertEq(expected = 0, Aeron.resourceCount())
}
