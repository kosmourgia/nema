mod TestCapture {

    @Test
    pub def jsonRpcIdTypesRemainDistinct(): Unit \ Assert = {
        let numeric = Nema.Capture.correlationKey("connection-a", 1i64, Nema.Capture.Peer.Codex, Nema.Capture.RpcId.IntegerId(7i64));
        let textual = Nema.Capture.correlationKey("connection-a", 1i64, Nema.Capture.Peer.Codex, Nema.Capture.RpcId.StringId("7"));
        Assert.assertEq(expected = false, numeric == textual)
    }

    @Test
    pub def initiatorRemainsPartOfCorrelation(): Unit \ Assert = {
        let codex = Nema.Capture.correlationKey("connection-a", 1i64, Nema.Capture.Peer.Codex, Nema.Capture.RpcId.StringId("same"));
        let nema = Nema.Capture.correlationKey("connection-a", 1i64, Nema.Capture.Peer.NemaClient, Nema.Capture.RpcId.StringId("same"));
        Assert.assertEq(expected = false, codex == nema)
    }
}
