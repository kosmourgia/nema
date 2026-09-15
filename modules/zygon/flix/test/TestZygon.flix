mod TestZygon {
    def reg(id: String, inc: String, kind: String, parent: Option[Zygon.Identity],
            presence: String, binding: Option[String]): Zygon.Registration = {
        identity = { id = id, incarnation = inc }, kind = kind, medium = "fixture",
        mode = "attached", parent = parent, operations = List#{"fixture.echo"},
        active = true, presence = presence, binding = binding, raw = "{}"
    }
    def pending(status: Zygon.Status): Zygon.Invocation = {
        id = "call-1", requester = { id = "client", incarnation = "client-inc" },
        target = { id = "page", incarnation = "document-1" }, operation = "fixture.echo",
        correlation = "original-correlation", arguments = "{}", status = status, raw = "{}"
    }

    @Test
    pub def nestedReachabilityRequiresExactLiveIncarnations(): Unit \ Assert = {
        let host = reg("host", "host-1", "host", None, "online", Some("{\"pid\":4242}"));
        let child = reg("child", "child-1", "surface", Some(host#identity), "online", None);
        let page = reg("page", "document-1", "document", Some(child#identity), "online", None);
        let before = Zygon.topology({cursor = "7", registrations = List#{host, child, page}, invocations = List#{pending(Zygon.Status.Started)}});
        Assert.assertEq(expected = Set#{("host", "host-1")}, before#hosts);
        Assert.assertEq(expected = Set#{("child", "child-1"), ("page", "document-1")}, before#surfaces);
        Assert.assertEq(expected = Set#{("host", "host-1", "child", "child-1"), ("host", "host-1", "page", "document-1"), ("child", "child-1", "page", "document-1")}, before#reachable);
        Assert.assertEq(expected = Set#{("call-1", "page", "document-1")}, before#pending);
        let restarted = reg("host", "host-2", "host", None, "online", Some("{\"pid\":4242}"));
        let after = Zygon.topology({cursor = "8", registrations = List#{restarted, child, page}, invocations = Nil});
        Assert.assertEq(expected = Set#{("child", "child-1", "page", "document-1")}, after#reachable)
    }

    @Test
    pub def unknownPresenceIsNotDeathAndUnavailableBindingStaysUnknown(): Unit \ Assert = {
        let host = reg("host", "host-1", "host", None, "unknown", None);
        let child = reg("child", "child-1", "surface", Some(host#identity), "online", None);
        let view = Zygon.topology({cursor = "9", registrations = List#{host, child}, invocations = Nil});
        Assert.assertEq(expected = Set#{("host", "host-1")}, view#hosts);
        Assert.assertEq(expected = Set#{("child", "child-1")}, view#live);
        Assert.assertEq(expected = Set.empty(), view#reachable);
        Assert.assertEq(expected = Set.empty(), view#bindings)
    }

    @Test
    pub def terminalAndUnknownNativeStatusesDoNotInventPending(): Unit \ Assert = {
        let check = s -> Zygon.topology({cursor = "0", registrations = Nil, invocations = List#{pending(s)}})#pending;
        Assert.assertEq(expected = Set.empty(), check(Zygon.Status.Completed));
        Assert.assertEq(expected = Set.empty(), check(Zygon.Status.OutcomeUnknown));
        Assert.assertEq(expected = Set.empty(), check(Zygon.status("future-host-status")));
        Assert.assertEq(expected = Zygon.Status.NativeUnknown("future-host-status"), Zygon.status("future-host-status"))
    }

    @Test
    pub def typedEffectResumesWithOriginalCorrelation(): Unit \ Assert = {
        let request = { requester = { id = "client", incarnation = "client-1" },
            target = { id = "host", incarnation = "host-1" }, operation = "fixture.echo",
            arguments = "{}", correlation = "original-correlation" };
        let result = run { Zygon.LocalRequest.invoke(request) } with handler Zygon.LocalRequest {
            def invoke(r, resume) = resume({invocationId = "fixture-call", correlation = r#correlation,
                status = Zygon.Status.Completed, value = "{\"fixture\":true}", recordId = "fixture-record", raw = "{}"})
        };
        Assert.assertEq(expected = request#correlation, result#correlation);
        Assert.assertEq(expected = Zygon.Status.Completed, result#status)
    }
}
