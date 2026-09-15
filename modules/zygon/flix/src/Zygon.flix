import zygon.flix.Bridge
import zygon.flix.{Bridge$Doc => Doc}
import java.lang.Exception

pub mod Zygon {
    pub type alias Identity = { id = String, incarnation = String }
    pub type alias Registration = {
        identity = Identity, kind = String, medium = String, mode = String,
        parent = Option[Identity], operations = List[String], active = Bool,
        presence = String, binding = Option[String], raw = String
    }
    pub enum Status with Eq, ToString {
        case Accepted
        case Started
        case Completed
        case Failed
        case Cancelled
        case OutcomeUnknown
        case NativeUnknown(String)
    }
    pub type alias Request = {
        requester = Identity, target = Identity, operation = String,
        arguments = String, correlation = String
    }
    pub type alias Invocation = {
        id = String, requester = Identity, target = Identity, operation = String,
        correlation = String, arguments = String, status = Status, raw = String
    }
    pub type alias Result = {
        invocationId = String, correlation = String, status = Status,
        value = String, recordId = String, raw = String
    }
    pub type alias Observation = {
        id = String, source = Identity, kind = String, sequence = String, raw = String
    }
    pub type alias Snapshot = {
        cursor = String, registrations = List[Registration], invocations = List[Invocation]
    }
    pub type alias Topology = {
        hosts = Set[(String, String)], surfaces = Set[(String, String)],
        live = Set[(String, String)], bindings = Set[(String, String, String)],
        pending = Set[(String, String, String)], reachable = Set[(String, String, String, String)]
    }
    pub enum Event {
        case Observed(Observation)
        case Resolved(Result)
    }
    pub eff LocalRequest { def invoke(request: Request): Result }

    pub def status(value: String): Status = match value {
        case "accepted" => Status.Accepted
        case "started" => Status.Started
        case "completed" => Status.Completed
        case "failed" => Status.Failed
        case "cancelled" => Status.Cancelled
        case "outcome-unknown" => Status.OutcomeUnknown
        case other => Status.NativeUnknown(other)
    }
    pub def terminal(value: Status): Bool = match value {
        case Status.Completed => true
        case Status.Failed => true
        case Status.Cancelled => true
        case Status.OutcomeUnknown => true
        case _ => false
    }
    pub def identity(doc: Doc): Identity \ IO = {
        id = doc.string("id"), incarnation = doc.string("incarnation")
    }
    def operations(doc: Doc, index: Int32): List[String] \ IO =
        if (index >= doc.count("operations")) Nil
        else doc.stringItem("operations", index) :: operations(doc, index + 1)
    pub def registration(doc: Doc): Registration \ IO = {
        identity = identity(doc), kind = doc.string("kind"), medium = doc.string("medium"),
        mode = doc.string("mode"), parent = if (doc.has("parent")) Some(identity(doc.object("parent"))) else None,
        operations = operations(doc, 0), active = doc.bool("active"), presence = doc.string("presence"),
        binding = if (doc.has("binding")) Some(doc.json("binding")) else None, raw = doc.raw()
    }
    pub def invocation(doc: Doc): Invocation \ IO = {
        id = doc.string("id"), requester = identity(doc.object("requester")),
        target = identity(doc.object("target")), operation = doc.string("operation"),
        correlation = doc.string("correlation"), arguments = doc.json("arguments"),
        status = status(doc.string("status")), raw = doc.raw()
    }
    pub def result(doc: Doc, recordId: String): Result \ IO = {
        invocationId = doc.string("id"), correlation = doc.string("correlation"),
        status = status(doc.string("status")), value = doc.json("value"), recordId = recordId, raw = doc.raw()
    }
    pub def observation(doc: Doc): Observation \ IO = {
        id = doc.string("id"), source = {id = doc.string("source"), incarnation = doc.string("incarnation")},
        kind = doc.string("kind"), sequence = doc.string("seq"), raw = doc.raw()
    }
    def registrations(doc: Doc, index: Int32): List[Registration] \ IO =
        if (index >= doc.count("registrations")) Nil
        else registration(doc.item("registrations", index)) :: registrations(doc, index + 1)
    def invocations(doc: Doc, index: Int32): List[Invocation] \ IO =
        if (index >= doc.count("invocations")) Nil
        else invocation(doc.item("invocations", index)) :: invocations(doc, index + 1)
    pub def snapshot(doc: Doc): Snapshot \ IO = {
        cursor = doc.string("cursor"), registrations = registrations(doc, 0), invocations = invocations(doc, 0)
    }

    /// Pure finite-snapshot projection. No rule sends a command or starts a host.
    pub def topology(world: Snapshot): Topology = {
        let hosts = world#registrations |> List.filter(r -> r#kind == "host")
            |> List.map(r -> (r#identity#id, r#identity#incarnation)) |> List.toSet;
        let surfaces = world#registrations |> List.filter(r -> r#kind == "surface" or r#kind == "document")
            |> List.map(r -> (r#identity#id, r#identity#incarnation)) |> List.toSet;
        let live = world#registrations |> List.filter(r -> r#active and r#presence == "online")
            |> List.map(r -> (r#identity#id, r#identity#incarnation)) |> List.toSet;
        let children = world#registrations |> List.filterMap(r -> match r#parent {
            case None => None
            case Some(p) => Some((r#identity#id, r#identity#incarnation, p#id, p#incarnation))
        }) |> List.toSet;
        let bindings = world#registrations |> List.filterMap(r -> match r#binding {
            case None => None
            case Some(b) => Some((r#identity#id, r#identity#incarnation, b))
        }) |> List.toSet;
        let pending = world#invocations |> List.filter(i -> i#status == Status.Accepted or i#status == Status.Started)
            |> List.map(i -> (i#id, i#target#id, i#target#incarnation)) |> List.toSet;
        let facts = inject hosts, surfaces, live, children, bindings, pending
            into Host/2, Surface/2, Live/2, Child/4, Binding/3, Pending/3;
        let rules = #{
            Reachable(p, pi, c, ci) :- Child(c, ci, p, pi), Live(p, pi), Live(c, ci).
            Reachable(p, pi, c, ci) :- Reachable(p, pi, m, mi), Child(c, ci, m, mi), Live(c, ci).
        };
        {
            hosts = query facts select (id, inc) from Host(id, inc) |> Vector.toSet,
            surfaces = query facts select (id, inc) from Surface(id, inc) |> Vector.toSet,
            live = query facts select (id, inc) from Live(id, inc) |> Vector.toSet,
            bindings = query facts select (id, inc, binding) from Binding(id, inc, binding) |> Vector.toSet,
            pending = query facts select (id, target, inc) from Pending(id, target, inc) |> Vector.toSet,
            reachable = query facts, rules select (p, pi, c, ci) from Reachable(p, pi, c, ci) |> Vector.toSet
        }
    }
    pub def printTopology(label: String, world: Snapshot): Unit \ IO = {
        let view = topology(world);
        println("FLIX ${label} cursor=${world#cursor} hosts=${view#hosts} surfaces=${view#surfaces}");
        println("FLIX ${label} live=${view#live} bindings=${view#bindings}");
        println("FLIX ${label} pending=${view#pending} reachable=${view#reachable}")
    }
    /// The effect handler lives inside this spawned worker. Its continuation
    /// waits on one CSP channel while observations use a separate channel.
    def requestWorker(control: Bridge, request: Request, accepted: Sender[Invocation],
                      reply: Receiver[Result], events: Sender[Event]): Unit \ {IO, Chan, NonDet} =
        run { Channel.send(Event.Resolved(LocalRequest.invoke(request)), events) }
        with handler LocalRequest {
            def invoke(r, resume) = {
                let doc = control.invoke(r#requester#id, r#requester#incarnation,
                    r#target#id, r#target#incarnation, r#operation, r#arguments, r#correlation);
                Channel.send(invocation(doc), accepted);
                resume(Channel.recv(reply))
            }
        }
    def scanPage(page: Doc, index: Int32, callId: String, acceptedSeen: Bool,
                 replies: Sender[Result], events: Sender[Event]): (Bool, Bool) \ {IO, Chan, NonDet} =
        if (index >= page.count("records")) (false, acceptedSeen) else {
            let record = page.item("records", index);
            let kind = record.string("kind");
            let body = record.object("body");
            let isCall = String.startsWith("invocation.", kind) and body.has("id") and body.string("id") == callId;
            if (isCall and body.has("status") and terminal(status(body.string("status")))) {
                Channel.send(result(body, record.string("id")), replies);
                (true, acceptedSeen)
            } else {
                let nowAccepted = acceptedSeen or (isCall and kind == "invocation.accepted");
                if (nowAccepted and record.string("source") != "zygon.registry")
                    Channel.send(Event.Observed(observation(record)), events)
                else ();
                scanPage(page, index + 1, callId, nowAccepted, replies, events)
            }
        }
    def watch(observer: Bridge, cursor: String, callId: String, acceptedSeen: Bool,
              replies: Sender[Result], events: Sender[Event]): Unit \ {IO, Chan, NonDet} = {
        let page = observer.subscribe(cursor);
        let (done, seen) = scanPage(page, 0, callId, acceptedSeen, replies, events);
        if (done) () else {
            if (page.bool("more")) () else Bridge.pause();
            watch(observer, page.string("cursor"), callId, seen, replies, events)
        }
    }
    def consume(events: Receiver[Event], observations: Int32): (Result, Int32) \ {IO, Chan, NonDet} =
        match Channel.recv(events) {
            case Event.Observed(o) =>
                println("FLIX observation while request waits id=${o#id} source=${o#source#id} incarnation=${o#source#incarnation} seq=${o#sequence} kind=${o#kind}");
                consume(events, observations + 1)
            case Event.Resolved(r) => (r, observations)
        }
    def exercise(control: Bridge, observer: Bridge, request: Request, before: Snapshot): (Result, Int32) \ {IO, Chan, NonDet} = region rc {
        let (acceptedTx, acceptedRx) = Channel.unbuffered();
        let (replyTx, replyRx) = Channel.unbuffered();
        let (eventTx, eventRx) = Channel.unbuffered();
        spawn requestWorker(control, request, acceptedTx, replyRx, eventTx) @ rc;
        let accepted = Channel.recv(acceptedRx);
        println("FLIX accepted id=${accepted#id} correlation=${accepted#correlation} status=${accepted#status}");
        printTopology("waiting", snapshot(control.inspect()));
        spawn watch(observer, before#cursor, accepted#id, false, replyTx, eventTx) @ rc;
        consume(eventRx, 0)
    }
    pub def main(): Unit \ {IO, Chan, NonDet} = {
        let socket = Bridge.environment("ZYGON_SOCKET", "");
        Bridge.require(socket != "", "ZYGON_SOCKET is required");
        let control = new Bridge(socket);
        let observer = new Bridge(socket);
        let caller = { id = Bridge.fresh("flix-client-"), incarnation = Bridge.fresh("flix-session-") };
        try {
            let registered = registration(control.register(caller#id, caller#incarnation, "client", "unix-socket/flix-csp", "observer"));
            println("FLIX participant id=${registered#identity#id} incarnation=${registered#identity#incarnation}");
            let before = snapshot(control.inspect());
            printTopology("before", before);
            let request = {
                requester = caller,
                target = { id = Bridge.environment("ZYGON_TARGET_ID", ""), incarnation = Bridge.environment("ZYGON_TARGET_INCARNATION", "") },
                operation = Bridge.environment("ZYGON_OPERATION", "demo.delay"),
                arguments = Bridge.environment("ZYGON_ARGUMENTS", "{}"), correlation = Bridge.fresh("flix-request-")
            };
            let (resolved, observations) = exercise(control, observer, request, before);
            Bridge.require(resolved#correlation == request#correlation, "Flix correlation changed while waiting");
            if (Bridge.environment("ZYGON_REQUIRE_OBSERVATION", "0") == "1")
                Bridge.require(observations > 0, "No observation arrived during this pending invocation")
            else ();
            println("FLIX resolved id=${resolved#invocationId} correlation=${resolved#correlation} status=${resolved#status} recordId=${resolved#recordId} observations=${observations} value=${resolved#value}");
            printTopology("after", snapshot(control.inspect()));
            control.unregister(caller#id, caller#incarnation);
            control.close(); observer.close()
        } catch {
            case error: Exception => control.close(); observer.close(); throw error
        }
    }
}
