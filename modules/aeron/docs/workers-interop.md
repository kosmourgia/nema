# Flix polling workers and independent UDP peers

`Workers.withWorker(subscription, cancel, body)` starts one Flix process in a
region and installs its `Source` effect handler inside that process. Only the
worker polls this subscription. Ordinary `Aeron.poll` / `Aeron.take` remain
available when the caller owns the duty cycle directly. Do not combine those
operations, a Java polling worker, and a Flix polling worker on one subscription.

```flix
Aeron.withDriverCsp(path, _ ->
    Aeron.withClientCsp(path, client ->
        Aeron.withSubscriptionCsp(client, Aeron.channel("aeron:ipc"),
            Aeron.stream(101), 8, subscription -> {
                let cancel = Aeron.cancelToken();
                Workers.withWorker(subscription, cancel, worker ->
                    Workers.next(worker, Aeron.deadlineAfterMillis(5000i64)))
            })))
```

The callback effects are explicitly `{IO, Chan, NonDet}`. Install application
effect handlers inside the callback: an outside handler that abandons a
continuation must not bypass a native resource scope's cleanup.

## Bounded staging and shutdown

The subscription's configured staging capacity bounds fully assembled owned
messages. When staging is full, the released Aeron `ControlledFragmentAssembler`
callback returns `ABORT`. Aeron retries that delivery after capacity becomes
available; the callback checks capacity before copying/enqueuing. The Flix
worker does not remove a staged message until a consumer requests one.

Demand crosses an actual Flix CSP channel of capacity one. An atomic permit
rejects a concurrent outstanding demand explicitly. Every request carries a
fresh reply channel of capacity one, so its single reply cannot block even if
the receiving caller has cancelled. Total adapter staging is the configured
subscription queue plus at most one in-flight owned reply, with native Aeron
term buffers and fragment assembly storage additional to that bound.

Both the worker and consumer use nonblocking channel selection and the shared
cancel token's one-millisecond park. `Aeron.stop(cancel)` is level triggered and
wakes parked threads. The scope stops the token, receives the completion report,
and joins the Flix region before returning. Normal exit and Java exceptions
both follow this cleanup path. Poll ownership is released by the worker itself.
Native polling failures are retained, returned to demand calls, and reported
again by scope exit even if the body ignores an individual error.

A deadline before delivery leaves staged data available to a later demand.
Cancellation during delivery may abandon the final in-flight reply; cancellation
is a shutdown boundary, not a receipt that proves the consumer applied a frame.
Use application recording coordinates/checkpoints and replay for recovery.
After Stop, `next` returns `Cancelled`. Handles are dynamically scoped; Flix
does not statically prevent retaining a handle past its scope.

The default parks after each idle duty cycle and when downstream is slow. It
does not spin an unattended VM. Ordering is per publication/image. Two sessions
do not acquire a shared total order by using this adapter.

## Reproduction

```sh
modules/aeron/bin/aeron test
modules/aeron/scripts/udp-interop.sh
```

`TestWorkers.flix` exercises idle Stop and poll ownership release, cancellation
during a pending receive, repeated deadlines in one usable scope, a slow
consumer saturating capacity two while preserving six ordered messages, and
an injected conflicting polling owner whose failure reaches scope exit.

`Interop.sendMain` and `Interop.receiveMain` are real Flix entrypoints. The
script runs each against `NativePeer`, an independent Java program which uses
raw Aeron publication/subscription APIs and never calls `Transport`. Each peer
owns a distinct fresh driver. UDP receivers bind `127.0.0.1:0` and write Aeron's
resolved channel URI into a readiness file; senders use that exact OS-allocated
port. All driver directories, logs, readiness files, and compiler output are
under this worktree's module `.nema` directory. Each launcher invocation builds
in a fresh private stage; the independent native fixtures also compile into
this run's private directory.

Both directions transfer six 16,384-byte messages over two publication sessions.
The deterministic payload includes all byte values, an embedded NUL, UTF-8,
and enough bytes to require fragmentation at the configured 1408-byte MTU.
Receivers verify bytes, exact count, and an additional quiet interval for
duplicate delivery. The Flix receiver also verifies two distinct native session
IDs. Peers and readiness waits have finite deadlines; script cleanup terminates
its children on failure. Logs remain available for diagnosis.

The Flix 0.75.3 source shipped in the installed compiler and the
[Flix concurrency documentation](https://doc.flix.dev/concurrency.html) confirm
that region exit joins spawned processes and `select` with a default does not
block. The implementation uses those observed APIs rather than assuming a
cancellable `Channel.send` operation exists.
