# Local IPC protocol

`./bin/nema serve` listens on `.nema/nema.sock` with mode `0600`. Each request
and response is one UTF-8 JSON object followed by LF. The current protocol is
version 1; request IDs are echoed without changing their JSON type.

```json
{"version":1,"id":"example","method":"surface.get","params":{}}
```

Methods are `health`, `surface.get`, `events.list`, `interaction.respond`,
`artifact.get`, `fork.inspect`, `hook.capture`, `rendezvous.join`,
`rendezvous.inspect`, and `rendezvous.cancel`. `events.list` accepts an
`afterRevision` watermark from a previous surface. Each client has its own
handler, so a slow or partial frame cannot block control traffic from another
client. Requests larger than 1 MiB are rejected.

`rendezvous.join` waits only for its supplied timeout (maximum five minutes).
Each join names a key, integer generation, participant, JSON payload, party
count, and timeout. A release returns peer messages. Timeout, explicit
cancellation, and a detected disconnect cancel only that generation.

The standard-library reference client is available through:

```sh
./bin/nema ipc health '{}'
./bin/nema ipc surface.get '{}'
./bin/nema ipc events.list '{"afterRevision":0}'
./bin/nema rendezvous example --generation 1 --participant alpha \
  --payload '{"message":"hello"}' --parties 2 --timeout 30s
```

The checked-in `ops/nema.service` is a project-specific user-unit template.
Install it explicitly if wanted; Nema does not enable linger or boot startup.

`hook.capture` is an internal raw-ingress method. Its byte payload is base64 and
the digest and original length are independently checked before commit. See
`docs/hooks.md` for the non-blocking fallback path.
