# Nema

Nema is an observable, Flix-first membrane and fork bench for Codex app-server.
The current seed retains bidirectional protocol traffic, derives versioned
interaction views without overwriting raw observations, and runs inspectable
fixture and native two-branch experiments through selection and parent delivery.

## Run it

```sh
./bin/nema doctor
./bin/nema check
./bin/nema test

# Compact Flix specimen: Objects, Pulses, Beats, Measures, and resumptions.
./bin/nema lab vivarium
./bin/nema lab vivarium-live

# No model request: real initialize + capability probe.
./bin/nema probe app-server

# Exact malformed/unknown/bidirectional capture and replay.
./bin/nema demo bidirectional --mode fixture

# Two branch artifacts, shared selection, restart-safe controller, result bundle.
./bin/nema demo fork-compare --mode fixture

# Small bounded live experiments; these make model requests.
./bin/nema demo live-tool --mode live
./bin/nema demo fork-compare --mode live

# Local daemon, hook state, and independent-process rendezvous.
./bin/nema serve
./bin/nema hooks inspect
./bin/nema rendezvous example --generation 1 \
  --participant alpha --payload '{"message":"hello"}' --parties 2 --timeout 30s

./bin/nema surface --format terminal
./bin/nema surface --format json
./bin/nema surface --format prompt
./bin/nema replay
```

The fork fixture intentionally starts, renders, answers, and advances through
separate processes. The live mode separately proves native persisted and
ephemeral historical forks. Neither mode claims to serialize a Flix stack.

Individual interactions are available from the same entry point:

```sh
./bin/nema interaction create example-choice \
  --prompt 'Choose one' --candidate a='Candidate A' --candidate b='Candidate B'
./bin/nema interaction list
./bin/nema interaction respond example-choice --revision 1 --answer a
./bin/nema changes --after 0
```

Use the exact `relevantInputRevision` shown by `interaction list`; unrelated
global events do not invalidate it. Immutable artifacts can be inspected with
`./bin/nema artifact show ID`.

## Requirements

The canonical Arch ARM guest currently passes on:

- Flix 0.75.3
- OpenJDK 26.0.2.1 (Flix requires Java 21 or newer)
- Codex CLI/app-server 0.154.0
- Python 3.14.7
- SQLite 3.53.4

Runtime databases, raw traces, sockets, and private model traffic belong under
`.nema/` and are ignored. The checked-in protocol report is sanitized.

Project lifecycle hooks are checked in but run only after Codex trust review.
Use `./bin/nema hooks install` for an additive/idempotent merge, `/hooks` in
Codex to review, `./bin/nema hooks inspect` for status, and
`./bin/nema hooks drain` after an offline period. See
[docs/hooks.md](docs/hooks.md) and [docs/ipc.md](docs/ipc.md).

The optional user service is not installed automatically:

```sh
systemctl --user link "$PWD/ops/nema.service"
systemctl --user enable --now nema.service
systemctl --user status nema.service
systemctl --user disable --now nema.service
```

Read [PROGRESS.md](PROGRESS.md), [ARCHITECTURE.md](ARCHITECTURE.md), and
[docs/capabilities.md](docs/capabilities.md) for the exact boundary. The
standalone Flix thought experiment has a verified walkthrough in
[docs/vivarium.md](docs/vivarium.md), with its live effects/CSP realization in
[docs/vivarium-live.md](docs/vivarium-live.md).
`NEMA_V0_HANDOFF.md` remains the scope brief.
