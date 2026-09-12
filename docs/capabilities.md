# Capability ledger

Observed against the canonical Arch guest on 12 September 2026. Generated wire
shapes alone are never counted as runtime support.

| Capability | Status | Evidence / limit |
|---|---|---|
| app-server stdio + experimental negotiation | supported | Real 0.154.0 process initialized, returned 140 features, and exited 0 |
| thread start | supported live | Persisted and ephemeral threads created in bounded live runs |
| persisted thread read with turns | supported live | Exact completed source history retained as an artifact |
| ephemeral thread read with turns | unsupported by observed contract | `includeTurns` returned `ephemeral threads do not support includeTurns`; completion notifications remain observable |
| thread resume | untested | Schema exists; no runtime claim |
| turn start/completion | supported live | Tool, branch, chooser, and parent-delivery turns completed |
| turn interrupt / steering | untested | Schema exists; no runtime claim |
| persisted historical fork | supported live | Forked from explicit completed `lastTurnId`; native `forkedFromId` verified |
| ephemeral historical fork | supported live | Required `excludeTurns: true` with paginated history; output retained by Nema after exit |
| dynamic tools | supported live | `item/tool/call` -> durable interaction -> separate client answer -> same turn continued |
| parent reintegration | supported live as next turn | Exact attributed bundle input persisted before send and parent acknowledged it |
| native history injection | untested | Not used as a substitute for explicit delivery |
| project hooks | supported live | Actual SessionStart/UserPromptSubmit/Stop/SessionEnd inputs spooled and ingested exactly |
| exact/unknown/malformed raw retention | supported | Real and fixture records are immutable blobs; typed interpretation is separate |
| bidirectional ID correlation | supported | Initiator and JSON ID type distinguish integer `1` from string `"1"` |
| non-blocking request routing | supported | Fixture delay and live custom-tool wait do not stop routing/observation |
| deterministic replay | supported | Raw, interaction, fork, artifact, and rendezvous checks rebuild without model calls |
| single-answer interaction + three views | supported | Restart, stale, invalid, retry, conflict, cursor, terminal/JSON/prompt tests pass |
| Flix multi-resumption | supported | One continuation produces two immutable recipes consumed by the fixture controller |
| escaped/delayed Flix resumption | conditionally supported | Later invocation works under supplied handler scope; not portable or serializable |
| Flix CSP / associated families / Datalog | supported | Local compile/runtime tests pass, including actual `inject` |
| versioned Unix IPC | supported | Mode-`0600` socket, bounded frames, concurrent clients, cursor catch-up |
| bounded rendezvous | supported | Independent processes release; timeout/cancel/generation/disconnect tested |
| filesystem branch isolation | not exercised | Chosen live task was read-only; native conversation fork is explicitly not a workspace fork |
| systemd user unit | verified, not installed | Unit validates; transient restart used a new PID, restored IPC, and cleaned the socket on stop |

## Protocol authority

The 426-file bundle in `protocol/codex/0.154.0/schema/` was generated locally:

```sh
codex app-server generate-json-schema --experimental \
  --out protocol/codex/0.154.0/schema
```

`protocol/codex/0.154.0/SHA256SUMS` records each file. The sanitized handshake
is `protocol/codex/0.154.0/observed-handshake.json`; private raw and live model
traffic remains ignored under `.nema/`.

The installed app-server omits a generic JSON-RPC `jsonrpc` member in its
documented envelopes. Nema follows the generated contract and preserves source
bytes instead of forcing a generic library's envelope.
