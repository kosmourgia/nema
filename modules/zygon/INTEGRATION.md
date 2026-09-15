# Integration seam

All source, fixtures, launcher, tests and documentation are owned by
`modules/zygon/**`. Root build/launcher/progress, fork bench, Vivarium and Aeron
are unchanged by this branch. The baseline contracts were read, not copied into
another Nema runtime. `PROTOCOL.md` documents this independent experimental owner.

An eventual transport can carry the opaque bytes exported by `zygon export`.
They follow `nema.lab.record/v1` with source-local decimal sequence and explicit
parent record IDs. The JSON schemas are in `schema/`. The source-native byte
capture and typed registration/invocation transitions remain distinct. A local
ingestion cursor is useful for catch-up, but is not a producer sequence or causal
clock. Consumers must not infer parents from arrival order.

The inspector can consume registrations (`id`, `incarnation`, `parent`, `kind`,
`medium`, `mode`, supported operations, native binding and optional unknown
metadata) and accepted/started/terminal invocations. Use the precise target
incarnation for actions. Presence unknown does not end an object. Unknown fields
and kinds remain inspectable in the retained record/byte stores.

Parent links here describe host/container relationships. They imply neither
conversation-fork ancestry, delegation ancestry, nor filesystem isolation.
Flix Datalog reads a finite observed snapshot and derives reachability/pending
relations; it never starts processes or executes page commands.

Local Unix peers are trusted same-UID participants with owner checks and
per-identity resume tokens. Browser attachment uses bearer-authenticated loopback
HTTP and an explicitly configured local demo origin; no remote eval or shell.
This protocol owner is not proposed as the authority for all Nema objects.

Runtime state, tokens, raw wire traffic, Chromium profiles, logs and databases
stay in ignored `.nema/` directories. Do not publish arbitrary exports without
review: ordinary process output may be private. Checked-in observed fixtures are
selected from controlled demo traffic and have an explicit sanitization note.
