# Nema repository

This repository is the canonical Nema project and runs natively in the Arch
Linux ARM guest at `/home/elkeegano.guest/nema`.

- Use native guest commands. Do not route execution through a macOS/Lima shim.
- Read `NEMA_V0_HANDOFF.md` in full before substantial implementation work.
- Treat `PROGRESS.md` as the durable resume point and update it after every
  runnable vertical slice.
- Prefer observed Flix and Codex behavior over remembered APIs. Preserve
  generated schemas, minimal reproducers, and exact diagnostics for capability
  claims.
- Preserve raw operational observations separately from typed reductions and
  semantic projections. Unknown data must remain inspectable.
- Keep fork ancestry distinct from delegation ancestry and from workspace
  isolation.
- Keep runtime state under `.nema/`; do not commit private traces, credentials,
  live databases, sockets, or unsanitized model traffic.
- Avoid concurrent edits to the same file and preserve unrelated user work.

The current machine default Java is the intended runtime. The `flix` version is
pinned by `flix.toml`; do not introduce a second JDK merely because Java 21 is
the minimum supported version.
