# Codex hook capture

The project-local `.codex/hooks.json` sends lifecycle hook inputs to
`tools/hook_capture.py`. The helper never writes to stdout: hook observations
cannot accidentally become model instructions. It preserves the exact stdin
bytes and only derives an event-name label for indexing.

When `.nema/nema.sock` is available, the helper uses IPC method `hook.capture`.
If Nema is stopped, slow, or the frame would exceed the IPC limit, it writes an
atomic mode-`0600` envelope below `.nema/hook-spool/`. Run
`./bin/nema hooks drain` to ingest the backlog. Accepted spool files move to
`.nema/hook-ingested/` after the database commit. A 64 MiB spool ceiling turns
later observations into explicit hash-and-length gap records instead of
silently pretending their bytes were captured.

`./bin/nema hooks install` merges missing Nema handlers without removing
existing handlers or unknown configuration. `./bin/nema hooks inspect` reports
configured events, the spool backlog, and exact/gap counts.

Codex loads project hooks only after the `.codex` layer and the exact hook
definitions are trusted. Inspect and approve them with `/hooks`. Automated
probes may instead use `--dangerously-bypass-hook-trust` after reviewing the
checked-in configuration. Hooks from other active layers remain additive.

The configuration and trust behavior follow the
[official Codex hooks documentation](https://developers.openai.com/codex/hooks/).
