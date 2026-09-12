# Codex app-server protocol 0.154.0

`schema/` is the unmodified experimental JSON Schema bundle generated from the
installed `codex-cli 0.154.0` binary on 12 September 2026:

```sh
codex app-server generate-json-schema --experimental --out schema
```

The bundle is retained as protocol evidence. Nema must still preserve unknown
fields and variants because a future or differently configured server can emit
data that this typed snapshot does not understand.

`SHA256SUMS` contains one digest for each of the 426 schema files. The
sanitized `observed-handshake.json` records a successful real initialization
and experimental feature-list request. Its machine-specific Codex home was
replaced with an explicit redaction marker; therefore the report is evidence,
not a byte-identical raw capture. Raw frames remain ignored under `.nema/`.
