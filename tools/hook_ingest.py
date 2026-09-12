#!/usr/bin/env python3
"""Drain atomically spooled Codex hook observations into the raw journal."""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import sys
import uuid
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.journal import ingest_external


def drain(database_path: Path, spool_path: Path) -> dict[str, Any]:
    accepted_path = spool_path.parent / "hook-ingested"
    accepted_path.mkdir(parents=True, exist_ok=True, mode=0o700)
    inserted = 0
    existing = 0
    for path in sorted(spool_path.glob("*.json")):
        claimed = path.with_name(f".{path.name}.{os.getpid()}-{uuid.uuid4().hex}.claimed")
        try:
            path.rename(claimed)
        except FileNotFoundError:
            continue
        try:
            params = json.loads(claimed.read_bytes())
            encoded = params.get("bytesBase64")
            available = (
                base64.b64decode(encoded, validate=True)
                if isinstance(encoded, str)
                else None
            )
            result = ingest_external(
                database_path,
                source=params["source"],
                source_id=params["sourceId"],
                observed_monotonic_ns=params["observedMonotonicNs"],
                available_bytes=available,
                original_byte_count=params["originalByteCount"],
                digest=params["sha256"],
                capture_status=params["captureStatus"],
                delayed_ingestion=True,
                spool_path=path.name,
            )
            if result["disposition"] == "inserted":
                inserted += 1
            else:
                existing += 1
            claimed.rename(accepted_path / path.name)
        except Exception:
            claimed.rename(path)
            raise
    return {"inserted": inserted, "existing": existing, "spool": str(spool_path)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--spool", type=Path, required=True)
    args = parser.parse_args()
    report = drain(args.db, args.spool)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
