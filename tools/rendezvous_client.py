#!/usr/bin/env python3
"""Independent process client for Nema's bounded rendezvous experiment."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
import uuid

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.ipc_client import call


def duration_ms(value: str) -> int:
    match = re.fullmatch(r"([0-9]+)(ms|s|m)?", value)
    if match is None:
        raise argparse.ArgumentTypeError("duration must look like 250ms, 5s, or 1m")
    amount = int(match.group(1))
    unit = match.group(2) or "s"
    multiplier = {"ms": 1, "s": 1000, "m": 60_000}[unit]
    milliseconds = amount * multiplier
    if not 1 <= milliseconds <= 300_000:
        raise argparse.ArgumentTypeError("duration must be between 1ms and 5m")
    return milliseconds


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("key")
    parser.add_argument("--generation", type=int, required=True)
    parser.add_argument("--payload", type=json.loads, required=True)
    parser.add_argument("--parties", type=int, default=2)
    parser.add_argument("--timeout", type=duration_ms, default=30_000)
    parser.add_argument(
        "--participant",
        default=f"process-{os.getpid()}-{uuid.uuid4().hex[:12]}",
    )
    args = parser.parse_args()
    result = call(
        args.socket,
        "rendezvous.join",
        {
            "key": args.key,
            "generation": args.generation,
            "participantId": args.participant,
            "payload": args.payload,
            "parties": args.parties,
            "timeoutMs": args.timeout,
        },
        timeout=args.timeout / 1000 + 5,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "released" else 3


if __name__ == "__main__":
    sys.exit(main())
