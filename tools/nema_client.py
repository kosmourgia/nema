#!/usr/bin/env python3
"""Small non-JVM client for Nema's durable interaction boundary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.state import respond, snapshot


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--interaction", required=True)
    parser.add_argument("--answer", required=True)
    parser.add_argument("--responder", default="human-client")
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        surface = snapshot(args.db)
        interaction = next(
            (
                item
                for item in surface["interactions"]
                if item["id"] == args.interaction
            ),
            None,
        )
        if interaction is None:
            time.sleep(0.05)
            continue
        receipt = respond(
            args.db,
            args.interaction,
            interaction["relevantInputRevision"],
            args.answer,
            responder=args.responder,
        )
        print(json.dumps(receipt, indent=2, sort_keys=True))
        return 0 if receipt["disposition"] in ("accepted", "identical-retry") else 2

    print(
        json.dumps(
            {
                "error": "timed out waiting for interaction",
                "interactionId": args.interaction,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
