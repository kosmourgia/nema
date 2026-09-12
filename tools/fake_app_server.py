#!/usr/bin/env python3
"""Deterministic bidirectional fake for Nema's protocol probes."""

from __future__ import annotations

import json
import sys
from typing import Any


def emit(value: Any) -> None:
    sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main() -> int:
    for raw_line in sys.stdin:
        try:
            message = json.loads(raw_line)
        except json.JSONDecodeError:
            continue

        method = message.get("method")
        if method == "initialize":
            # These observations deliberately precede and surround the reply.
            # A membrane must retain them even though one is malformed and one
            # uses a future method/field it does not understand.
            sys.stdout.write("{malformed-frame\n")
            sys.stdout.flush()
            emit(
                {
                    "method": "future/notification",
                    "params": {"known": False, "futureField": {"nested": [1, 2, 3]}},
                }
            )
            emit(
                {
                    "id": message["id"],
                    "result": {
                        "userAgent": "nema-fake/1",
                        "platformFamily": "unix",
                        "platformOs": "linux",
                        "codexHome": "/fake/private/codex-home",
                        "futureInitializeField": "retained-in-raw-and-summary",
                    },
                }
            )
        elif method == "experimentalFeature/list":
            # Same apparent id value as the client's numeric initialize id,
            # but a server-origin string request. Direction and JSON type both
            # matter to correlation.
            emit(
                {
                    "id": "1",
                    "method": "future/serverRequest",
                    "params": {"wait": True, "unknownPayload": "preserve-me"},
                }
            )
            emit(
                {
                    "id": message["id"],
                    "result": {
                        "data": [
                            {
                                "name": "fake_feature",
                                "stage": "underDevelopment",
                                "enabled": True,
                                "defaultEnabled": False,
                            }
                        ],
                        "nextCursor": None,
                    },
                }
            )
        elif method == "initialized":
            emit({"method": "fake/initialized", "params": {"accepted": True}})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
