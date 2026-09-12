#!/usr/bin/env python3
"""Reference client for Nema's Unix-domain JSONL protocol."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import socket
import sys
from typing import Any
import uuid


def call(
    socket_path: Path,
    method: str,
    params: dict[str, Any],
    *,
    timeout: float = 10.0,
) -> Any:
    identifier = f"client-{uuid.uuid4()}"
    request = {
        "version": 1,
        "id": identifier,
        "method": method,
        "params": params,
    }
    payload = json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(socket_path))
        client.sendall(payload)
        response_bytes = bytearray()
        while b"\n" not in response_bytes:
            chunk = client.recv(65536)
            if not chunk:
                raise RuntimeError("daemon closed before sending a response")
            response_bytes.extend(chunk)
    response = json.loads(bytes(response_bytes).split(b"\n", 1)[0])
    if response.get("id") != identifier:
        raise RuntimeError("daemon returned a mismatched response id")
    if "error" in response:
        raise RuntimeError(f"{response['error']['code']}: {response['error']['message']}")
    return response["result"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("method")
    parser.add_argument("params_json", nargs="?", default="{}")
    args = parser.parse_args()
    params = json.loads(args.params_json)
    if not isinstance(params, dict):
        parser.error("params_json must decode to an object")
    result = call(args.socket, args.method, params)
    print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
