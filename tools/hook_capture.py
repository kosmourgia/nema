#!/usr/bin/env python3
"""Loss-aware, stdout-silent ingress for Codex command hooks."""

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.ipc_client import call


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOCKET = ROOT / ".nema/nema.sock"
DEFAULT_SPOOL = ROOT / ".nema/hook-spool"
MAX_DIRECT_BYTES = 512 * 1024
MAX_SPOOL_BYTES = 64 * 1024 * 1024


def event_source(payload: bytes) -> str:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "codex-hook:unknown"
    if not isinstance(value, dict):
        return "codex-hook:unknown"
    name = value.get("hook_event_name")
    return f"codex-hook:{name}" if isinstance(name, str) else "codex-hook:unknown"


def capture_params(payload: bytes) -> dict[str, Any]:
    observed = time.monotonic_ns()
    digest = hashlib.sha256(payload).hexdigest()
    return {
        "source": event_source(payload),
        "sourceId": f"hook-{observed}-{os.getpid()}-{uuid.uuid4().hex}",
        "observedMonotonicNs": observed,
        "bytesBase64": base64.b64encode(payload).decode("ascii"),
        "captureStatus": "exact",
        "originalByteCount": len(payload),
        "sha256": digest,
        "delayedIngestion": False,
    }


def atomic_spool(directory: Path, params: dict[str, Any]) -> Path:
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    lock_path = directory / ".lock"
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        params["delayedIngestion"] = True
        exact = json.dumps(params, ensure_ascii=False, separators=(",", ":")).encode()
        used = sum(
            path.stat().st_size
            for path in directory.glob("*.json")
            if path.is_file()
        )
        envelope = exact
        if used + len(exact) > MAX_SPOOL_BYTES:
            gap = dict(params)
            gap.pop("bytesBase64", None)
            gap["captureStatus"] = "gap"
            gap["delayedIngestion"] = True
            envelope = json.dumps(gap, ensure_ascii=False, separators=(",", ":")).encode()
        name = f"{params['observedMonotonicNs']}-{params['sourceId']}.json"
        final_path = directory / name
        temporary_path = directory / f".{name}.{uuid.uuid4().hex}.tmp"
        descriptor = os.open(temporary_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(envelope)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary_path, final_path)
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary_path.exists():
                temporary_path.unlink()
        return final_path


def main() -> int:
    payload = sys.stdin.buffer.read()
    params = capture_params(payload)
    socket_path = Path(os.environ.get("NEMA_SOCKET", DEFAULT_SOCKET))
    spool_path = Path(os.environ.get("NEMA_HOOK_SPOOL", DEFAULT_SPOOL))

    if len(payload) <= MAX_DIRECT_BYTES:
        try:
            call(socket_path, "hook.capture", params, timeout=0.2)
            return 0
        except (OSError, RuntimeError, TimeoutError):
            pass

    try:
        atomic_spool(spool_path, params)
    except Exception:
        # Hook stdout is semantic output to Codex, so failures are represented
        # by the exit status alone rather than contaminating model context.
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
