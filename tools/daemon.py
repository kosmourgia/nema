#!/usr/bin/env python3
"""Nema's small versioned Unix-domain JSONL protocol."""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import select
import signal
import socket
import socketserver
import stat
import sys
import time
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.state import (
    StateError,
    artifact_from_row,
    changes_after,
    experiment_from_row,
    open_database,
    respond,
    snapshot,
)
from tools.journal import ingest_external
from tools.rendezvous import arrive as rendezvous_arrive
from tools.rendezvous import cancel as rendezvous_cancel
from tools.rendezvous import inspect as rendezvous_inspect


PROTOCOL_VERSION = 1
MAX_LINE_BYTES = 1024 * 1024


class NemaUnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    block_on_close = True

    def __init__(self, socket_path: Path, database_path: Path) -> None:
        self.socket_path = socket_path
        self.database_path = database_path
        super().__init__(str(socket_path), NemaRequestHandler)


def error_response(identifier: Any, code: str, message: str) -> dict[str, Any]:
    return {
        "version": PROTOCOL_VERSION,
        "id": identifier,
        "error": {"code": code, "message": message},
    }


def dispatch(database_path: Path, method: str, params: dict[str, Any]) -> Any:
    if method == "health":
        return {"status": "ok", "protocolVersion": PROTOCOL_VERSION}
    if method == "surface.get":
        return snapshot(database_path)
    if method == "events.list":
        after = params.get("afterRevision")
        if not isinstance(after, int):
            raise StateError("events.list requires integer afterRevision")
        return changes_after(database_path, after)
    if method == "rendezvous.inspect":
        key = params.get("key")
        generation = params.get("generation")
        if not isinstance(key, str) or not isinstance(generation, int):
            raise StateError("rendezvous.inspect requires key and integer generation")
        return rendezvous_inspect(database_path, key, generation)
    if method == "rendezvous.cancel":
        key = params.get("key")
        generation = params.get("generation")
        participant_id = params.get("participantId", "ipc-client")
        reason = params.get("reason", "explicit-cancellation")
        if not isinstance(key, str) or not isinstance(generation, int):
            raise StateError("rendezvous.cancel requires key and integer generation")
        if not isinstance(participant_id, str) or not isinstance(reason, str):
            raise StateError("rendezvous.cancel participantId/reason must be strings")
        return rendezvous_cancel(
            database_path,
            key,
            generation,
            participant_id=participant_id,
            reason=reason,
        )
    if method == "hook.capture":
        source_id = params.get("sourceId")
        source = params.get("source", "codex-hook")
        observed = params.get("observedMonotonicNs")
        byte_count = params.get("originalByteCount")
        digest = params.get("sha256")
        status = params.get("captureStatus")
        encoded = params.get("bytesBase64")
        if not isinstance(source_id, str) or not isinstance(source, str):
            raise StateError("hook.capture requires sourceId and source")
        if not isinstance(observed, int) or not isinstance(byte_count, int):
            raise StateError("hook.capture requires integer observation time and byte count")
        if not isinstance(digest, str) or status not in ("exact", "gap"):
            raise StateError("hook.capture requires sha256 and exact/gap captureStatus")
        available_bytes = None
        if encoded is not None:
            if not isinstance(encoded, str):
                raise StateError("hook.capture bytesBase64 must be a string")
            try:
                available_bytes = base64.b64decode(encoded, validate=True)
            except ValueError as error:
                raise StateError(f"hook.capture has invalid base64: {error}") from error
        try:
            return ingest_external(
                database_path,
                source=source,
                source_id=source_id,
                observed_monotonic_ns=observed,
                available_bytes=available_bytes,
                original_byte_count=byte_count,
                digest=digest,
                capture_status=status,
                delayed_ingestion=bool(params.get("delayedIngestion", False)),
                spool_path=(
                    params.get("spoolPath")
                    if isinstance(params.get("spoolPath"), str)
                    else None
                ),
            )
        except ValueError as error:
            raise StateError(str(error)) from error
    if method == "interaction.respond":
        interaction_id = params.get("interactionId")
        revision = params.get("relevantInputRevision")
        answer = params.get("answer")
        responder = params.get("responder", "ipc-client")
        if not isinstance(interaction_id, str) or not isinstance(revision, int):
            raise StateError("interaction.respond requires interactionId and integer revision")
        if not isinstance(answer, str) or not isinstance(responder, str):
            raise StateError("interaction.respond answer/responder must be strings")
        return respond(
            database_path,
            interaction_id,
            revision,
            answer,
            responder=responder,
        )
    if method == "artifact.get":
        artifact_id = params.get("artifactId")
        if not isinstance(artifact_id, str):
            raise StateError("artifact.get requires artifactId")
        with open_database(database_path) as database:
            row = database.execute(
                "SELECT * FROM artifacts WHERE artifact_id = ?", (artifact_id,)
            ).fetchone()
            if row is None:
                raise StateError(f"unknown artifact {artifact_id!r}")
            return artifact_from_row(row, include_content=True)
    if method == "fork.inspect":
        experiment_id = params.get("experimentId")
        if not isinstance(experiment_id, str):
            raise StateError("fork.inspect requires experimentId")
        with open_database(database_path) as database:
            row = database.execute(
                "SELECT * FROM fork_experiments WHERE experiment_id = ?", (experiment_id,)
            ).fetchone()
            if row is None:
                raise StateError(f"unknown fork experiment {experiment_id!r}")
            return experiment_from_row(database, row)
    raise StateError(f"unknown method {method!r}")


def peer_disconnected(peer: socket.socket) -> bool:
    readable, _, _ = select.select([peer], [], [], 0)
    if not readable:
        return False
    try:
        return peer.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT) == b""
    except BlockingIOError:
        return False


def rendezvous_join(
    database_path: Path, params: dict[str, Any], peer: socket.socket
) -> dict[str, Any]:
    key = params.get("key")
    generation = params.get("generation")
    participant_id = params.get("participantId")
    parties = params.get("parties")
    timeout_ms = params.get("timeoutMs")
    if not isinstance(key, str) or not isinstance(participant_id, str):
        raise StateError("rendezvous.join requires key and participantId")
    if not isinstance(generation, int) or not isinstance(parties, int):
        raise StateError("rendezvous.join requires integer generation and parties")
    if not isinstance(timeout_ms, int) or not 1 <= timeout_ms <= 300_000:
        raise StateError("rendezvous.join timeoutMs must be between 1 and 300000")
    state = rendezvous_arrive(
        database_path,
        key,
        generation,
        participant_id,
        params.get("payload"),
        parties,
    )
    deadline = time.monotonic() + timeout_ms / 1000
    while state["status"] == "waiting":
        if peer_disconnected(peer):
            return rendezvous_cancel(
                database_path,
                key,
                generation,
                participant_id=participant_id,
                reason=f"client-disconnected:{participant_id}",
            )
        if time.monotonic() >= deadline:
            state = rendezvous_cancel(
                database_path,
                key,
                generation,
                participant_id=participant_id,
                reason=f"timeout:{participant_id}",
            )
            break
        time.sleep(0.02)
        state = rendezvous_inspect(database_path, key, generation)
    state["peerMessages"] = [
        arrival
        for arrival in state["arrivals"]
        if arrival["participantId"] != participant_id
    ]
    return state


class NemaRequestHandler(socketserver.StreamRequestHandler):
    server: NemaUnixServer

    def handle(self) -> None:
        while True:
            line = self.rfile.readline(MAX_LINE_BYTES + 1)
            if not line:
                return
            if len(line) > MAX_LINE_BYTES:
                response = error_response(None, "request-too-large", "request exceeds 1 MiB")
                self.wfile.write(json.dumps(response, separators=(",", ":")).encode() + b"\n")
                self.wfile.flush()
                return
            identifier: Any = None
            try:
                request = json.loads(line)
                if not isinstance(request, dict):
                    raise StateError("request must be a JSON object")
                identifier = request.get("id")
                if request.get("version") != PROTOCOL_VERSION:
                    raise StateError(f"unsupported protocol version {request.get('version')!r}")
                method = request.get("method")
                params = request.get("params", {})
                if not isinstance(method, str) or not isinstance(params, dict):
                    raise StateError("request requires string method and object params")
                result = (
                    rendezvous_join(self.server.database_path, params, self.request)
                    if method == "rendezvous.join"
                    else dispatch(self.server.database_path, method, params)
                )
                response = {
                    "version": PROTOCOL_VERSION,
                    "id": identifier,
                    "result": result,
                }
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                response = error_response(identifier, "invalid-json", str(error))
            except StateError as error:
                response = error_response(identifier, "invalid-request", str(error))
            except Exception as error:
                response = error_response(
                    identifier,
                    "internal-error",
                    f"{type(error).__name__}: {error}",
                )
            try:
                self.wfile.write(
                    json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    + b"\n"
                )
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return


def prepare_socket(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_socket():
        mode = path.lstat().st_mode
        if not stat.S_ISSOCK(mode):
            raise RuntimeError(f"refusing to replace non-socket path: {path}")
        path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    args = parser.parse_args()
    prepare_socket(args.socket)
    server = NemaUnixServer(args.socket, args.db)
    os.chmod(args.socket, 0o600)

    def stop(_signum: int, _frame: Any) -> None:
        # shutdown must run outside the serving loop's current call stack.
        import threading

        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()
        if args.socket.exists() and args.socket.is_socket():
            args.socket.unlink()
    return 0


if __name__ == "__main__":
    sys.exit(main())
