#!/usr/bin/env python3
"""Reusable, journaled stdio session for Codex app-server.

The routing thread is the only child-stdin writer and SQLite raw-journal owner.
Server-origin request handlers run in bounded worker threads and return their
dispositions through a queue, so a waiting interaction does not stop framing,
notifications, responses, or lifecycle observation.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import queue
import selectors
import sqlite3
import subprocess
import threading
import time
from typing import Any, Callable
import uuid

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.journal import ingest_envelope, open_database


Message = dict[str, Any]
ServerRequestHandler = Callable[[Message], Message]


class SessionError(RuntimeError):
    pass


def compact_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"


def id_key(value: Any) -> tuple[str, str]:
    if isinstance(value, bool):
        return ("boolean", "true" if value else "false")
    if isinstance(value, int):
        return ("integer", str(value))
    if isinstance(value, str):
        return ("string", value)
    if value is None:
        return ("null", "null")
    return ("other", json.dumps(value, ensure_ascii=False, sort_keys=True))


class DurableCapture:
    def __init__(
        self,
        database_path: Path,
        capture_path: Path,
        connection_id: str,
        connection_epoch: int,
    ) -> None:
        self.database: sqlite3.Connection = open_database(database_path)
        self.capture_path = capture_path
        self.capture_path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.capture_path.open("w", encoding="utf-8")
        self.connection_id = connection_id
        self.connection_epoch = connection_epoch
        self.local_sequence = 0

    def observe(self, direction: str, stream: str, payload: bytes) -> dict[str, Any]:
        self.local_sequence += 1
        envelope = {
            "connectionId": self.connection_id,
            "connectionEpoch": self.connection_epoch,
            "localSequence": self.local_sequence,
            "direction": direction,
            "stream": stream,
            "observedMonotonicNs": time.monotonic_ns(),
            "bytesBase64": base64.b64encode(payload).decode("ascii"),
            "utf8": payload.decode("utf-8", errors="replace"),
        }
        serialized = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
        self.handle.write(serialized + "\n")
        self.handle.flush()
        try:
            ingest_envelope(self.database, envelope, str(self.capture_path))
            self.database.commit()
        except Exception:
            self.database.rollback()
            raise
        return envelope

    def close(self) -> None:
        self.handle.close()
        self.database.close()


class AppServerSession:
    def __init__(
        self,
        database_path: Path,
        capture_path: Path,
        *,
        command: list[str] | None = None,
        connection_id: str | None = None,
        connection_epoch: int = 1,
        server_request_handler: ServerRequestHandler | None = None,
        max_request_workers: int = 8,
    ) -> None:
        self.command = command or ["codex", "app-server", "--stdio"]
        self.connection_id = connection_id or f"session-{uuid.uuid4()}"
        self.connection_epoch = connection_epoch
        self.capture = DurableCapture(
            database_path, capture_path, self.connection_id, self.connection_epoch
        )
        self.process = subprocess.Popen(
            self.command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        assert self.process.stderr is not None
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ, "stdout")
        self.selector.register(self.process.stderr, selectors.EVENT_READ, "stderr")
        self.buffers = {"stdout": bytearray(), "stderr": bytearray()}
        self.responses: dict[tuple[str, str], Message] = {}
        self.notifications: list[Message] = []
        self.decoded_messages: list[Message] = []
        self.request_counter = 0
        self.request_handler = server_request_handler or self._unsupported_request
        self.worker_slots = threading.BoundedSemaphore(max_request_workers)
        self.worker_results: queue.SimpleQueue[Message] = queue.SimpleQueue()
        self.workers: list[threading.Thread] = []
        self.pending_workers = 0
        self.server_request_count = 0
        self.closed = False

    @staticmethod
    def _unsupported_request(message: Message) -> Message:
        return {
            "error": {
                "code": -32601,
                "message": "unsupported by this Nema session",
                "data": {"method": message.get("method")},
            }
        }

    def send(self, message: Message) -> None:
        if self.closed:
            raise SessionError("cannot write to a closed app-server session")
        payload = compact_json(message)
        self.capture.observe("nema-to-server", "stdin", payload)
        try:
            self.process.stdin.write(payload)
            self.process.stdin.flush()
        except BrokenPipeError as error:
            raise SessionError("app-server closed stdin before the frame was written") from error

    def notify(self, method: str, params: Any | None = None) -> None:
        message: Message = {"method": method}
        if params is not None:
            message["params"] = params
        self.send(message)

    def request(
        self,
        method: str,
        params: Any,
        *,
        timeout: float = 30.0,
        identifier: Any | None = None,
    ) -> Any:
        if identifier is None:
            self.request_counter += 1
            identifier = f"nema-{self.request_counter}"
        key = id_key(identifier)
        self.send({"id": identifier, "method": method, "params": params})
        deadline = time.monotonic() + timeout
        while key not in self.responses:
            if time.monotonic() >= deadline:
                raise SessionError(f"timed out waiting for {method} ({identifier})")
            self.pump_once(min(0.1, max(0.0, deadline - time.monotonic())))
            if self.process.poll() is not None and key not in self.responses:
                raise SessionError(
                    f"app-server exited {self.process.returncode} while waiting for {method}"
                )
        response = self.responses.pop(key)
        if "error" in response:
            raise SessionError(f"{method} failed: {response['error']}")
        if "result" not in response:
            raise SessionError(f"{method} response has neither result nor error")
        return response["result"]

    def initialize(self, *, timeout: float = 15.0) -> Message:
        result = self.request(
            "initialize",
            {
                "clientInfo": {"name": "nema", "version": "0.0.1"},
                "capabilities": {"experimentalApi": True},
            },
            timeout=timeout,
            identifier=1,
        )
        if not isinstance(result, dict):
            raise SessionError("initialize result is not an object")
        self.notify("initialized")
        return result

    def _start_server_request(self, message: Message) -> None:
        self.server_request_count += 1
        if not self.worker_slots.acquire(blocking=False):
            self.worker_results.put(
                {
                    "id": message["id"],
                    "error": {
                        "code": -32000,
                        "message": "Nema server-request worker limit reached",
                    },
                }
            )
            return

        self.pending_workers += 1

        def run_handler() -> None:
            try:
                disposition = self.request_handler(message)
                if not isinstance(disposition, dict) or not (
                    "result" in disposition or "error" in disposition
                ):
                    raise SessionError("server request handler returned no result/error disposition")
                self.worker_results.put({"id": message["id"], **disposition})
            except Exception as error:  # retain an explicit local failure disposition
                self.worker_results.put(
                    {
                        "id": message["id"],
                        "error": {
                            "code": -32603,
                            "message": "Nema server request handler failed",
                            "data": {"type": type(error).__name__, "message": str(error)},
                        },
                    }
                )
            finally:
                self.worker_slots.release()

        worker = threading.Thread(target=run_handler, name="nema-server-request", daemon=True)
        self.workers.append(worker)
        worker.start()

    def _write_worker_results(self) -> None:
        while True:
            try:
                disposition = self.worker_results.get_nowait()
            except queue.Empty:
                return
            self.send(disposition)
            self.pending_workers -= 1

    def _accept_payload(self, stream: str, payload: bytes) -> None:
        self.capture.observe("server-to-nema", stream, payload)
        if stream != "stdout":
            return
        try:
            message = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return
        if not isinstance(message, dict):
            return
        self.decoded_messages.append(message)
        if "method" in message and "id" in message:
            self._start_server_request(message)
        elif "method" in message:
            self.notifications.append(message)
        elif "id" in message and ("result" in message or "error" in message):
            self.responses[id_key(message["id"])] = message

    def pump_once(self, timeout: float = 0.1) -> None:
        self._write_worker_results()
        for key, _ in self.selector.select(timeout):
            stream = key.data
            chunk = os.read(key.fileobj.fileno(), 65536)
            if not chunk:
                self.selector.unregister(key.fileobj)
                continue
            self.buffers[stream].extend(chunk)
            while b"\n" in self.buffers[stream]:
                line, _, rest = self.buffers[stream].partition(b"\n")
                self.buffers[stream] = bytearray(rest)
                self._accept_payload(stream, bytes(line) + b"\n")

    def wait_for_notification(
        self,
        method: str,
        *,
        predicate: Callable[[Message], bool] | None = None,
        timeout: float = 120.0,
    ) -> Message:
        predicate = predicate or (lambda _: True)
        checked = 0
        deadline = time.monotonic() + timeout
        while True:
            while checked < len(self.notifications):
                message = self.notifications[checked]
                checked += 1
                if message.get("method") == method and predicate(message):
                    return message
            if time.monotonic() >= deadline:
                raise SessionError(f"timed out waiting for notification {method}")
            self.pump_once(min(0.1, max(0.0, deadline - time.monotonic())))
            if self.process.poll() is not None:
                raise SessionError(
                    f"app-server exited {self.process.returncode} while waiting for {method}"
                )

    def drain_workers(self, *, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while self.pending_workers > 0:
            if time.monotonic() >= deadline:
                raise SessionError(
                    f"timed out with {self.pending_workers} server requests still pending"
                )
            self.pump_once(min(0.1, max(0.0, deadline - time.monotonic())))

    def close(self) -> int:
        if self.closed:
            return self.process.returncode if self.process.returncode is not None else -1
        try:
            self.drain_workers(timeout=2.0)
        except SessionError:
            pass
        try:
            self.process.stdin.close()
        except BrokenPipeError:
            pass
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and (
            self.process.poll() is None or bool(self.selector.get_map())
        ):
            self.pump_once(0.05)
            if self.process.poll() is not None and not self.selector.get_map():
                break
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2.0)
        for worker in self.workers:
            worker.join(timeout=0.1)
        for stream, remainder in self.buffers.items():
            if remainder:
                self._accept_payload(stream, bytes(remainder))
        self.selector.close()
        self.process.stdout.close()
        self.process.stderr.close()
        self.closed = True
        self.capture.close()
        return int(self.process.returncode)

    def __enter__(self) -> "AppServerSession":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
