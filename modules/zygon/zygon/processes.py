"""Owned Linux process groups, raw durable capture, and cooperative control.

No fork-runtime dependency. Capture cursor is ingestion order; streamSeq is
order within one actual OS stream. A PTY supplies one combined output stream.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import ctypes
import errno
import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import struct
import sys
import termios
import time
from typing import Any
import uuid

MAX_FRAME = 65536
TERMINAL = {"completed", "failed", "cancelled", "outcome-unknown"}


def compact(value: Any) -> bytes:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
    if len(payload) > MAX_FRAME:
        raise ValueError("cooperative frame exceeds 65536 bytes")
    return payload


def private_directory(path: Path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def save_private(path: Path, value: Any) -> None:
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(compact(value))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class CaptureFull(RuntimeError):
    pass


class RawSpool:
    """FULL synchronous SQLite, bounded encoded bytes and row count.

    Readers page disk without an observer queue. One reserved small failure row
    records the first unavailable read and marks all later bytes unknown.
    """

    def __init__(self, path: Path, source: str, incarnation: str,
                 max_bytes: int = 64 * 1024 * 1024, max_records: int = 100000):
        private_directory(path.parent)
        self.path, self.source, self.incarnation = path, source, incarnation
        self.max_bytes, self.max_records = max_bytes, max_records
        self.database = sqlite3.connect(path)
        path.chmod(0o600)
        self.database.execute("PRAGMA journal_mode=DELETE")
        self.database.execute("PRAGMA synchronous=FULL")
        self.database.execute("CREATE TABLE IF NOT EXISTS records (cursor INTEGER PRIMARY KEY, record TEXT NOT NULL)")
        self.database.commit()
        self.sequence = 0
        self.stream_sequences: dict[str, int] = {}
        self.bytes_used, self.record_count = self.database.execute(
            "SELECT coalesce(sum(length(CAST(record AS BLOB))),0),count(*) FROM records").fetchone()
        self.failure = None
        self.closed = False

    def append(self, kind: str, body: dict, parents: list[str] | None = None) -> dict:
        if self.failure is not None:
            raise CaptureFull(self.failure["body"]["reason"])
        self.sequence += 1
        record = {"schema": "nema.lab.record/v1", "id": "capture-" + uuid.uuid4().hex,
                  "source": self.source, "incarnation": self.incarnation, "seq": str(self.sequence),
                  "kind": kind, "parents": parents or [], "body": body}
        encoded = compact(record)
        if self.bytes_used + len(encoded) > self.max_bytes or self.record_count >= self.max_records:
            self.sequence -= 1
            self.fail("capture spool capacity exceeded", body.get("stream"),
                      len(base64.b64decode(body.get("bytesBase64", ""))))
            raise CaptureFull("capture spool capacity exceeded")
        self.database.execute("INSERT INTO records(record) VALUES (?)", (encoded.decode(),))
        self.database.commit()
        self.bytes_used += len(encoded)
        self.record_count += 1
        return record

    def fail(self, reason: str, stream=None, unavailable_bytes=None):
        if self.failure is not None:
            return self.failure
        self.sequence += 1
        self.failure = {"schema": "nema.lab.record/v1", "id": "capture-failure-" + uuid.uuid4().hex,
                        "source": self.source, "incarnation": self.incarnation, "seq": str(self.sequence),
                        "kind": "capture.failure", "parents": [], "body": {
                            "reason": reason, "stream": stream, "unavailableBytesInRead": unavailable_bytes,
                            "laterBytes": "unknown", "action": "stop-owned-process",
                            "monotonicNs": str(time.monotonic_ns())}}
        self.database.execute("INSERT INTO records(record) VALUES (?)", (compact(self.failure).decode(),))
        self.database.commit()
        return self.failure

    def chunk(self, stream: str, payload: bytes, parents=None):
        self.stream_sequences[stream] = self.stream_sequences.get(stream, 0) + 1
        return self.append("capture.chunk", {"stream": stream,
            "streamSeq": str(self.stream_sequences[stream]), "bytesBase64": base64.b64encode(payload).decode(),
            "monotonicNs": str(time.monotonic_ns())}, parents)

    def page(self, cursor: int = 0, limit: int = 64):
        return [(row[0], json.loads(row[1])) for row in self.database.execute(
            "SELECT cursor,record FROM records WHERE cursor>? ORDER BY cursor LIMIT ?", (cursor, limit))]

    def raw_bytes(self, stream: str) -> bytes:
        parts = []
        for (encoded,) in self.database.execute("SELECT record FROM records ORDER BY cursor"):
            record = json.loads(encoded)
            if record["kind"] == "capture.chunk" and record["body"]["stream"] == stream:
                parts.append(base64.b64decode(record["body"]["bytesBase64"]))
        return b"".join(parts)

    def close(self):
        if not self.closed:
            self.closed = True
            self.database.close()


class CooperativeChannel:
    """Native response waiting is independent of unsolicited observations."""

    def __init__(self, reader, writer, observe=None):
        self.reader, self.writer, self.observe = reader, writer, observe
        self.pending = {}
        self.write_lock = asyncio.Lock()
        self.task = asyncio.create_task(self._read())
        self.closed = False

    async def send(self, value):
        payload = compact(value)
        async with self.write_lock:
            if self.observe:
                self.observe("control.out", payload)
            self.writer.write(payload)
            await asyncio.wait_for(self.writer.drain(), 2)

    async def request(self, method, arguments=None, *, correlation=None, timeout=30):
        identifier = correlation or uuid.uuid4().hex
        if identifier in self.pending:
            raise ValueError("duplicate pending native correlation")
        waiter = asyncio.get_running_loop().create_future()
        self.pending[identifier] = waiter
        try:
            await self.send({"version": 1, "id": identifier, "method": method, "arguments": arguments or {}})
            return await asyncio.wait_for(asyncio.shield(waiter), timeout)
        finally:
            self.pending.pop(identifier, None)
            if not waiter.done():
                waiter.cancel()

    async def cancel(self, correlation):
        await self.send({"version": 1, "method": "cancel", "arguments": {"correlation": correlation}})

    async def _read(self):
        try:
            while True:
                line = await self.reader.readline()
                if not line:
                    raise ConnectionError("cooperative endpoint EOF; pending outcome unknown")
                if len(line) > MAX_FRAME or not line.endswith(b"\n"):
                    raise ValueError("invalid/oversized cooperative frame")
                if self.observe:
                    self.observe("control.in", line)
                value = json.loads(line)
                if not isinstance(value, dict) or value.get("version") != 1:
                    raise ValueError("invalid cooperative envelope")
                waiter = self.pending.get(value.get("id"))
                if waiter is not None and not waiter.done() and value.get("status") in TERMINAL:
                    waiter.set_result(value)
                # Duplicates and unknown observations remain in raw capture.
        except (Exception, asyncio.CancelledError) as error:
            for waiter in list(self.pending.values()):
                if not waiter.done():
                    waiter.set_exception(ConnectionError(str(error) or "cooperative channel closed"))

    async def close(self):
        if self.closed:
            return
        self.closed = True
        self.writer.close()
        with contextlib.suppress(Exception):
            await self.writer.wait_closed()
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)


async def read_fd(fd: int, size=16384) -> bytes:
    loop = asyncio.get_running_loop()
    while True:
        try:
            return os.read(fd, size)
        except BlockingIOError:
            waiter = loop.create_future()
            loop.add_reader(fd, lambda: not waiter.done() and waiter.set_result(None))
            try:
                await waiter
            finally:
                loop.remove_reader(fd)
        except OSError as error:
            if error.errno == errno.EIO:  # Linux PTY slave closed.
                return b""
            raise


async def write_fd(fd: int, payload: bytes) -> int:
    loop = asyncio.get_running_loop()
    written = 0
    while written < len(payload):
        try:
            written += os.write(fd, payload[written:])
        except BlockingIOError:
            waiter = loop.create_future()
            loop.add_writer(fd, lambda: not waiter.done() and waiter.set_result(None))
            try:
                await waiter
            finally:
                loop.remove_writer(fd)
    return written


class OwnedProcess:
    @classmethod
    async def launch(cls, client, runtime_dir: Path, command=None, *, participant_id="process-demo",
                     mode="pipe", cooperative=None, max_capture_bytes=64 * 1024 * 1024):
        self = cls()
        self.client, self.runtime_dir = client, private_directory(Path(runtime_dir))
        self.participant_id, self.mode = participant_id, mode
        if mode not in {"pipe", "pty"}:
            raise ValueError("mode must be pipe or pty")
        self.incarnation = "process-" + uuid.uuid4().hex
        self.ref = {"id": participant_id, "incarnation": self.incarnation}
        self.spool = RawSpool(self.runtime_dir / (self.incarnation + ".sqlite"), participant_id,
                              self.incarnation, max_capture_bytes)
        self.state_path = self.runtime_dir / "registration.json"
        self.tokens = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        self.operations, self.read_tasks = {}, []
        self.native = self.master = self.process = self.lifecycle_task = self.publisher = None
        self.stopping = self.closed = False
        self.finished = asyncio.Event()
        self.stdin_lock = asyncio.Lock()
        self.publish_cursor, self.publish_error = 0, None
        self.cooperative = (command is None) if cooperative is None else cooperative
        self.command = command or [sys.executable, "-m", "zygon.demo_child"]
        controls = ["process.stdin.write", "process.stop", "process.interrupt"]
        controls += ["process.stdin.eof"] if mode == "pipe" else ["process.pty.resize", "process.pty.eof"]
        self.registration = {**self.ref, "kind": "host", "medium": mode, "mode": "owned",
            "operations": controls, "parent": None,
            "metadata": {"command": self.command, "terminalEmulatorTab": None},
            "binding": {"pid": None, "processGroup": None}}
        self.surfaces = [
            {"id": participant_id + "/requests", "incarnation": self.incarnation + "/requests",
             "kind": "surface", "medium": "cooperative-socket" if self.cooperative else mode,
             "mode": "owned", "operations": ["demo.echo", "demo.delay", "demo.read"] if self.cooperative else [],
             "parent": self.ref},
            {"id": participant_id + "/output", "incarnation": self.incarnation + "/output",
             "kind": "surface", "medium": mode, "mode": "owned", "operations": [], "parent": self.ref,
             "metadata": {"streams": ["stdout", "stderr"] if mode == "pipe" else ["pty"]}}]
        self.client.on_invoke, self.client.on_cancel = self._invoke, self._cancel
        await self._register()
        self.publisher = asyncio.create_task(self._publish())
        parent_socket = child_socket = slave = None
        try:
            if self.cooperative:
                parent_socket, child_socket = socket.socketpair()
                self.command = [*self.command, "--control-fd", str(child_socket.fileno())]
            parent_pid = os.getpid()
            libc = ctypes.CDLL(None, use_errno=True)

            def prepare_child():
                # Linux parent-death signal covers the direct child on abrupt
                # companion death; systemd's cgroup covers escaped descendants.
                if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
                    raise OSError(ctypes.get_errno(), "PR_SET_PDEATHSIG failed")
                if os.getppid() != parent_pid:
                    os._exit(125)
                if mode == "pty":
                    os.setsid()
                    fcntl.ioctl(0, termios.TIOCSCTTY, 0)

            kwargs = {"pass_fds": (child_socket.fileno(),) if child_socket else (), "preexec_fn": prepare_child}
            if mode == "pipe":
                kwargs.update(stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                              stderr=asyncio.subprocess.PIPE, start_new_session=True)
            else:
                self.master, slave = os.openpty()
                os.set_blocking(self.master, False)
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
                kwargs.update(stdin=slave, stdout=slave, stderr=slave)
            self.process = await asyncio.create_subprocess_exec(*self.command, **kwargs)
            if child_socket:
                child_socket.close()
                child_socket = None
            if slave is not None:
                os.close(slave)
                slave = None
            self.registration["binding"] = {"pid": self.process.pid, "processGroup": self.process.pid,
                                             "linuxStartTicks": self._start_ticks(self.process.pid)}
            self.spool.append("process.started", {"pid": self.process.pid, "mode": mode,
                                                   "monotonicNs": str(time.monotonic_ns())})
            if mode == "pipe":
                self.read_tasks = [asyncio.create_task(self._capture_pipe("stdout", self.process.stdout)),
                                   asyncio.create_task(self._capture_pipe("stderr", self.process.stderr))]
            else:
                self.read_tasks = [asyncio.create_task(self._capture_pty())]
            if parent_socket:
                reader, writer = await asyncio.open_connection(sock=parent_socket, limit=MAX_FRAME)
                parent_socket = None
                self.native = CooperativeChannel(reader, writer, self._native_capture)
            self.lifecycle_task = asyncio.create_task(self._lifecycle())
            await self.client.call("update", {**self.ref, "patch": {"binding": self.registration["binding"]}})
            return self
        except BaseException as error:
            self._event("process.startup_failed", {"type": type(error).__name__, "message": str(error)})
            for endpoint in (parent_socket, child_socket):
                if endpoint:
                    endpoint.close()
            if slave is not None:
                os.close(slave)
            await self.close()
            raise

    @staticmethod
    def _start_ticks(pid):
        try:
            return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
        except (OSError, IndexError):
            return None

    async def _register(self):
        for registration in [self.registration, *self.surfaces]:
            arguments = {"registration": registration}
            if registration["id"] in self.tokens:
                arguments["resumeToken"] = self.tokens[registration["id"]]
            receipt = await self.client.call("register", arguments)
            self.tokens[registration["id"]] = receipt["resumeToken"]
        save_private(self.state_path, self.tokens)

    async def reconnect(self, client):
        """Reassert owned continuity; never resend native pending requests."""
        if self.publisher:
            self.publisher.cancel()
            await asyncio.gather(self.publisher, return_exceptions=True)
        self.client = client
        client.on_invoke, client.on_cancel = self._invoke, self._cancel
        await self._register()
        self.publish_error = None
        self.publisher = asyncio.create_task(self._publish())

    def _event(self, kind, body, parents=None):
        try:
            return self.spool.append(kind, body, parents)
        except (CaptureFull, sqlite3.Error, OSError) as error:
            self._capture_failed(error)

    def _capture_failed(self, error):
        if self.spool.failure is None:
            with contextlib.suppress(Exception):
                self.spool.fail(str(error))
        if not self.stopping and self.process is not None:
            asyncio.create_task(self.stop(grace=0.05))

    def _native_capture(self, stream, payload):
        try:
            for offset in range(0, len(payload), 16384):
                self.spool.chunk(stream, payload[offset:offset + 16384])
        except (CaptureFull, sqlite3.Error, OSError) as error:
            self._capture_failed(error)

    async def _capture_pipe(self, stream, reader):
        try:
            while payload := await reader.read(16384):
                self.spool.chunk(stream, payload)
            self._event("process.stream_eof", {"stream": stream})
        except (CaptureFull, sqlite3.Error, OSError) as error:
            self._capture_failed(error)

    async def _capture_pty(self):
        try:
            while payload := await read_fd(self.master):
                self.spool.chunk("pty", payload)
            self._event("process.stream_eof", {"stream": "pty"})
        except (CaptureFull, sqlite3.Error, OSError) as error:
            self._capture_failed(error)

    async def _publish(self):
        try:
            while not self.closed:
                page = self.spool.page(self.publish_cursor, 16)
                for cursor, record in page:
                    await self.client.call("observe", {"source": record["source"], "incarnation": record["incarnation"],
                        "kind": record["kind"], "parents": record["parents"], "body": {**record["body"],
                        "captureRecordId": record["id"], "captureSequence": record["seq"]}})
                    self.publish_cursor = cursor
                if not page:
                    await asyncio.sleep(0.025)
        except Exception as error:
            self.publish_error = str(error)  # All bytes remain in the local spool.

    async def flush_observations(self, timeout=3):
        watermark = self.spool.database.execute("SELECT coalesce(max(cursor),0) FROM records").fetchone()[0]
        deadline = asyncio.get_running_loop().time() + timeout
        while self.publish_cursor < watermark:
            if self.publish_error:
                raise ConnectionError(self.publish_error)
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("capture remains in local spool; publication deadline expired")
            await asyncio.sleep(0.01)

    def _group_signal(self, sig):
        if self.process is None:
            return
        original = self.registration["binding"].get("linuxStartTicks")
        current = self._start_ticks(self.process.pid)
        if original is not None and current is not None and original != current:
            return  # Never signal a recycled PID's unrelated group.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, sig)

    async def _lifecycle(self):
        # wait() can await EOF retained by a descendant after the leader exits.
        while self.process.returncode is None:
            await asyncio.sleep(0.02)
        self._group_signal(signal.SIGTERM)
        await asyncio.sleep(0.1)
        self._group_signal(signal.SIGKILL)
        try:
            await asyncio.wait_for(asyncio.gather(*self.read_tasks), 2)
        except TimeoutError:
            self._event("capture.failure", {"reason": "descendant retained stream beyond cleanup deadline", "laterBytes": "unknown"})
        self._event("process.exited", {"exitCode": self.process.returncode,
            "signal": -self.process.returncode if self.process.returncode < 0 else None})
        if self.native:
            await self.native.close()
        self.finished.set()
        for registration in [self.registration, *self.surfaces]:
            with contextlib.suppress(Exception):
                await self.client.call("update", {"id": registration["id"], "incarnation": registration["incarnation"],
                    "patch": {"metadata": {**registration.get("metadata", {}), "exitCode": self.process.returncode}, "operations": []}})

    async def wait(self, timeout=10):
        await asyncio.wait_for(self.finished.wait(), timeout)
        return self.process.returncode

    async def write_stdin(self, payload: bytes):
        if len(payload) > 32768:
            raise ValueError("stdin write limit is 32768 bytes")
        if self.process is None or self.process.returncode is not None:
            raise ProcessLookupError("owned incarnation has exited")
        async with self.stdin_lock:
            intentions = [self.spool.chunk("stdin.intent", payload[offset:offset + 16384])["id"]
                          for offset in range(0, len(payload), 16384)]
            try:
                if self.mode == "pipe":
                    self.process.stdin.write(payload)
                    await asyncio.wait_for(self.process.stdin.drain(), 2)
                    count = len(payload)
                else:
                    count = await asyncio.wait_for(write_fd(self.master, payload), 2)
            except Exception as error:
                self._event("process.stdin_outcome_unknown", {"reason": str(error)}, intentions)
                raise ConnectionError("stdin delivery outcome unknown; write was not retried") from error
            self._event("process.stdin_written", {"bytes": count, "consumedByChild": None}, intentions)
            return {"bytesWritten": count, "consumedByChild": None}

    async def close_stdin(self):
        if self.mode != "pipe":
            raise ValueError("PTY has no independent stdin close; process.pty.eof sends terminal VEOF")
        self.process.stdin.close()
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            await self.process.stdin.wait_closed()
        self._event("process.stdin_eof_sent", {})

    def resize(self, rows, cols):
        if self.mode != "pty":
            raise ValueError("resize requires PTY mode")
        if type(rows) is not int or type(cols) is not int or not (1 <= rows <= 1000 and 1 <= cols <= 1000):
            raise ValueError("rows and cols must be integers within 1..1000")
        if self.process.returncode is not None:
            raise ProcessLookupError("owned incarnation has exited")
        fcntl.ioctl(self.master, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
        self._event("process.pty_resized", {"rows": rows, "cols": cols})
        return {"rows": rows, "cols": cols}

    def interrupt(self):
        if self.process.returncode is not None:
            raise ProcessLookupError("owned incarnation has exited")
        self._group_signal(signal.SIGINT)
        self._event("process.interrupt_sent", {"signal": "SIGINT"})
        return {"signalSent": "SIGINT", "handledByChild": None}

    async def stop(self, grace=0.5):
        if self.process is None:
            return {"exitCode": None, "started": False}
        if self.stopping:
            await asyncio.wait_for(self.finished.wait(), 5)
            return {"exitCode": self.process.returncode, "captureFailure": self.spool.failure}
        self.stopping = True
        if self.process.returncode is None:
            if self.native:
                with contextlib.suppress(Exception):
                    await self.native.request("demo.stop", timeout=max(0.05, grace))
            elif self.mode == "pipe":
                with contextlib.suppress(Exception):
                    await self.close_stdin()
            deadline = asyncio.get_running_loop().time() + grace
            while self.process.returncode is None and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.01)
            if self.process.returncode is None:
                self._event("process.escalation", {"signal": "SIGTERM"})
                self._group_signal(signal.SIGTERM)
                await asyncio.sleep(grace)
            if self.process.returncode is None:
                self._event("process.escalation", {"signal": "SIGKILL"})
                self._group_signal(signal.SIGKILL)
        if self.lifecycle_task is None:
            self.lifecycle_task = asyncio.create_task(self._lifecycle())
        await asyncio.wait_for(self.finished.wait(), 5)
        return {"exitCode": self.process.returncode, "captureFailure": self.spool.failure}

    async def _reply(self, invocation, status, value=None):
        return await self.client.call("result", {"invocationId": invocation["id"],
            "target": invocation["target"], "status": status, "value": value})

    async def _invoke(self, invocation):
        if invocation["id"] in self.operations:
            return
        if len(self.operations) >= 32:
            await self._reply(invocation, "failed", {"reason": "owned operation concurrency limit (32)"})
            return
        task = asyncio.create_task(self._run_invocation(invocation))
        self.operations[invocation["id"]] = task
        task.add_done_callback(lambda _: self.operations.pop(invocation["id"], None))

    async def _run_invocation(self, invocation):
        operation, arguments = invocation["operation"], invocation["arguments"]
        try:
            target = next((r for r in [self.registration, *self.surfaces]
                           if {"id": r["id"], "incarnation": r["incarnation"]} == invocation["target"]), None)
            if target is None or operation not in target["operations"]:
                raise ValueError("stale owned incarnation or unsupported surface operation")
            if self.process is None or self.process.returncode is not None:
                raise ProcessLookupError("owned incarnation has exited")
            await self._reply(invocation, "started")
            if operation.startswith("demo.") and self.native:
                native = await self.native.request(operation, arguments, correlation=invocation["id"])
                await self._reply(invocation, native["status"], native.get("value"))
                return
            if operation == "process.stdin.write":
                value = await self.write_stdin(base64.b64decode(arguments["bytesBase64"], validate=True))
            elif operation == "process.stdin.eof":
                await self.close_stdin()
                value = {"stdin": "closed"}
            elif operation == "process.pty.eof":
                value = await self.write_stdin(b"\x04")
            elif operation == "process.pty.resize":
                value = self.resize(arguments["rows"], arguments["cols"])
            elif operation == "process.interrupt":
                value = self.interrupt()
            elif operation == "process.stop":
                value = await self.stop()
            else:
                raise ValueError("operation not available on this process")
            await self._reply(invocation, "completed", value)
        except (ConnectionError, TimeoutError) as error:
            with contextlib.suppress(Exception):
                await self._reply(invocation, "outcome-unknown", {"reason": str(error)})
        except Exception as error:
            with contextlib.suppress(Exception):
                await self._reply(invocation, "failed", {"reason": str(error)})

    async def _cancel(self, invocation):
        if self.native and invocation["id"] in self.operations and invocation["operation"].startswith("demo."):
            await self.native.cancel(invocation["id"])
        # A request cannot undo an OS write/signal. Only acknowledgement cancels.

    async def close(self):
        if self.closed:
            return
        if self.process is not None:
            with contextlib.suppress(Exception):
                await self.stop()
        if self.native:
            await self.native.close()
        if self.operations:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.gather(*list(self.operations.values())), 2)
        with contextlib.suppress(Exception):
            await self.flush_observations(1)
        for registration in reversed([self.registration, *self.surfaces]):
            with contextlib.suppress(Exception):
                await self.client.call("unregister", {"id": registration["id"],
                    "incarnation": registration["incarnation"], "reason": "owned companion closed"})
        self.closed = True
        if self.publisher:
            self.publisher.cancel()
            await asyncio.gather(self.publisher, return_exceptions=True)
        if self.lifecycle_task:
            await asyncio.gather(self.lifecycle_task, return_exceptions=True)
        if self.master is not None:
            os.close(self.master)
            self.master = None
        self.spool.close()


async def _main_async(args):
    from zygon.client import Client
    client = await Client.connect(args.socket)
    owned = await OwnedProcess.launch(client, args.runtime_dir, args.command or None, participant_id=args.id, mode=args.mode)
    print(json.dumps({"host": owned.ref, "surfaces": owned.surfaces,
                      "pid": owned.process.pid, "spool": str(owned.spool.path)}), flush=True)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(owned.stop()))
    try:
        await owned.finished.wait()
        if owned.spool.failure:
            print(json.dumps(owned.spool.failure), file=sys.stderr)
            return 74
        return owned.process.returncode or 0
    finally:
        await owned.close()
        await client.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--runtime-dir", required=True, type=Path)
    parser.add_argument("--id", default="process-demo")
    parser.add_argument("--mode", choices=("pipe", "pty"), default="pipe")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    raise SystemExit(asyncio.run(_main_async(args)))


if __name__ == "__main__":
    main()
