"""An ordinary byte-producing Python host with a separate cooperative socket.

An attached --listen host owns its lifetime: disconnect cannot stop it.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
from pathlib import Path
import signal
import socket
import sys
import uuid

from zygon.processes import MAX_FRAME, OwnedProcess, compact, private_directory, read_fd, save_private


class DemoHost:
    def __init__(self, *, owned, identity=None):
        self.owned = owned
        self.id = identity or "python-host-" + uuid.uuid4().hex
        self.incarnation = "python-session-" + uuid.uuid4().hex
        self.value = "initial"
        self.stopped = asyncio.Event()
        self.connections, self.connection_tasks = set(), set()

    @staticmethod
    def output(stream, payload):
        with contextlib.suppress(BrokenPipeError, OSError):
            os.write(stream, payload)

    def hello(self):
        return {"id": self.id, "incarnation": self.incarnation, "pid": os.getpid(),
                "linuxStartTicks": OwnedProcess._start_ticks(os.getpid()),
                "operations": ["demo.echo", "demo.delay", "demo.read"],
                "surfaces": ["requests", "output"], "sourceStdoutAvailable": False}

    async def handle(self, reader, writer):
        if len(self.connections) >= 8:
            writer.close()
            return
        self.connections.add(writer)
        task = asyncio.current_task()
        self.connection_tasks.add(task)
        pending, replies = {}, {}
        lock = asyncio.Lock()

        async def send(value):
            async with lock:
                writer.write(compact(value))
                await asyncio.wait_for(writer.drain(), 2)

        async def execute(frame):
            identifier, method = frame["id"], frame["method"]
            try:
                args = frame.get("arguments", {})
                if method == "hello":
                    value = self.hello()
                elif method == "demo.read":
                    value = {"value": self.value, "host": self.id, "incarnation": self.incarnation}
                elif method in {"demo.echo", "demo.delay"}:
                    if method == "demo.delay":
                        seconds = args.get("seconds", 0.3)
                        if type(seconds) not in (int, float) or not 0 <= seconds <= 30:
                            raise ValueError("seconds must be finite and within 0..30")
                        self.output(1, b"unsolicited:request-is-waiting\n")
                        await send({"version": 1, "method": "observation", "body": {
                            "kind": "demo.progress", "correlation": identifier, "phase": "waiting"}})
                        await asyncio.sleep(seconds)
                    value = {"echo": args.get("text", "hello"), "correlation": identifier}
                    self.value = str(args.get("text", "hello"))
                    self.output(1, ("reply:" + identifier + ":" + self.value + "\n").encode())
                elif method == "demo.stop" and self.owned:
                    value = {"stopping": True}
                    self.stopped.set()
                else:
                    raise ValueError("unsupported cooperative operation")
                response = {"version": 1, "id": identifier, "status": "completed", "value": value}
            except asyncio.CancelledError:
                response = {"version": 1, "id": identifier, "status": "cancelled", "value": {"cooperative": True}}
            except Exception as error:
                response = {"version": 1, "id": identifier, "status": "failed", "value": {"reason": str(error)}}
            replies[identifier] = response
            if len(replies) > 256:
                replies.pop(next(iter(replies)))
            with contextlib.suppress(ConnectionError, OSError, TimeoutError):
                await send(response)
            pending.pop(identifier, None)

        try:
            while line := await reader.readline():
                if len(line) > MAX_FRAME or not line.endswith(b"\n"):
                    raise ValueError("invalid/oversized cooperative frame")
                frame = json.loads(line)
                if not isinstance(frame, dict) or frame.get("version") != 1:
                    raise ValueError("invalid cooperative envelope")
                if frame.get("method") == "cancel":
                    identifier = frame.get("arguments", {}).get("correlation")
                    if identifier in pending:
                        pending[identifier].cancel()
                    continue
                identifier = frame.get("id")
                if not isinstance(identifier, str) or len(identifier) > 512:
                    raise ValueError("cooperative request needs bounded string ID")
                if identifier in replies:
                    await send(replies[identifier])
                elif identifier in pending:
                    continue
                elif len(pending) >= 32:
                    await send({"version": 1, "id": identifier, "status": "failed",
                                "value": {"reason": "cooperative pending limit (32)"}})
                else:
                    pending[identifier] = asyncio.create_task(execute(frame))
        except (ValueError, UnicodeError, ConnectionError, OSError):
            pass
        finally:
            for worker in list(pending.values()):
                worker.cancel()
            await asyncio.gather(*list(pending.values()), return_exceptions=True)
            self.connections.discard(writer)
            self.connection_tasks.discard(task)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            if self.owned:
                self.stopped.set()

    async def output_loop(self):
        self.output(1, b"birth:partial")
        await asyncio.sleep(0.025)
        self.output(1, b"\xff\xfe\n")
        self.output(2, b"diagnostic:\x80\n")
        counter = 0
        while not self.stopped.is_set():
            await asyncio.sleep(0.15)
            counter += 1
            self.output(1, f"unsolicited:tick:{counter}\n".encode())

    async def stdin_loop(self):
        os.set_blocking(0, False)
        try:
            while payload := await read_fd(0):
                self.output(1, b"stdin:" + payload)
            self.output(1, b"stdin:EOF\n")
        except OSError:
            pass
        self.stopped.set()

    async def run(self, control_fd=None, listen=None):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self.stopped.set)
        loop.add_signal_handler(signal.SIGINT, lambda: self.output(1, b"signal:SIGINT:cooperatively-handled\n"))
        loop.add_signal_handler(signal.SIGWINCH, lambda: self.output(1, b"signal:SIGWINCH\n"))
        server = None
        tasks = [asyncio.create_task(self.output_loop())]
        try:
            if listen:
                private_directory(listen.parent)
                if listen.exists():
                    raise FileExistsError(f"endpoint already exists: {listen}; do not replace a live host")
                server = await asyncio.start_unix_server(self.handle, str(listen), limit=MAX_FRAME)
                listen.chmod(0o600)
                print(json.dumps({"endpoint": str(listen), **self.hello()}), file=sys.stderr, flush=True)
            elif control_fd is not None:
                reader, writer = await asyncio.open_connection(sock=socket.socket(fileno=control_fd), limit=MAX_FRAME)
                tasks.append(asyncio.create_task(self.handle(reader, writer)))
                tasks.append(asyncio.create_task(self.stdin_loop()))
            else:
                tasks.append(asyncio.create_task(self.stdin_loop()))
            await self.stopped.wait()
        finally:
            if server:
                server.close()
                await server.wait_closed()
                listen.unlink(missing_ok=True)
            for writer in list(self.connections):
                writer.close()
            all_tasks = tasks + list(self.connection_tasks)
            for task in all_tasks:
                task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-fd", type=int)
    parser.add_argument("--listen", type=Path)
    args = parser.parse_args()
    identity = None
    if args.listen:
        private_directory(args.listen.parent)
        state = args.listen.with_suffix(args.listen.suffix + ".identity.json")
        if state.exists():
            identity = json.loads(state.read_text())["hostId"]
        else:
            identity = "python-host-" + uuid.uuid4().hex
            save_private(state, {"hostId": identity})
    asyncio.run(DemoHost(owned=args.listen is None, identity=identity).run(args.control_fd, args.listen))


if __name__ == "__main__":
    main()
