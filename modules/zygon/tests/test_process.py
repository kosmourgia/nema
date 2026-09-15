"""Real process/PTY integration and actual registry attachment continuity."""
from __future__ import annotations

import asyncio
import base64
import contextlib
import fcntl
import json
from pathlib import Path
import signal
import socket
import sqlite3
import struct
import sys
import tempfile
import termios
import unittest
import uuid

from zygon.attached import AttachedCompanion
from zygon.processes import CaptureFull, CooperativeChannel, OwnedProcess, RawSpool, compact


class FakeRegistry:
    """Isolates OS tests. RealRegistry tests below cover the actual socket seam."""
    def __init__(self, stall_observations=False):
        self.registrations, self.records, self.results = {}, [], []
        self.on_invoke = self.on_cancel = None
        self.stall = asyncio.Event() if stall_observations else None
        self.closed = False

    async def close(self):
        self.closed = True

    async def call(self, method, params):
        if method == "register":
            registration = params["registration"]
            self.registrations[registration["id"]] = registration.copy()
            return {"registration": registration, "resumeToken": "token-" + registration["id"]}
        if method == "update":
            self.registrations[params["id"]].update(params["patch"])
            return self.registrations[params["id"]]
        if method == "unregister":
            self.registrations.pop(params["id"], None)
            return {}
        if method == "observe":
            if self.stall is not None:
                await self.stall.wait()
            self.records.append(params)
            return {}
        if method == "result":
            self.results.append(params)
            return params
        raise AssertionError(method)


async def eventually(predicate, timeout=3):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition did not become true before deadline")
        await asyncio.sleep(0.01)


def exited(pid):
    path = Path(f"/proc/{pid}/stat")
    return not path.exists() or path.read_text().rsplit(")", 1)[1].split()[0] == "Z"


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        root = Path(".nema")
        root.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="p-", dir=root)
        self.runtime = Path(self.temporary.name).resolve()
        self.owned, self.external, self.attached, self.clients, self.servers = [], [], [], [], []

    async def asyncTearDown(self):
        for attached in self.attached:
            await attached.detach()
        for owned in self.owned:
            await owned.close()
        for external in self.external:
            if external.returncode is None:
                external.terminate()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(external.wait(), 3)
            if external.returncode is None:
                external.kill()
                await external.wait()
        for client in self.clients:
            await client.close()
        for server in self.servers:
            await server.close()
        self.temporary.cleanup()

    async def launch(self, command=None, **options):
        client = options.pop("client", FakeRegistry())
        runtime = options.pop("runtime", self.runtime / ("o-" + uuid.uuid4().hex[:6]))
        owned = await OwnedProcess.launch(client, runtime, command, **options)
        self.owned.append(owned)
        return owned, client

    def invocation(self, owned, operation="demo.delay", arguments=None, identifier=None):
        surface = owned.surfaces[0]
        return {"id": identifier or uuid.uuid4().hex, "target": {"id": surface["id"], "incarnation": surface["incarnation"]},
            "requester": {"id": "test", "incarnation": "test-run"}, "operation": operation,
            "arguments": arguments or {"text": "slow", "seconds": 0.3}, "correlation": "test-correlation"}

    async def test_pipe_preserves_partial_invalid_bytes_and_stdio(self):
        owned, client = await self.launch()
        await eventually(lambda: b"\xff\xfe\n" in owned.spool.raw_bytes("stdout"))
        self.assertIn(b"birth:partial\xff\xfe\n", owned.spool.raw_bytes("stdout"))
        self.assertEqual(b"diagnostic:\x80\n", owned.spool.raw_bytes("stderr"))
        chunks = [r for _, r in owned.spool.page(0, 200) if r["kind"] == "capture.chunk" and r["body"]["stream"] == "stdout"]
        self.assertEqual(base64.b64decode(chunks[0]["body"]["bytesBase64"]), b"birth:partial")
        await owned.write_stdin(b"hello\x80\n")
        await eventually(lambda: b"stdin:hello\x80\n" in owned.spool.raw_bytes("stdout"))
        self.assertEqual(len(client.registrations), 3)
        self.assertEqual(owned.surfaces[0]["parent"], owned.ref)
        self.assertIsNone(owned.registration["metadata"]["terminalEmulatorTab"])
        await owned.close_stdin()
        self.assertEqual(await owned.wait(), 0)
        self.assertIn(b"stdin:EOF", owned.spool.raw_bytes("stdout"))
        kinds = [r["kind"] for _, r in owned.spool.page(0, 200)]
        self.assertEqual(kinds.count("process.stream_eof"), 2)

    async def test_pending_request_observation_and_independent_reply(self):
        owned, client = await self.launch()
        await owned._invoke(self.invocation(owned, identifier="slow-correlation"))
        await eventually(lambda: b"request-is-waiting" in owned.spool.raw_bytes("stdout"))
        await owned._invoke(self.invocation(owned, "demo.echo", {"text": "quick"}, "quick-correlation"))
        await eventually(lambda: any(r["status"] == "completed" and r["invocationId"] == "quick-correlation" for r in client.results))
        self.assertFalse(any(r["status"] == "completed" and r["invocationId"] == "slow-correlation" for r in client.results))
        await eventually(lambda: any(r["status"] == "completed" and r["invocationId"] == "slow-correlation" for r in client.results))
        value = next(r["value"] for r in client.results if r["status"] == "completed" and r["invocationId"] == "slow-correlation")
        self.assertEqual(value["correlation"], "slow-correlation")
        self.assertIn(b'"method":"observation"', owned.spool.raw_bytes("control.in"))
        self.assertNotIn(b'"version":1', owned.spool.raw_bytes("stdout"))

    async def test_cooperative_cancel_is_acknowledged(self):
        owned, client = await self.launch()
        invocation = self.invocation(owned, arguments={"seconds": 5, "text": "will-cancel"})
        await owned._invoke(invocation)
        await eventually(lambda: b"request-is-waiting" in owned.spool.raw_bytes("stdout"))
        await owned._cancel(invocation)
        await eventually(lambda: any(r["status"] == "cancelled" for r in client.results))
        self.assertEqual([r["status"] for r in client.results], ["started", "cancelled"])

    async def test_native_disconnect_marks_pending_outcome_unknown(self):
        owned, client = await self.launch()
        await owned._invoke(self.invocation(owned, arguments={"seconds": 5}))
        await eventually(lambda: b"request-is-waiting" in owned.spool.raw_bytes("stdout"))
        await owned.native.close()
        await eventually(lambda: any(r["status"] == "outcome-unknown" for r in client.results))

    async def test_pty_resize_interrupt_eof_and_combined_stream(self):
        owned, _ = await self.launch(mode="pty")
        await eventually(lambda: b"diagnostic:\x80" in owned.spool.raw_bytes("pty"))
        owned.resize(37, 109)
        dimensions = struct.unpack("HHHH", fcntl.ioctl(owned.master, termios.TIOCGWINSZ, bytes(8)))
        self.assertEqual(dimensions[:2], (37, 109))
        owned.interrupt()
        await eventually(lambda: b"cooperatively-handled" in owned.spool.raw_bytes("pty"))
        await owned.write_stdin(b"pty-line\n")
        await eventually(lambda: b"stdin:pty-line" in owned.spool.raw_bytes("pty"))
        self.assertEqual(owned.spool.raw_bytes("stderr"), b"")
        self.assertIn(b"\r\n", owned.spool.raw_bytes("pty"))
        with self.assertRaises(ValueError):
            await owned.close_stdin()
        await owned.write_stdin(b"\x04")
        self.assertEqual(await owned.wait(), 0)

    async def test_nonzero_exit_and_no_cooperative_assumption(self):
        command = [sys.executable, "-c", "import os; os.write(1,b'last-no-newline\\xff'); os.write(2,b'err'); raise SystemExit(7)"]
        owned, _ = await self.launch(command)
        self.assertEqual(await owned.wait(), 7)
        self.assertEqual(owned.spool.raw_bytes("stdout"), b"last-no-newline\xff")
        self.assertEqual(owned.spool.raw_bytes("stderr"), b"err")
        self.assertEqual(owned.surfaces[0]["operations"], [])

    async def test_startup_failure_is_retained_and_unregistered(self):
        client, runtime = FakeRegistry(), self.runtime / "failed"
        with self.assertRaises(FileNotFoundError):
            await OwnedProcess.launch(client, runtime, ["/nonexistent/zygon-program"])
        self.assertEqual(client.registrations, {})
        database = sqlite3.connect(next(runtime.glob("*.sqlite")))
        self.addCleanup(database.close)
        records = [json.loads(row[0]) for row in database.execute("SELECT record FROM records")]
        self.assertIn("process.startup_failed", [record["kind"] for record in records])

    async def test_descendant_cleanup_when_leader_exits(self):
        script = "import subprocess,sys; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); print(p.pid,flush=True)"
        owned, _ = await self.launch([sys.executable, "-c", script])
        self.assertEqual(await owned.wait(), 0)
        self.assertTrue(exited(int(owned.spool.raw_bytes("stdout").strip())))

    async def test_stop_escalates_when_eof_and_term_ignored(self):
        script = "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); time.sleep(60)"
        owned, _ = await self.launch([sys.executable, "-c", script])
        await eventually(lambda: b"ready" in owned.spool.raw_bytes("stdout"))
        self.assertEqual((await owned.stop(grace=0.05))["exitCode"], -signal.SIGKILL)
        signals = [r["body"]["signal"] for _, r in owned.spool.page(0, 200) if r["kind"] == "process.escalation"]
        self.assertEqual(signals, ["SIGTERM", "SIGKILL"])

    async def test_capture_limit_explicitly_stops_child(self):
        owned, _ = await self.launch([sys.executable, "-c", "import os,time; os.write(1,b'x'*100000); time.sleep(60)"], max_capture_bytes=4096)
        await owned.wait()
        self.assertEqual(owned.spool.failure["kind"], "capture.failure")
        self.assertGreater(owned.spool.failure["body"]["unavailableBytesInRead"], 0)
        self.assertLessEqual(owned.spool.bytes_used, 4096)

    async def test_stalled_publication_does_not_block_capture_or_stop(self):
        client = FakeRegistry(stall_observations=True)
        owned, _ = await self.launch([sys.executable, "-c", "import os; os.write(1,b'x'*131072)"], client=client)
        self.assertEqual(await owned.wait(), 0)
        self.assertEqual(len(owned.spool.raw_bytes("stdout")), 131072)
        self.assertEqual(owned.publish_cursor, 0)
        with self.assertRaises(TimeoutError):
            await owned.flush_observations(0.03)
        client.stall.set()
        await owned.flush_observations()
        self.assertTrue(client.records)

    async def test_restart_changes_incarnation_and_reuses_saved_registration_token(self):
        runtime = self.runtime / "continuity"
        owned, client = await self.launch(runtime=runtime)
        old_ref = owned.ref
        await owned.close()
        restarted, _ = await self.launch(runtime=runtime, client=client)
        self.assertEqual(old_ref["id"], restarted.ref["id"])
        self.assertNotEqual(old_ref["incarnation"], restarted.ref["incarnation"])
        self.assertEqual(restarted.tokens[restarted.ref["id"]], "token-" + restarted.ref["id"])

    async def external_host(self, endpoint):
        process = await asyncio.create_subprocess_exec(sys.executable, "-m", "zygon.demo_child", "--listen", str(endpoint),
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        self.external.append(process)
        await eventually(lambda: endpoint.exists() or process.returncode is not None)
        if process.returncode is not None:
            raise AssertionError((await process.stderr.read()).decode())
        return process

    async def registry(self):
        from zygon.server import Server
        server = await Server(self.runtime / "r").start()
        self.servers.append(server)
        return server

    async def client(self, server):
        from zygon.client import Client
        client = await Client.connect(server.socket_path)
        self.clients.append(client)
        return client

    async def test_real_attach_detach_and_restart_preserve_only_justified_identity(self):
        server = await self.registry()
        observer = await self.client(server)
        endpoint = self.runtime / "h.sock"
        external = await self.external_host(endpoint)
        attached = await AttachedCompanion.attach(await self.client(server), endpoint, self.runtime / "a")
        self.attached.append(attached)
        old_ref = attached.ref
        self.assertFalse(attached.registration["metadata"]["sourceStdoutAvailable"])
        self.assertFalse(any(op.startswith("process.") for registration in [attached.registration, *attached.surfaces] for op in registration["operations"]))
        self.assertEqual((await attached.native.request("demo.echo", {"text": "attached"}))["value"]["echo"], "attached")
        await attached.detach()
        await asyncio.sleep(0.05)
        self.assertIsNone(external.returncode)
        rows = (await observer.call("inspect", {}))["registrations"]
        self.assertTrue(all(row["presence"] == "unknown" and row["active"] for row in rows))
        again = await AttachedCompanion.attach(await self.client(server), endpoint, self.runtime / "a")
        self.attached.append(again)
        self.assertEqual(old_ref, again.ref)
        self.assertEqual((await again.native.request("demo.stop"))["status"], "failed")
        await again.detach()
        external.terminate()
        await external.wait()
        await self.external_host(endpoint)
        restarted = await AttachedCompanion.attach(await self.client(server), endpoint, self.runtime / "a")
        self.attached.append(restarted)
        self.assertEqual(old_ref["id"], restarted.ref["id"])
        self.assertNotEqual(old_ref["incarnation"], restarted.ref["incarnation"])

    async def test_endpoint_loss_marks_all_surfaces_unknown_and_pending_uncertain(self):
        server = await self.registry()
        requester = await self.client(server)
        caller = {"id": "caller", "incarnation": "caller-1", "kind": "client", "medium": "unix", "mode": "observer", "parent": None, "operations": []}
        await requester.call("register", {"registration": caller})
        endpoint = self.runtime / "h.sock"
        external = await self.external_host(endpoint)
        attached = await AttachedCompanion.attach(await self.client(server), endpoint, self.runtime / "a")
        self.attached.append(attached)
        target = {k: attached.surfaces[0][k] for k in ("id", "incarnation")}
        invocation = await requester.call("invoke", {"requester": {"id": "caller", "incarnation": "caller-1"},
            "target": target, "operation": "demo.delay", "arguments": {"seconds": 5}, "correlation": "loss"})
        await eventually(lambda: b'"phase":"waiting"' in attached.spool.raw_bytes("control.in"))
        external.kill()
        await external.wait()
        terminal = await requester.wait_result(invocation["id"], timeout=3)
        self.assertEqual(terminal["status"], "outcome-unknown")
        await asyncio.sleep(0.05)
        rows = [r for r in (await requester.call("inspect", {}))["registrations"] if r["id"] != "caller"]
        self.assertTrue(all(row["presence"] == "unknown" for row in rows))

    async def test_attached_capture_limit_detaches_without_stopping_host(self):
        server = await self.registry()
        observer = await self.client(server)
        endpoint = self.runtime / "h.sock"
        external = await self.external_host(endpoint)
        attached = await AttachedCompanion.attach(await self.client(server), endpoint, self.runtime / "a")
        self.attached.append(attached)
        attached.spool.max_bytes = attached.spool.bytes_used
        with self.assertRaises(CaptureFull):
            await attached.native.request("demo.read")
        await asyncio.wait_for(attached.monitor, 3)
        self.assertEqual(attached.spool.failure["body"]["action"], "detach-attached-companion")
        self.assertIsNone(external.returncode)
        await asyncio.sleep(0.05)
        self.assertTrue(all(row["presence"] == "unknown" for row in (await observer.call("inspect", {}))["registrations"]))

    async def test_owned_reconnect_keeps_incarnation_and_publishes_spool(self):
        server = await self.registry()
        provider = await self.client(server)
        owned, _ = await self.launch(client=provider)
        old_ref = owned.ref
        await provider.close()
        await asyncio.sleep(0.2)
        retained = len(owned.spool.raw_bytes("stdout"))
        self.assertGreater(retained, 0)
        await owned.reconnect(await self.client(server))
        self.assertEqual(owned.ref, old_ref)
        await owned.flush_observations()
        rows = (await owned.client.call("inspect", {}))["registrations"]
        self.assertTrue(all(row["presence"] == "online" for row in rows))

    async def test_abrupt_companion_death_kills_direct_child(self):
        script = ("import asyncio,sys\n"
                  f"sys.path.insert(0,{str(Path(__file__).resolve().parent)!r})\n"
                  "from test_process import FakeRegistry\n"
                  "from pathlib import Path\n"
                  "from zygon.processes import OwnedProcess\n"
                  "async def run():\n"
                  f" p=await OwnedProcess.launch(FakeRegistry(),Path({str(self.runtime / 'death')!r}),"
                  "[sys.executable,'-c','import time; time.sleep(60)'])\n"
                  " print(p.process.pid,flush=True)\n"
                  " await asyncio.Event().wait()\n"
                  "asyncio.run(run())\n")
        companion = await asyncio.create_subprocess_exec(sys.executable, "-c", script, stdout=asyncio.subprocess.PIPE)
        self.external.append(companion)
        child_pid = int(await asyncio.wait_for(companion.stdout.readline(), 3))
        self.assertFalse(exited(child_pid))
        companion.kill()
        await companion.wait()
        await eventually(lambda: exited(child_pid))

    async def test_native_duplicate_terminal_and_partial_invalid_bytes_retained(self):
        first, second = socket.socketpair()
        reader, writer = await asyncio.open_connection(sock=first)
        peer_reader, peer_writer = await asyncio.open_connection(sock=second)
        observed = []
        channel = CooperativeChannel(reader, writer, lambda stream, data: observed.append((stream, data)))
        try:
            pending = asyncio.create_task(channel.request("demo.echo", correlation="native-duplicate"))
            request = json.loads(await peer_reader.readline())
            self.assertEqual(request["id"], "native-duplicate")
            first_reply = compact({"version": 1, "id": "native-duplicate", "status": "completed", "value": 1})
            duplicate = compact({"version": 1, "id": "native-duplicate", "status": "failed", "value": 2})
            peer_writer.write(first_reply + duplicate + b'partial-native:\xff')
            await peer_writer.drain()
            peer_writer.close()
            await peer_writer.wait_closed()
            self.assertEqual((await pending)["value"], 1)
            await channel.task
            received = b"".join(data for stream, data in observed if stream == "control.in")
            self.assertEqual(received, first_reply + duplicate + b'partial-native:\xff')
        finally:
            await channel.close()
            peer_writer.close()


class SpoolTests(unittest.TestCase):
    def test_bounded_record_count_has_reserved_failure(self):
        root = Path(".nema")
        root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="spool-", dir=root) as directory:
            spool = RawSpool(Path(directory) / "raw.sqlite", "source", "incarnation", max_records=2)
            try:
                spool.chunk("stdout", b"\x80")
                spool.chunk("stderr", b"last")
                with self.assertRaises(CaptureFull):
                    spool.chunk("stdout", b"lost")
                records = spool.page()
                self.assertEqual([r["seq"] for _, r in records], ["1", "2", "3"])
                self.assertEqual(records[-1][1]["kind"], "capture.failure")
                self.assertEqual(records[-1][1]["body"]["unavailableBytesInRead"], 4)
                self.assertEqual(spool.raw_bytes("stdout"), b"\x80")
            finally:
                spool.close()


if __name__ == "__main__":
    unittest.main()
