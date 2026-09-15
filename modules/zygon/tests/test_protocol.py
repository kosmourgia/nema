"""Adversarial public-wire tests of a real Unix server and independent clients."""
import asyncio
import contextlib
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest

from zygon.client import Client
from zygon.protocol import ProtocolError
from zygon.server import Server

MODULE = Path(__file__).resolve().parents[1]
TERMINAL = {"completed", "failed", "cancelled", "outcome-unknown"}


def registration(identity, incarnation="incarnation-1", **fields):
    return dict(id=identity, incarnation=incarnation, kind="host", medium="cooperative-test-endpoint",
                mode="attached", operations=["fixture.echo"], parent=None) | fields


def reference(reg):
    return {k: reg[k] for k in ("id", "incarnation")}


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Keep socket paths below Linux's 108-byte sockaddr_un limit.
        runtimes = MODULE.parents[1] / ".nema" / "pt"
        runtimes.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="r-", dir=runtimes)
        self.server = await Server(Path(self.temp.name)).start()
        self.clients = []
        self.owner, self.requester, self.observer = [await self.connect() for _ in range(3)]
        self.host = registration("host")
        self.caller = registration("caller", kind="client", mode="observer", operations=[])
        self.host_receipt = await self.owner.call("register", {"registration": self.host})
        await self.requester.call("register", {"registration": self.caller})
        self.incoming, self.cancels = asyncio.Queue(), asyncio.Queue()
        self.owner.on_invoke, self.owner.on_cancel = self.incoming.put, self.cancels.put

    async def asyncTearDown(self):
        for client in self.clients:
            with contextlib.suppress(Exception):
                await client.close()
        await self.server.close()
        self.temp.cleanup()

    async def connect(self):
        client = await Client.connect(self.server.socket_path)
        self.clients.append(client)
        return client

    async def invoke(self, correlation="request-1", **fields):
        return await self.requester.call("invoke", dict(requester=reference(self.caller), target=reference(self.host),
            operation="fixture.echo", arguments={"text": "hello"}, correlation=correlation) | fields)

    async def reply(self, invocation, status="completed", **fields):
        return await self.owner.call("result", dict(invocationId=invocation["id"], target=reference(self.host), status=status) | fields)

    async def snapshot(self, client=None):
        return await asyncio.wait_for((client or self.observer).call("inspect", {}), 2)

    async def wait_status(self, identifier, statuses=TERMINAL):
        async def poll():
            while True:
                item = next(i for i in (await self.snapshot())["invocations"] if i["id"] == identifier)
                if item["status"] in statuses:
                    return item
                await asyncio.sleep(0.01)
        return await asyncio.wait_for(poll(), 3)

    async def catch_up(self, cursor="0", limit=7):
        records = []
        for _ in range(1000):
            page = await self.observer.call("subscribe", {"cursor": cursor, "limit": limit})
            self.assertIsInstance(page["cursor"], str)
            self.assertLessEqual(len(page["records"]), limit)
            self.assertGreaterEqual(int(page["cursor"]), int(cursor))
            records.extend(page["records"])
            if not page["more"]:
                return page["cursor"], records
            self.assertGreater(int(page["cursor"]), int(cursor))
            cursor = page["cursor"]
        self.fail("finite catch-up did not advance")

    async def observe(self, number):
        return await self.owner.call("observe", dict(source=self.host["id"], incarnation=self.host["incarnation"],
            kind="fixture.unsolicited", body={"number": number}, parents=[]))

    async def test_unknown_registration_fields_and_unknown_geometry_survive(self):
        fixture = json.loads((MODULE / "fixtures/protocol/unknown-registration.json").read_text())
        first = await self.owner.call("register", {"registration": fixture})
        second = await self.owner.call("register", {"registration": fixture})
        self.assertEqual(first["resumeToken"], second["resumeToken"])
        rows = [r for r in (await self.snapshot())["registrations"] if r["id"] == fixture["id"]]
        self.assertEqual(len(rows), 1)
        for key, value in fixture.items():
            self.assertEqual(rows[0][key], value)
        self.assertIsNone(rows[0].get("geometry"))
        self.assertIsNone(rows[0]["metadata"]["terminalEmulatorTab"])

    async def test_nested_surface_parent_incarnations(self):
        child = registration("surface-a", kind="surface", parent=reference(self.host))
        grandchild = registration("surface-b", kind="surface", parent=reference(child))
        for reg in (child, grandchild):
            await self.owner.call("register", {"registration": reg})
        rows = {r["id"]: r for r in (await self.snapshot())["registrations"]}
        self.assertEqual(rows["surface-a"]["parent"], reference(self.host))
        self.assertEqual(rows["surface-b"]["parent"], reference(child))

    async def test_owner_token_and_requester_authentication(self):
        stranger = await self.connect()
        invocation = await self.invoke()
        operations = [
            ("register", {"registration": self.host}),
            ("register", {"registration": self.host, "resumeToken": "wrong"}),
            ("update", reference(self.host) | {"patch": {"metadata": {"spoof": True}}}),
            ("observe", dict(source="host", incarnation=self.host["incarnation"], kind="spoof", body={})),
            ("invoke", dict(requester=reference(self.caller), target=reference(self.host), operation="fixture.echo", arguments={}, correlation="spoof")),
            ("result", dict(invocationId=invocation["id"], target=reference(self.host), status="completed")),
            ("cancel", dict(invocationId=invocation["id"], requester=reference(self.caller))),
            ("unregister", reference(self.host) | {"reason": "spoof"}),
        ]
        for method, params in operations:
            with self.subTest(method=method), self.assertRaises(ProtocolError):
                await stranger.call(method, params)

    async def test_immutable_fields_cannot_be_updated(self):
        for field, value in (("id", "hijacked"), ("incarnation", "new"), ("parent", reference(self.caller)), ("mode", "owned")):
            with self.subTest(field=field), self.assertRaises(ProtocolError):
                await self.owner.call("update", reference(self.host) | {"patch": {field: value}})
        await self.owner.call("update", reference(self.host) | {"patch": {"metadata": {"known": True, "future": None}}})
        host = next(r for r in (await self.snapshot())["registrations"] if r["id"] == "host")
        self.assertEqual(host["metadata"], {"known": True, "future": None})

    async def test_failed_large_registration_does_not_steal_owner(self):
        replacement = await self.connect()
        with self.assertRaises(ProtocolError):
            await replacement.call("register", {"registration": self.host | {"metadata": {"large": "x" * 50000}}, "resumeToken": self.host_receipt["resumeToken"]})
        await self.observe(77)
        with self.assertRaises(ProtocolError):
            await replacement.call("update", reference(self.host) | {"patch": {"metadata": {"stolen": True}}})

    async def test_connection_replacement_marks_pending_unknown(self):
        invocation = await self.invoke()
        await asyncio.wait_for(self.incoming.get(), 2)
        replacement = await self.connect()
        replayed = asyncio.Queue()
        replacement.on_invoke = replayed.put
        receipt = await replacement.call("register", {"registration": self.host, "resumeToken": self.host_receipt["resumeToken"]})
        self.assertEqual(receipt["registration"]["incarnation"], self.host["incarnation"])
        self.assertEqual((await self.wait_status(invocation["id"]))["status"], "outcome-unknown")
        with self.assertRaises(ProtocolError):
            await self.reply(invocation, value="old connection")
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(replayed.get(), 0.05)

    async def test_duplicate_correlation_does_not_dispatch_twice(self):
        first = await self.invoke()
        self.assertEqual((await asyncio.wait_for(self.incoming.get(), 2))["id"], first["id"])
        self.assertEqual((await self.invoke())["id"], first["id"])
        with self.assertRaises(ProtocolError):
            await self.invoke(arguments={"differentEffect": True})
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.incoming.get(), 0.05)
        self.assertEqual(len((await self.snapshot())["invocations"]), 1)

    async def test_duplicate_terminal_keeps_first_value_and_record(self):
        invocation = await self.invoke()
        first = await self.reply(invocation, value={"winner": 1})
        for status, value in (("completed", {"winner": 1}), ("failed", {"winner": 2})):
            duplicate = await self.reply(invocation, status, value=value)
            self.assertEqual(duplicate["recordId"], first["recordId"])
            self.assertEqual(duplicate["status"], "completed")
            self.assertEqual(duplicate["value"], {"winner": 1})

    async def test_slow_handler_keeps_observations_and_other_calls_moving(self):
        waiting, release = asyncio.Event(), asyncio.Event()
        async def handler(invocation):
            if invocation["correlation"] == "slow":
                await self.reply(invocation, "started")
                waiting.set()
                await release.wait()
            await self.reply(invocation, value={"correlation": invocation["correlation"]})
        self.owner.on_invoke = handler
        slow = await self.invoke("slow")
        await asyncio.wait_for(waiting.wait(), 2)
        cursor = (await self.snapshot(self.owner))["cursor"]
        await self.observe(41)
        fast = await self.invoke("fast")
        self.assertEqual((await self.wait_status(fast["id"]))["status"], "completed")
        _, records = await self.catch_up(cursor)
        self.assertTrue(any(r["kind"] == "fixture.unsolicited" and r["body"] == {"number": 41} for r in records))
        self.assertEqual((await self.wait_status(slow["id"], {"started"}))["status"], "started")
        release.set()
        self.assertEqual((await self.wait_status(slow["id"]))["correlation"], "slow")

    async def test_cancel_request_does_not_prejudge_completion_race(self):
        invocation = await self.invoke()
        cancel = await self.requester.call("cancel", dict(invocationId=invocation["id"], requester=reference(self.caller)))
        self.assertNotEqual(cancel["status"], "cancelled")
        self.assertEqual((await asyncio.wait_for(self.cancels.get(), 2))["id"], invocation["id"])
        first = await self.reply(invocation, value="completion won")
        late = await self.reply(invocation, "cancelled")
        self.assertEqual(first["recordId"], late["recordId"])
        self.assertEqual(late["status"], "completed")

    async def test_acknowledged_cancellation_is_terminal(self):
        invocation = await self.invoke()
        await self.requester.call("cancel", dict(invocationId=invocation["id"], requester=reference(self.caller)))
        await asyncio.wait_for(self.cancels.get(), 2)
        first = await self.reply(invocation, "cancelled")
        late = await self.reply(invocation, value="late")
        self.assertEqual(late["status"], "cancelled")
        self.assertEqual(first["recordId"], late["recordId"])

    async def test_new_incarnation_cannot_retarget_pending_with_reused_pid(self):
        await self.owner.call("update", reference(self.host) | {"patch": {"metadata": {"pid": 4242}}})
        invocation = await self.invoke()
        await asyncio.wait_for(self.incoming.get(), 2)
        old = self.host
        self.host = registration("host", "incarnation-2", metadata={"pid": 4242})
        await self.owner.call("register", {"registration": self.host})
        terminal = await self.wait_status(invocation["id"])
        self.assertEqual(terminal["status"], "outcome-unknown")
        self.assertEqual(terminal["target"], reference(old))
        with self.assertRaises(ProtocolError):
            await self.invoke("stale", target=reference(old))
        fresh = await self.invoke("fresh")
        self.assertEqual(fresh["target"], reference(self.host))
        self.assertEqual((await asyncio.wait_for(self.incoming.get(), 2))["id"], fresh["id"])

    async def test_wrong_incarnation_reply_cannot_resolve(self):
        invocation = await self.invoke()
        with self.assertRaises(ProtocolError):
            await self.owner.call("result", dict(invocationId=invocation["id"], target={"id": "host", "incarnation": "old-document"}, status="completed"))
        item = next(i for i in (await self.snapshot())["invocations"] if i["id"] == invocation["id"])
        self.assertNotIn(item["status"], TERMINAL)

    async def test_disconnect_is_unknown_presence_and_reconnect_never_retries(self):
        invocation = await self.invoke(timeoutMs=100)
        await asyncio.wait_for(self.incoming.get(), 2)
        await self.owner.close()
        self.assertEqual((await self.wait_status(invocation["id"]))["status"], "outcome-unknown")
        host = next(r for r in (await self.snapshot())["registrations"] if r["id"] == "host")
        self.assertTrue(host["active"])
        self.assertEqual(host["presence"], "unknown")
        self.owner = await self.connect()
        self.owner.on_invoke = self.incoming.put
        await self.owner.call("register", {"registration": self.host, "resumeToken": self.host_receipt["resumeToken"]})
        repeated = await self.invoke(timeoutMs=100)
        self.assertEqual(repeated["id"], invocation["id"])
        self.assertEqual(repeated["status"], "outcome-unknown")
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.incoming.get(), 0.05)

    async def test_provider_can_close_from_its_own_callback_without_self_join(self):
        closed = asyncio.Event()
        async def handler(_invocation):
            await self.owner.close()
            closed.set()
        self.owner.on_invoke = handler
        invocation = await self.invoke()
        await asyncio.wait_for(closed.wait(), 2)
        terminal = await self.wait_status(invocation["id"])
        self.assertEqual(terminal["status"], "outcome-unknown")
        host = next(r for r in (await self.snapshot())["registrations"] if r["id"] == "host")
        self.assertEqual(host["presence"], "unknown")
        self.assertTrue(host["active"])

    async def test_explicit_unregister_ends_binding_and_settles_pending(self):
        invocation = await self.invoke()
        await self.owner.call("unregister", reference(self.host) | {"reason": "detach"})
        self.assertEqual((await self.wait_status(invocation["id"]))["status"], "outcome-unknown")
        host = next(r for r in (await self.snapshot())["registrations"] if r["id"] == "host")
        self.assertFalse(host["active"])
        self.assertEqual(host["presence"], "ended")
        with self.assertRaises(ProtocolError):
            await self.invoke("after-detach")

    async def test_deadline_retains_unknown_instead_of_losing_pending(self):
        invocation = await self.invoke(timeoutMs=50)
        terminal = await self.wait_status(invocation["id"])
        self.assertEqual(terminal["status"], "outcome-unknown")
        self.assertEqual(terminal["correlation"], invocation["correlation"])
        late = await self.reply(invocation, value="late")
        self.assertEqual(late["status"], "outcome-unknown")
        self.assertEqual(late["recordId"], terminal["recordId"])

    async def test_snapshot_cursor_catches_race_without_inventing_causality(self):
        cursor = (await self.snapshot())["cursor"]
        for number in range(20):
            await self.observe(number)
        _, records = await self.catch_up(cursor, limit=3)
        observations = [r for r in records if r["kind"] == "fixture.unsolicited"]
        self.assertEqual([r["body"]["number"] for r in observations], list(range(20)))
        sequences = [int(r["seq"]) for r in observations]
        self.assertEqual(sequences, sorted(set(sequences)))
        for record in observations:
            self.assertEqual(record["schema"], "nema.lab.record/v1")
            self.assertEqual(record["source"], "host")
            self.assertEqual(record["incarnation"], self.host["incarnation"])
            self.assertEqual(record["parents"], [])

    async def test_stalled_polling_reader_can_catch_up(self):
        cursor = (await self.snapshot())["cursor"]
        await self.observer.close()
        for number in range(75):
            await self.observe(number)
        self.assertGreater(int((await self.snapshot(self.requester))["cursor"]), int(cursor))
        self.observer = await self.connect()
        final_cursor, records = await self.catch_up(cursor, limit=4)
        self.assertEqual([r["body"]["number"] for r in records if r["kind"] == "fixture.unsolicited"], list(range(75)))
        self.assertEqual(len(records), len({r["id"] for r in records}))
        _, later = await self.catch_up(final_cursor)
        self.assertFalse(any(r["kind"] == "fixture.unsolicited" for r in later))

    async def test_restart_retains_token_cursor_and_uncertain_invocation(self):
        invocation = await self.invoke()
        await asyncio.wait_for(self.incoming.get(), 2)
        await self.observe(1)
        cursor = (await self.snapshot())["cursor"]
        for client in self.clients:
            await client.close()
        self.clients = []
        await self.server.close()
        self.server = await Server(Path(self.temp.name)).start()
        self.owner, self.observer = await self.connect(), await self.connect()
        self.owner.on_invoke = self.incoming.put
        snapshot = await self.snapshot()
        recovered = next(i for i in snapshot["invocations"] if i["id"] == invocation["id"])
        self.assertEqual(recovered["status"], "outcome-unknown")
        self.assertEqual(recovered["correlation"], invocation["correlation"])
        host = next(r for r in snapshot["registrations"] if r["id"] == "host")
        self.assertEqual(host["presence"], "unknown")
        receipt = await self.owner.call("register", {"registration": self.host, "resumeToken": self.host_receipt["resumeToken"]})
        self.assertEqual(receipt["resumeToken"], self.host_receipt["resumeToken"])
        await self.observe(2)
        _, later = await self.catch_up(cursor)
        self.assertTrue(any(r["kind"] == "fixture.unsolicited" and r["body"] == {"number": 2} for r in later))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.incoming.get(), 0.05)

    async def test_invalid_and_oversized_frames_are_isolated(self):
        frames = [
            b'{"version":2,"id":"bad","method":"inspect","params":{}}\n',
            b'{"version":1,"id":"bad","method":"inspect","params":[]}\n',
            b'{"version":1,"id":"bad","id":"ambiguous","method":"inspect","params":{}}\n',
            b'{"version":1,"id":"bad","method":"inspect","params":{"bad":NaN}}\n',
            b'\xff\xfe\n', b'{' + b' ' * 65536 + b'}\n',
        ]
        for frame in frames:
            with self.subTest(size=len(frame)):
                reader, writer = await asyncio.open_unix_connection(self.server.socket_path, limit=65536)
                try:
                    writer.write(frame)
                    await asyncio.wait_for(writer.drain(), 2)
                    reply = await asyncio.wait_for(reader.readline(), 2)
                    if reply:
                        self.assertLessEqual(len(reply), 65536)
                        self.assertIn("error", json.loads(reply))
                    self.assertIn("registrations", await self.snapshot())
                finally:
                    writer.close()
                    with contextlib.suppress(ConnectionError):
                        await writer.wait_closed()

    async def test_partial_frame_and_unread_responses_do_not_block_control(self):
        await self.owner.call("update", reference(self.host) | {"patch": {"metadata": {"padding": "x" * 12000}}})
        _, partial = await asyncio.open_unix_connection(self.server.socket_path)
        _, stalled = await asyncio.open_unix_connection(self.server.socket_path)
        try:
            partial.write(b'{"version":1,"id":"partial"')
            await partial.drain()
            stalled.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            stalled.write(b''.join(json.dumps(dict(version=1, id=str(i), method="inspect", params={})).encode() + b'\n' for i in range(128)))
            await asyncio.wait_for(stalled.drain(), 2)
            await self.observe(900)
            self.assertIn("registrations", await self.snapshot())
        finally:
            partial.close()
            stalled.close()
            await asyncio.gather(partial.wait_closed(), stalled.wait_closed(), return_exceptions=True)

    async def test_oversized_snapshot_fails_and_records_stay_pageable(self):
        for i in range(8):
            await self.owner.call("register", {"registration": registration(f"large-{i}", metadata={"payload": "x" * 9000})})
        with self.assertRaises(ProtocolError):
            await asyncio.wait_for(self.observer.call("inspect", {}), 2)
        _, records = await self.catch_up(limit=1)
        self.assertTrue(records)
        for record in records:
            self.assertLess(len(json.dumps(record).encode()), 65536)

    async def test_sigkill_preserves_accepted_intent_as_unknown(self):
        runtime = Path(self.temp.name) / "crash"
        code = ("import asyncio,sys\nfrom zygon.server import Server\nasync def main():\n"
                " server=await Server(sys.argv[1]).start()\n print(str(server.socket_path),flush=True)\n"
                " await asyncio.Event().wait()\nasyncio.run(main())\n")
        process = await asyncio.create_subprocess_exec(sys.executable, "-u", "-c", code, str(runtime),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        clients, recovered_server = [], None
        try:
            path = (await asyncio.wait_for(process.stdout.readline(), 5)).decode().strip()
            if not path:
                self.fail("server subprocess failed: " + (await process.stderr.read()).decode())
            provider, caller = await Client.connect(path), await Client.connect(path)
            clients.extend((provider, caller))
            offered = asyncio.Queue()
            provider.on_invoke = offered.put
            receipt = await provider.call("register", {"registration": self.host})
            await caller.call("register", {"registration": self.caller})
            invocation = await caller.call("invoke", dict(requester=reference(self.caller), target=reference(self.host),
                operation="fixture.echo", arguments={"nonIdempotent": True}, correlation="durable-before-kill"))
            self.assertEqual((await asyncio.wait_for(offered.get(), 2))["id"], invocation["id"])
            self.assertEqual(invocation["status"], "accepted")
            process.kill()
            await asyncio.wait_for(process.wait(), 5)
            for client in clients:
                await client.close()
            clients.clear()
            recovered_server = await Server(runtime).start()
            recovered = await Client.connect(recovered_server.socket_path)
            clients.append(recovered)
            replayed = asyncio.Queue()
            recovered.on_invoke = replayed.put
            snapshot = await recovered.call("inspect", {})
            intent = next(i for i in snapshot["invocations"] if i["id"] == invocation["id"])
            self.assertEqual(intent["status"], "outcome-unknown")
            self.assertEqual(intent["correlation"], "durable-before-kill")
            await recovered.call("register", {"registration": self.host, "resumeToken": receipt["resumeToken"]})
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(replayed.get(), 0.05)
        finally:
            for client in clients:
                with contextlib.suppress(Exception):
                    await client.close()
            if recovered_server:
                await recovered_server.close()
            if process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.wait(), 5)


if __name__ == "__main__":
    unittest.main()
