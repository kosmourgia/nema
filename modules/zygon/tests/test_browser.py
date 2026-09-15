"""Real Unix/HTTP protocol tests; page payloads here are explicit fixtures.

Run the independent demo_browser harness for real Chromium extension evidence.
"""
import asyncio
import base64
import copy
import json
from pathlib import Path
import tempfile
import unittest
import uuid

from zygon.browser import BrowserBridge, MAX_FRAME
from zygon.client import Client
from zygon.server import Server


class BrowserProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        Path(".nema").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="bu-", dir=".nema")
        self.runtime = Path(self.temp.name).resolve()
        self.server = await Server(self.runtime / "r").start()
        self.bridge = await BrowserBridge(self.server.socket_path, self.runtime / "b").start()
        self.client = await Client.connect(self.server.socket_path)
        self.requester = {"id": "browser-test", "incarnation": "client-one", "kind": "client", "medium": "test",
                          "mode": "observer", "parent": None, "operations": []}
        await self.client.call("register", {"registration": self.requester})
        self.message = {"version": 1, "installation": "installation-one", "session": "session-one", "extensionId": "a" * 32,
            "tabs": [{"key": "tab-inc-one", "tabId": 7, "windowId": 1,
                      "document": {"id": "document-one", "nativeDocumentId": "native-document-one", "url": self.bridge.origin + "/demo"}}], "results": []}

    async def asyncTearDown(self):
        await self.bridge.close()
        await self.client.close()
        await asyncio.wait_for(self.server.close(), 3)
        self.temp.cleanup()

    async def http(self, payload=None, *, token=True, origin=None, host=None, length=None):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.bridge.state["port"])
        raw = json.dumps(payload or self.message).encode()
        headers = ["POST /exchange HTTP/1.1", "Host: " + (host or f'127.0.0.1:{self.bridge.state["port"]}'),
                   "Content-Length: " + str(len(raw) if length is None else length), "Content-Type: application/json"]
        if token:
            headers.append("Authorization: Bearer " + self.bridge.state["token"])
        if origin:
            headers.append("Origin: " + origin)
        writer.write(("\r\n".join(headers) + "\r\n\r\n").encode() + raw)
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), 5)
        writer.close()
        await writer.wait_closed()
        header, body = response.split(b"\r\n\r\n", 1)
        return int(header.split(b" ")[1]), json.loads(body)

    async def doc(self):
        return next(r for r in (await self.client.call("inspect", {}))["registrations"] if r["kind"] == "document")

    async def invoke(self, arguments=None, *, timeout=10000):
        doc = await self.doc()
        invocation = await self.client.call("invoke", {"requester": {k: self.requester[k] for k in ("id", "incarnation")},
            "target": {k: doc[k] for k in ("id", "incarnation")}, "operation": "browser.annotate",
            "arguments": arguments or {"value": "test"}, "correlation": uuid.uuid4().hex, "timeoutMs": timeout})
        for _ in range(100):
            if invocation["id"] in self.bridge.pending:
                return invocation
            await asyncio.sleep(.01)
        self.fail("provider never received invocation")

    async def test_authentication_origin_host_and_size_limits(self):
        self.assertEqual((await self.http(token=False))[0], 401)
        self.assertEqual((await self.http(origin="http://malicious.invalid"))[0], 403)
        self.assertEqual((await self.http(host="malicious.invalid"))[0], 403)
        self.assertEqual((await self.http(length=MAX_FRAME + 1))[0], 413)
        self.assertEqual((await self.http(origin="chrome-extension://" + "a" * 32))[0], 200)
        self.assertNotIn(self.bridge.state["token"], (self.bridge.runtime / "extension" / "page_config.js").read_text())
        self.assertEqual((self.bridge.runtime / "browser-state.json").stat().st_mode & 0o777, 0o600)

    async def test_nested_registration_duplicate_and_unknown_native_bytes(self):
        self.message["futureNativeMember"] = {"uninterpreted": [1, "retained"]}
        raw = json.dumps(self.message, indent=1).encode()
        await self.bridge.exchange(self.message, raw)
        first = await self.doc()
        await self.bridge.exchange(self.message)
        second = await self.doc()
        self.assertEqual(first["incarnation"], second["incarnation"])
        snapshot = await self.client.call("inspect", {})
        self.assertEqual(sorted(r["kind"] for r in snapshot["registrations"]), ["client", "document", "extension", "tab"])
        page = await self.client.call("subscribe", {"cursor": "0"})
        chunks = [r["body"] for r in page["records"] if r["kind"] == "browser.transport.body"]
        self.assertEqual(b"".join(base64.b64decode(c["bytesBase64"]) for c in chunks), raw)

    async def test_navigation_late_result_is_retained_without_poisoning_exchange(self):
        await self.bridge.exchange(self.message)
        pending = await self.invoke()
        old = await self.doc()
        self.message["tabs"][0]["document"]["id"] = "document-two"
        self.message["tabs"][0]["document"]["nativeDocumentId"] = "native-document-two"
        self.message["results"] = [{"invocationId": pending["id"], "status": "completed", "value": {"appliedTo": "old-document"}}]
        response = await self.bridge.exchange(self.message)
        self.assertIn(pending["id"], response["acknowledged"])
        snapshot = await self.client.call("inspect", {})
        result = next(i for i in snapshot["invocations"] if i["id"] == pending["id"])
        self.assertEqual(result["status"], "outcome-unknown")
        self.assertEqual((await self.doc())["id"], old["id"])
        page = await self.client.call("subscribe", {"cursor": "0"})
        self.assertTrue(any(r["kind"] == "browser.result-observed" and r["body"]["native"]["value"]["appliedTo"] == "old-document" for r in page["records"]))
        self.message["results"] = []
        self.assertEqual((await self.bridge.exchange(self.message))["commands"], response["commands"][:0])

    async def test_bridge_detach_reconnect_preserves_host_and_document(self):
        await self.bridge.exchange(self.message)
        original = await self.doc()
        await self.bridge.close()
        await asyncio.sleep(.05)
        self.assertEqual((await self.doc())["presence"], "unknown")
        self.bridge = await BrowserBridge(self.server.socket_path, self.runtime / "b").start()
        await self.bridge.exchange(self.message)
        new = await self.doc()
        self.assertEqual((new["id"], new["incarnation"], new["presence"]), (original["id"], original["incarnation"], "online"))

    async def test_registry_restart_reconnects_client_without_replaying_mutation(self):
        await self.bridge.exchange(self.message)
        invocation = await self.invoke()
        await asyncio.wait_for(self.server.close(), 3)
        await self.client.close()
        self.server = await Server(self.runtime / "r").start()
        self.client = await Client.connect(self.server.socket_path)
        response = await self.bridge.exchange(self.message)
        self.assertEqual(response["commands"], [])
        result = next(i for i in (await self.client.call("inspect", {}))["invocations"] if i["id"] == invocation["id"])
        self.assertEqual(result["status"], "outcome-unknown")
        self.assertEqual((await self.doc())["presence"], "online")

    async def test_queued_cancellation_and_at_most_once_dispatch(self):
        await self.bridge.exchange(self.message)
        pending = await self.invoke()
        await self.client.call("cancel", {"invocationId": pending["id"], "requester": {k: self.requester[k] for k in ("id", "incarnation")}})
        await asyncio.sleep(.05)
        self.assertEqual((await self.bridge.exchange(self.message))["commands"], [])
        statuses = {i["id"]: i["status"] for i in (await self.client.call("inspect", {}))["invocations"]}
        self.assertEqual(statuses[pending["id"]], "cancelled")
        pending = await self.invoke()
        self.assertEqual(len((await self.bridge.exchange(self.message))["commands"]), 1)
        self.assertEqual((await self.bridge.exchange(self.message))["commands"], [])
        # Duplicate replies are observed but cannot produce another terminal transition.
        self.message["results"] = [{"invocationId": pending["id"], "status": "completed", "value": {"annotation": "test"}}]
        await self.bridge.exchange(self.message)
        before = next(i for i in (await self.client.call("inspect", {}))["invocations"] if i["id"] == pending["id"])
        await self.bridge.exchange(self.message)
        after = next(i for i in (await self.client.call("inspect", {}))["invocations"] if i["id"] == pending["id"])
        self.assertEqual(after["recordId"], before["recordId"])

    async def test_command_batch_is_length_bounded_and_no_duplicate_dispatch(self):
        await self.bridge.exchange(self.message)
        doc = await self.doc()
        for _ in range(12):
            # Provider invocations are deliberately large enough that the
            # registry's bounded snapshot cannot be used during this test.
            invocation = await self.client.call("invoke", {"requester": {k: self.requester[k] for k in ("id", "incarnation")},
                "target": {k: doc[k] for k in ("id", "incarnation")}, "operation": "browser.annotate",
                "arguments": {"value": "x" * 8000}, "correlation": uuid.uuid4().hex, "timeoutMs": 10000})
            for _ in range(100):
                if invocation["id"] in self.bridge.pending:
                    break
                await asyncio.sleep(.01)
        first = await self.bridge.exchange(self.message)
        self.assertLessEqual(len(json.dumps(first, separators=(",", ":")).encode()), MAX_FRAME)
        self.assertGreater(len(first["commands"]), 0)
        self.assertLess(len(first["commands"]), 12)
        second = await self.bridge.exchange(self.message)
        ids = [c["invocationId"] for c in first["commands"] + second["commands"]]
        self.assertEqual(len(ids), 12)
        self.assertEqual(len(set(ids)), 12)

    async def test_partial_http_consumer_does_not_block_control(self):
        reader, stalled = await asyncio.open_connection("127.0.0.1", self.bridge.state["port"])
        stalled.write(b"POST /exchange HTTP/1.1\r\n")
        await stalled.drain()
        self.assertEqual((await asyncio.wait_for(self.http(), 1))[0], 200)
        stalled.close()
        await stalled.wait_closed()

    async def test_unselected_page_and_duplicate_tab_rejected(self):
        other = copy.deepcopy(self.message)
        other["tabs"][0]["document"]["url"] = "https://example.com/private"
        with self.assertRaises(ValueError):
            await self.bridge.exchange(other)
        duplicate = copy.deepcopy(self.message)
        duplicate["tabs"].append(duplicate["tabs"][0])
        with self.assertRaises(ValueError):
            await self.bridge.exchange(duplicate)
        await self.bridge.exchange(self.message)
        forged = copy.deepcopy(self.message)
        forged["tabs"][0]["document"]["nativeDocumentId"] = "different-native-document"
        with self.assertRaises(ValueError):
            await self.bridge.exchange(forged)


if __name__ == "__main__":
    unittest.main()
