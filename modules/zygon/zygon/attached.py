"""Explicit cooperative attachment; the external host keeps its own lifetime.

Use a dedicated registry Client. Detach closes that client so presence becomes
unknown, preserving the still-existing host's incarnation for later reattach.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path
import signal
import uuid

from zygon.processes import MAX_FRAME, CooperativeChannel, RawSpool, private_directory, save_private


class AttachedCompanion:
    @classmethod
    async def attach(cls, client, endpoint_path, runtime_dir):
        self = cls()
        self.client, self.runtime_dir = client, private_directory(Path(runtime_dir))
        self.endpoint_path = Path(endpoint_path)
        self.spool = RawSpool(self.runtime_dir / ("attached-" + uuid.uuid4().hex + ".sqlite"),
                              "attachment-" + uuid.uuid4().hex, "connection-" + uuid.uuid4().hex)
        self.operations = {}
        self.publisher = self.monitor = None
        self.closed = False
        self.publish_cursor, self.publish_error = 0, None
        self.state_path = self.runtime_dir / "attachment.json"
        self.tokens = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        reader, writer = await asyncio.open_unix_connection(str(endpoint_path), limit=MAX_FRAME)
        self.native = CooperativeChannel(reader, writer, self._capture)
        try:
            response = await self.native.request("hello", timeout=2)
            if response["status"] != "completed":
                raise ValueError("cooperative endpoint refused discovery")
            hello = response["value"]
            self.ref = {"id": hello["id"], "incarnation": hello["incarnation"]}
            self.incarnation = hello["incarnation"]
            self.spool.source, self.spool.incarnation = self.ref["id"], self.ref["incarnation"]
            operations = [op for op in hello["operations"] if op in {"demo.echo", "demo.delay", "demo.read"}]
            self.registration = {**self.ref, "kind": "host", "medium": "cooperative-unix-socket", "mode": "attached",
                "operations": [], "parent": None, "binding": {"endpoint": str(endpoint_path), "pid": hello.get("pid"),
                "linuxStartTicks": hello.get("linuxStartTicks")}, "metadata": {"sourceStdoutAvailable": False,
                "historicalOutput": None, "hostOwnsLifetime": True, "nativeHello": hello}}
            self.surfaces = [
                {"id": hello["id"] + "/requests", "incarnation": hello["incarnation"] + "/requests",
                 "kind": "surface", "medium": "cooperative-unix-socket", "mode": "attached",
                 "operations": operations, "parent": self.ref},
                {"id": hello["id"] + "/output", "incarnation": hello["incarnation"] + "/output",
                 "kind": "surface", "medium": "cooperative-observations", "mode": "attached", "operations": [],
                 "parent": self.ref, "metadata": {"sourceStdoutAvailable": False, "observedSinceAttachment": True}}]
            client.on_invoke, client.on_cancel = self._invoke, self._cancel
            await self._register()
            self.publisher = asyncio.create_task(self._publish())
            self.monitor = asyncio.create_task(self._monitor())
            return self
        except BaseException:
            await self.native.close()
            await self.client.close()
            self.spool.close()
            raise

    async def _register(self):
        for registration in [self.registration, *self.surfaces]:
            arguments = {"registration": registration}
            if registration["id"] in self.tokens:
                arguments["resumeToken"] = self.tokens[registration["id"]]
            receipt = await self.client.call("register", arguments)
            self.tokens[registration["id"]] = receipt["resumeToken"]
        save_private(self.state_path, self.tokens)

    async def reconnect(self, client):
        if self.native.task.done() or self.closed:
            raise ConnectionError("endpoint binding lost; detach and discover endpoint identity again")
        self.publisher.cancel()
        await asyncio.gather(self.publisher, return_exceptions=True)
        self.client = client
        client.on_invoke, client.on_cancel = self._invoke, self._cancel
        await self._register()
        self.publish_error = None
        self.publisher = asyncio.create_task(self._publish())

    def _capture(self, stream, payload):
        for offset in range(0, len(payload), 16384):
            self.spool.chunk(stream, payload[offset:offset + 16384])

    async def _publish(self):
        try:
            while not self.closed:
                for cursor, record in self.spool.page(self.publish_cursor, 16):
                    await self.client.call("observe", {"source": self.ref["id"], "incarnation": self.incarnation,
                        "kind": record["kind"], "parents": [], "body": {**record["body"],
                        "captureRecordId": record["id"], "captureSource": record["source"],
                        "captureIncarnation": record["incarnation"], "captureSequence": record["seq"]}})
                    self.publish_cursor = cursor
                await asyncio.sleep(0.025)
        except Exception as error:
            self.publish_error = str(error)

    async def _monitor(self):
        await self.native.task
        if not self.closed:
            # This connection is no longer a usable provider for any child. A
            # registry disconnect settles pending calls uncertain and marks the
            # host and child surfaces unknown, without asserting host death.
            await self.client.close()

    async def _reply(self, invocation, status, value=None):
        return await self.client.call("result", {"invocationId": invocation["id"],
            "target": invocation["target"], "status": status, "value": value})

    async def _invoke(self, invocation):
        if invocation["id"] in self.operations:
            return
        if len(self.operations) >= 32:
            await self._reply(invocation, "failed", {"reason": "attached operation concurrency limit (32)"})
            return
        task = asyncio.create_task(self._run(invocation))
        self.operations[invocation["id"]] = task
        task.add_done_callback(lambda _: self.operations.pop(invocation["id"], None))

    async def _run(self, invocation):
        try:
            target = self.surfaces[0]
            if invocation["target"] != {"id": target["id"], "incarnation": target["incarnation"]}:
                raise ValueError("stale or unsupported attached surface")
            if invocation["operation"] not in target["operations"]:
                raise ValueError("operation not exposed by attached host")
            await self._reply(invocation, "started")
            response = await self.native.request(invocation["operation"], invocation["arguments"], correlation=invocation["id"])
            await self._reply(invocation, response["status"], response.get("value"))
        except (ConnectionError, TimeoutError) as error:
            with contextlib.suppress(Exception):
                await self._reply(invocation, "outcome-unknown", {"reason": str(error)})
        except Exception as error:
            with contextlib.suppress(Exception):
                await self._reply(invocation, "failed", {"reason": str(error)})

    async def _cancel(self, invocation):
        if invocation["id"] in self.operations:
            await self.native.cancel(invocation["id"])

    async def detach(self):
        if self.closed:
            return
        self.closed = True
        await self.native.close()
        if self.operations:
            await asyncio.gather(*list(self.operations.values()), return_exceptions=True)
        if self.publisher:
            self.publisher.cancel()
            await asyncio.gather(self.publisher, return_exceptions=True)
        if self.monitor:
            await asyncio.gather(self.monitor, return_exceptions=True)
        # Do not unregister: the external host did not cease to exist. The
        # dedicated connection's loss leaves its retained bindings unknown.
        await self.client.close()
        self.spool.close()

    close = detach


async def _main_async(args):
    from zygon.client import Client
    client = await Client.connect(args.socket)
    attached = await AttachedCompanion.attach(client, args.endpoint, args.runtime_dir)
    print(json.dumps({"host": attached.ref, "surfaces": attached.surfaces,
                      "attached": True, "sourceStdoutAvailable": False}), flush=True)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    try:
        await stopped.wait()
    finally:
        await attached.detach()


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--endpoint", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    asyncio.run(_main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
