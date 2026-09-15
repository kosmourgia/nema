"""Real isolated Chromium/MV3 acceptance harness, using inherited DevTools pipes."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import re
from pathlib import Path
import shutil
import signal
import sys
import time
import uuid

from .browser import BrowserBridge
from .client import Client
from .protocol import ProtocolError


class ChromiumPipe:
    """Test-only CDP codec: our child has no DevTools TCP listener."""
    async def start(self, executable, profile, extension, log_path, *, headful=False):
        self.closed = False
        command_read, self.command_write = os.pipe()
        response_read, response_write = os.pipe()
        command = [executable, "--user-data-dir=" + str(profile), "--no-first-run", "--no-default-browser-check",
            "--disable-background-networking", "--disable-sync", "--disable-component-update", "--disable-dev-shm-usage",
            "--remote-debugging-pipe", "--enable-unsafe-extension-debugging",
            "--disable-extensions-except=" + str(extension), "--load-extension=" + str(extension), "about:blank"]
        if not headful:
            command.insert(1, "--headless=new")
        if os.geteuid() == 0:
            command.insert(1, "--no-sandbox")
        self.log = open(log_path, "ab", buffering=0)
        # Duplicate above fd 64 before remapping: dup() can itself return 3/4.
        # Touching fd 3/4 in the asyncio parent would corrupt its event loop.
        wrapper = ("import os,sys,fcntl; r=fcntl.fcntl(int(sys.argv[1]),fcntl.F_DUPFD,64); "
            "w=fcntl.fcntl(int(sys.argv[2]),fcntl.F_DUPFD,64); os.dup2(r,3,inheritable=True); "
            "os.dup2(w,4,inheritable=True); os.close(r); os.close(w); os.execv(sys.argv[3],sys.argv[3:])")
        self.process = await asyncio.create_subprocess_exec(sys.executable, "-c", wrapper,
            str(command_read), str(response_write), *command, pass_fds=(command_read, response_write),
            stdout=self.log, stderr=self.log, start_new_session=True)
        os.close(command_read)
        os.close(response_write)
        self.pending, self.sequence = {}, 0
        self.reader = asyncio.StreamReader(limit=1024 * 1024)
        self.transport, _ = await asyncio.get_running_loop().connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(self.reader), os.fdopen(response_read, "rb", buffering=0))
        self.task = asyncio.create_task(self.read())
        return self

    async def read(self):
        try:
            while True:
                message = json.loads((await self.reader.readuntil(b"\0"))[:-1])
                waiter = self.pending.pop(message.get("id"), None)
                if waiter and not waiter.done():
                    if "error" in message:
                        waiter.set_exception(RuntimeError(str(message["error"])))
                    else:
                        waiter.set_result(message.get("result", {}))
        except (asyncio.IncompleteReadError, asyncio.CancelledError) as error:
            for waiter in self.pending.values():
                if not waiter.done():
                    waiter.set_exception(ConnectionError("Chromium debug pipe closed: " + type(error).__name__))

    async def call(self, method, params=None, *, session_id=None):
        self.sequence += 1
        message = {"id": self.sequence, "method": method, "params": params or {}}
        if session_id:
            message["sessionId"] = session_id
        future = asyncio.get_running_loop().create_future()
        self.pending[self.sequence] = future
        await asyncio.to_thread(os.write, self.command_write, json.dumps(message).encode() + b"\0")
        return await asyncio.wait_for(future, 20)

    async def close(self):
        if self.closed:
            return
        self.closed = True
        if self.process.returncode is None:
            with contextlib.suppress(Exception):
                await self.call("Browser.close")
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except asyncio.TimeoutError:
                os.killpg(self.process.pid, signal.SIGTERM)
                try:
                    await asyncio.wait_for(self.process.wait(), 3)
                except asyncio.TimeoutError:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    await self.process.wait()
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        self.transport.close()
        os.close(self.command_write)
        self.log.close()


async def eventually(client, predicate, *, timeout=20):
    deadline, snapshot = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        snapshot = await client.call("inspect", {})
        value = predicate(snapshot)
        if value:
            return value
        await asyncio.sleep(0.1)
    raise AssertionError("browser condition timed out; snapshot=" + json.dumps(snapshot))


def ref(registration):
    return {"id": registration["id"], "incarnation": registration["incarnation"]}


async def run(args):
    executable = shutil.which(args.chromium)
    if not executable:
        raise RuntimeError(f"real browser test blocked: executable {args.chromium!r} is missing")
    runtime = Path(args.runtime).resolve()
    runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
    server = None
    if not args.socket:
        from .server import Server
        server = await Server(runtime / "registry").start()
        args.socket = server.socket_path
    bridge = await BrowserBridge(args.socket, runtime / "bridge").start()
    browser = None
    client = await Client.connect(args.socket)
    observer = await Client.connect(args.socket)
    requester = {"id": "browser-demo:" + uuid.uuid4().hex, "incarnation": uuid.uuid4().hex,
        "kind": "client", "medium": "terminal", "mode": "observer", "parent": None, "operations": []}
    await client.call("register", {"registration": requester})
    output = []

    def note(name, **fields):
        item = {"check": name, **fields}
        output.append(item)
        print(json.dumps(item, sort_keys=True), flush=True)

    async def invoke(target, operation, arguments):
        invocation = await client.call("invoke", {"requester": ref(requester), "target": ref(target),
            "operation": operation, "arguments": arguments, "correlation": uuid.uuid4().hex, "timeoutMs": 10000})
        return await eventually(client, lambda snapshot: next((i for i in snapshot["invocations"] if i["id"] == invocation["id"] and
            i["status"] in {"completed", "failed", "cancelled", "outcome-unknown"}), None))

    def document(snapshot, *, exclude=None):
        return next((r for r in snapshot["registrations"] if r["kind"] == "document" and r.get("active", True)
            and r.get("presence") == "online" and r["incarnation"] != exclude), None)

    try:
        browser = await ChromiumPipe().start(executable, runtime / "profile", bridge.runtime / "extension",
            runtime / "chromium.log", headful=args.headful)
        version = await browser.call("Browser.getVersion")
        note("real-chromium-started", product=version["product"], transport="inherited-debug-pipe", profile=str(runtime / "profile"))
        target = await browser.call("Target.createTarget", {"url": bridge.origin + "/demo"})
        first = await eventually(client, document)
        snapshot = await client.call("inspect", {})
        tab = next(r for r in snapshot["registrations"] if r["id"] == first["parent"]["id"])
        extension = next(r for r in snapshot["registrations"] if r["id"] == tab["parent"]["id"])
        assert extension["kind"] == "extension" and tab["kind"] == "tab"
        note("extension-tab-document-registered", extension=ref(extension), tab=ref(tab), document=ref(first))
        assert (await invoke(extension, "browser.enumerate", {}))["status"] == "completed"
        result = await invoke(first, "browser.annotate", {"value": "addressed by terminal"})
        assert result["status"] == "completed" and result["value"]["annotation"] == "addressed by terminal", result
        observed = next(i for i in (await observer.call("inspect", {}))["invocations"] if i["id"] == result["id"])
        assert observed["recordId"] == result["recordId"]
        note("annotation-round-trip-shared-transition", status=result["status"], invocationId=result["id"], recordId=result["recordId"], annotation=result["value"]["annotation"])
        assert (await invoke(first, "browser.replace", {"value": "an individually addressable page"}))["status"] == "completed"
        assert (await invoke(first, "browser.read", {}))["value"]["text"] == "an individually addressable page"
        focused = await invoke(tab, "browser.focus", {})
        note("read-replace-focus", read="completed", replace="completed", focus=focused["status"])
        if args.with_flix:
            # This is an actual Flix process joined to the same Unix registry.
            launcher = Path(__file__).resolve().parents[1] / "flix" / "run"
            child_env = dict(os.environ, ZYGON_SOCKET=str(args.socket), ZYGON_TARGET_ID=first["id"],
                ZYGON_TARGET_INCARNATION=first["incarnation"], ZYGON_OPERATION="browser.annotate",
                ZYGON_ARGUMENTS='{"value":"addressed by Flix"}', ZYGON_REQUIRE_OBSERVATION="0")
            process = await asyncio.create_subprocess_exec(str(launcher), env=child_env,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, start_new_session=True)
            try:
                stdout, _ = await asyncio.wait_for(process.communicate(), 180)
            finally:
                if process.returncode is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    await process.wait()
            print(stdout.decode(errors="replace"), end="", flush=True)
            if process.returncode:
                raise AssertionError("actual Flix participant failed")
            match = re.search(r"FLIX resolved id=(\S+).*recordId=(\S+)", stdout.decode(errors="replace"))
            if not match:
                raise AssertionError("Flix did not report its retained terminal record")
            shared = next(i for i in (await observer.call("inspect", {}))["invocations"] if i["id"] == match.group(1))
            assert shared["recordId"] == match.group(2), shared
            assert (await invoke(first, "browser.read", {}))["value"]["annotation"] == "addressed by Flix"
            note("flix-targeted-real-document", completed=True, invocationId=shared["id"], recordId=shared["recordId"])

        workers = await browser.call("Target.getTargets")
        worker = next(t for t in workers["targetInfos"] if t["type"] == "service_worker" and t["url"].endswith("/worker.js"))
        await browser.call("Target.closeTarget", {"targetId": worker["targetId"]})
        await asyncio.sleep(2.5)
        assert (await invoke(first, "browser.read", {}))["status"] == "completed"
        note("worker-reconnected", documentContinuity=True)

        bridge.server.close()
        await bridge.server.wait_closed()
        await asyncio.sleep(bridge.presence_timeout + 0.7)
        await eventually(client, lambda s: next((r for r in s["registrations"] if r["id"] == first["id"] and r["presence"] == "unknown"), None))
        bridge.server = await asyncio.start_server(bridge.http, "127.0.0.1", bridge.state["port"], limit=65536)
        await eventually(client, lambda s: document(s) if document(s) and document(s)["incarnation"] == first["incarnation"] else None)
        assert (await invoke(first, "browser.read", {}))["status"] == "completed"
        note("transport-loss-reconnect", preservedDocumentIncarnation=True)

        await bridge.close()
        bridge = await BrowserBridge(args.socket, runtime / "bridge").start()
        await eventually(client, lambda s: document(s) if document(s) and document(s)["incarnation"] == first["incarnation"] else None)
        assert (await invoke(first, "browser.read", {}))["status"] == "completed"
        note("bridge-restart-same-browser", preservedStableId=True, preservedDocumentIncarnation=True)

        session = await browser.call("Target.attachToTarget", {"targetId": target["targetId"], "flatten": True})
        await browser.call("Page.navigate", {"url": bridge.origin + "/demo?navigation=second"}, session_id=session["sessionId"])
        second = await eventually(client, lambda s: document(s, exclude=first["incarnation"]))
        assert first["id"] == second["id"] and first["incarnation"] != second["incarnation"]
        try:
            stale = await invoke(first, "browser.annotate", {"value": "must not reach replacement"})
            assert stale["status"] != "completed", stale
            stale_disposition = stale["status"]
        except ProtocolError as error:
            assert error.code == "stale-incarnation", error
            stale_disposition = str(error)
        assert (await invoke(second, "browser.read", {}))["value"]["annotation"] == ""
        note("navigation-rejects-stale-document", stableSurface=first["id"], oldIncarnation=first["incarnation"], newIncarnation=second["incarnation"], stale=stale_disposition)
        await browser.call("Target.closeTarget", {"targetId": target["targetId"]})
        await eventually(client, lambda s: not any(r["id"] == second["id"] and r.get("active", True) for r in s["registrations"]))
        reopened_target = await browser.call("Target.createTarget", {"url": bridge.origin + "/demo?reopened=yes"})
        reopened = await eventually(client, document)
        assert reopened["id"] != second["id"] and reopened["incarnation"] != second["incarnation"]
        note("tab-close-reopen", newSurface=ref(reopened))

        # Same profile establishes installation continuity, never session/tab
        # continuity. Chromium's session storage clears across this full restart.
        await browser.close()
        browser = await ChromiumPipe().start(executable, runtime / "profile", bridge.runtime / "extension",
            runtime / "chromium.log", headful=args.headful)
        restarted_target = await browser.call("Target.createTarget", {"url": bridge.origin + "/demo?restart=yes"})
        restarted = await eventually(client, lambda s: document(s, exclude=reopened["incarnation"]))
        after = await client.call("inspect", {})
        extension_after = next(r for r in after["registrations"] if r["id"] == extension["id"])
        assert extension_after["incarnation"] != extension["incarnation"] and restarted["id"] != reopened["id"]
        note("browser-restart", stableInstallationId=extension_after["id"], newSessionIncarnation=extension_after["incarnation"])
        await bridge.close()
        assert any(t["targetId"] == restarted_target["targetId"] for t in (await browser.call("Target.getTargets"))["targetInfos"])
        note("detach-preserves-browser", browserStillRunning=browser.process.returncode is None)
        assert not any(i["status"] in {"accepted", "started"} for i in (await client.call("inspect", {}))["invocations"] if i["requester"] == ref(requester))
        note("all-browser-demo-invocations-terminal", passed=True)
    finally:
        await bridge.close()
        if browser:
            await browser.close()
        await client.close()
        await observer.close()
        if server:
            await server.close()
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", help="Existing registry; omitted starts an isolated registry under --runtime")
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--chromium", default="chromium")
    parser.add_argument("--headful", action="store_true")
    parser.add_argument("--with-flix", action="store_true")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
