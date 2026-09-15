"""Authenticated loopback bridge for one explicitly selected local-demo species."""
from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import time

from .client import Client
from .protocol import ProtocolError

MAX_FRAME = 65536
MAX_COMMANDS = 64
MAX_TABS = 32
PAGE = b'''<!doctype html><html><head><meta charset="utf-8"><title>Nema zygon demo</title>
<style>body{font:20px system-ui;max-width:50rem;margin:4rem auto;padding:1rem;background:#f4f3ed;color:#253534}
section{border:2px solid #698978;padding:2rem;border-radius:12px}small{color:#53645d}</style></head>
<body><h1>An ordinary page, with a companion</h1><p>This selected region is independently addressable.</p>
<section id="nema-demo-region" data-nema-annotation="">original demo value</section>
<p><small id="nema-zygon-status">Waiting for the unpacked extension.</small></p>
<p><a href="/demo?navigation=next">Navigate to a new document incarnation</a></p></body></html>'''


def identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value):
        raise ValueError("invalid browser identity")
    return value


def private_json(path, value):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with os.fdopen(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as stream:
        json.dump(value, stream, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


class BrowserBridge:
    def __init__(self, socket_path, runtime, *, presence_timeout=8.0):
        self.socket_path = str(socket_path)
        self.runtime = Path(runtime).resolve()
        self.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.runtime.chmod(0o700)
        state_path = self.runtime / "browser-state.json"
        self.state = json.loads(state_path.read_text()) if state_path.exists() else {
            "token": secrets.token_urlsafe(32), "tokens": {}, "registrations": {}, "port": 0}
        self.client = self.server = self.watchdog = None
        self.lock = asyncio.Lock()
        self.last_seen = 0.0
        self.presence_timeout = presence_timeout
        self.commands = []
        self.pending = {}
        self.http_tasks = set()

    def save(self):
        private_json(self.runtime / "browser-state.json", self.state)

    async def start(self):
        # An occupied saved port fails explicitly; silently changing it would
        # break the private configuration already installed in the extension.
        self.server = await asyncio.start_server(self.http, "127.0.0.1", self.state["port"], limit=MAX_FRAME)
        self.state["port"] = self.server.sockets[0].getsockname()[1]
        self.origin = f'http://127.0.0.1:{self.state["port"]}'
        self.save()
        extension = self.runtime / "extension"
        extension.mkdir(mode=0o700, exist_ok=True)
        for source in (Path(__file__).resolve().parents[1] / "extension").iterdir():
            if source.is_file():
                shutil.copyfile(source, extension / source.name)
        (extension / "config.js").write_text("globalThis.NEMA_CONFIG = " + json.dumps({"origin": self.origin, "token": self.state["token"]}) + ";\n")
        (extension / "config.js").chmod(0o600)
        (extension / "page_config.js").write_text("globalThis.NEMA_PAGE = " + json.dumps({"origin": self.origin}) + ";\n")
        self.watchdog = asyncio.create_task(self.presence_watch())
        return self

    async def ensure_client(self):
        if self.client and (self.client.reader_task.done() or self.client.writer.is_closing()):
            await self.client.close()
            self.client = None
            self.commands.clear()
            self.pending.clear()
        if self.client is None:
            self.last_seen = 0.0
            self.client = await Client.connect(self.socket_path)
            self.client.on_invoke = self.invoke
            self.client.on_cancel = self.cancel

    async def presence_watch(self):
        while True:
            await asyncio.sleep(0.5)
            async with self.lock:
                expired = {key for key, invocation in self.pending.items()
                           if int(invocation.get("deadlineNs", "0")) < time.monotonic_ns()}
                for key in expired:
                    self.pending.pop(key, None)
                self.commands = [c for c in self.commands if c["invocationId"] not in expired]
                if self.client and self.last_seen and time.monotonic() - self.last_seen > self.presence_timeout:
                    # Silence means uncertain presence. Closing settles any
                    # outstanding dispatches in the registry as outcome-unknown.
                    await self.client.close()
                    self.client = None
                    self.commands.clear()
                    self.pending.clear()

    async def register(self, registration):
        if self.state["registrations"].get(registration["id"]) == registration and self.last_seen:
            return
        params = {"registration": registration}
        if registration["id"] in self.state["tokens"]:
            params["resumeToken"] = self.state["tokens"][registration["id"]]
        reply = await self.client.call("register", params)
        self.state["tokens"][registration["id"]] = reply["resumeToken"]
        self.state["registrations"][registration["id"]] = registration
        self.save()

    async def unregister(self, registration):
        try:
            await self.client.call("unregister", {"id": registration["id"], "incarnation": registration["incarnation"], "reason": "browser-snapshot-removed"})
        except ProtocolError as error:
            if error.code not in {"stale-incarnation", "not-owner"}:
                raise
        self.state["registrations"].pop(registration["id"], None)
        self.save()

    def registrations(self, message):
        installation, session, extension_id = (identity(message.get(key)) for key in ("installation", "session", "extensionId"))
        tabs = message.get("tabs")
        if not isinstance(tabs, list) or len(tabs) > MAX_TABS:
            raise ValueError("at most 32 selected demo tabs")
        extension = {"id": "browser:" + installation, "incarnation": session,
                     "kind": "extension", "medium": "chromium.mv3", "mode": "attached", "parent": None,
                     "operations": ["browser.enumerate"], "metadata": {"extensionId": extension_id, "installation": installation}}
        result, seen = [extension], set()
        for tab in tabs:
            key, native_id = identity(tab.get("key")), tab.get("tabId")
            if isinstance(native_id, bool) or not isinstance(native_id, int) or native_id < 0 or native_id in seen:
                raise ValueError("invalid or duplicate native tab ID")
            seen.add(native_id)
            tab_id = f'{extension["id"]}:tab:{session}:{native_id}'
            tab_ref = {"id": tab_id, "incarnation": key}
            result.append({**tab_ref, "kind": "tab", "medium": "chromium.tab", "mode": "attached",
                           "parent": {"id": extension["id"], "incarnation": session}, "operations": ["browser.focus"],
                           "binding": {"nativeTabId": native_id, "nativeWindowId": tab.get("windowId")}, "metadata": {"native": tab}})
            document = tab.get("document")
            if document is None:
                continue
            document_id, native_document = identity(document.get("id")), identity(document.get("nativeDocumentId"))
            url = document.get("url")
            if not isinstance(url, str) or not (url == self.origin + "/demo" or url.startswith(self.origin + "/demo?")):
                raise ValueError("document is outside the explicitly selected local demo page")
            result.append({"id": tab_id + ":demo", "incarnation": document_id, "kind": "document",
                           "medium": "dom.demo-region", "mode": "attached", "parent": tab_ref,
                           "operations": ["browser.read", "browser.annotate", "browser.replace"],
                           "binding": {"nativeTabId": native_id, "nativeDocumentId": native_document, "region": "nema-demo-region"},
                           "metadata": {"native": document}})
        return result

    async def exchange(self, message, raw=None):
        if not isinstance(message, dict) or message.get("version") != 1:
            raise ValueError("browser protocol version 1 required")
        registrations = self.registrations(message)
        results = message.get("results", [])
        if not isinstance(results, list) or len(results) > MAX_COMMANDS or not all(isinstance(r, dict) for r in results):
            raise ValueError("at most 64 result objects")
        async with self.lock:
            await self.ensure_client()
            current = {r["id"]: r for r in registrations}
            # An authenticated complete tab snapshot is evidence of closure.
            # Register parent first on reconnect so its token reclaims ownership.
            await self.register(registrations[0])
            for old in reversed(list(self.state["registrations"].values())):
                if old["id"] not in current:
                    await self.unregister(old)
            for reg in registrations[1:]:
                await self.register(reg)
            self.last_seen = time.monotonic()
            extension = registrations[0]
            if raw is not None:
                # Exact authenticated body bytes, separately from reductions.
                # Authentication headers/secret are deliberately never captured.
                for offset in range(0, len(raw), 12000):
                    await self.client.call("observe", {"source": extension["id"], "incarnation": extension["incarnation"],
                        "kind": "browser.transport.body", "body": {"bytesBase64": base64.b64encode(raw[offset:offset+12000]).decode(),
                            "offset": offset, "length": len(raw), "monotonicNs": str(time.monotonic_ns())}})
            acknowledged = []
            for native_result in results:
                invocation_id = identity(native_result.get("invocationId"))
                pending = self.pending.get(invocation_id)
                status = native_result.get("status")
                if status not in {"completed", "failed", "cancelled", "outcome-unknown"}:
                    raise ValueError("invalid terminal result")
                await self.client.call("observe", {"source": extension["id"], "incarnation": extension["incarnation"],
                    "kind": "browser.result-observed", "parents": [pending["recordId"]] if pending else [],
                    "body": {"native": native_result, "knownDispatch": pending is not None}})
                if pending:
                    try:
                        await self.client.call("result", {"invocationId": invocation_id, "target": pending["target"],
                            "status": status, "value": native_result.get("value")})
                    except ProtocolError as error:
                        if error.code not in {"stale-incarnation", "not-owner"}:
                            raise
                        # A retired document's late native result remains
                        # inspectable; it cannot rewrite the terminal winner.
                    self.pending.pop(invocation_id, None)
                acknowledged.append(invocation_id)
            commands = []
            budget = MAX_FRAME - len(json.dumps(acknowledged).encode()) - 128
            while self.commands:
                size = len(json.dumps(self.commands[0], separators=(",", ":")).encode()) + 1
                if size > budget:
                    break
                commands.append(self.commands.pop(0))
                budget -= size
            # Removing on response construction deliberately does not promise
            # delivery. Lost responses expire to unknown; mutations are not retried.
            return {"version": 1, "commands": commands, "acknowledged": acknowledged}

    async def result(self, invocation, status, value=None):
        return await self.client.call("result", {"invocationId": invocation["id"], "target": invocation["target"], "status": status, "value": value})

    async def invoke(self, invocation):
        reg = self.state["registrations"].get(invocation["target"]["id"])
        if not reg or reg["incarnation"] != invocation["target"]["incarnation"]:
            await self.result(invocation, "failed", {"code": "stale-document"})
        elif invocation["operation"] == "browser.enumerate":
            await self.result(invocation, "completed", {"surfaces": list(self.state["registrations"].values())})
        elif len(self.pending) >= MAX_COMMANDS:
            await self.result(invocation, "failed", {"code": "browser-queue-full"})
        elif len(json.dumps(invocation["arguments"]).encode()) > 12000:
            await self.result(invocation, "failed", {"code": "browser-arguments-too-large"})
        else:
            self.pending[invocation["id"]] = invocation
            self.commands.append({"invocationId": invocation["id"], "target": invocation["target"],
                "operation": invocation["operation"], "arguments": invocation["arguments"], "binding": reg.get("binding", {})})
            await self.result(invocation, "started")

    async def cancel(self, invocation):
        identifier = invocation["id"]
        if identifier in self.pending and any(c["invocationId"] == identifier for c in self.commands):
            self.commands = [c for c in self.commands if c["invocationId"] != identifier]
            self.pending.pop(identifier)
            await self.result(invocation, "cancelled", {"beforeDispatch": True})
        # Synchronous DOM mutation cannot be undone by a late cancellation.

    async def http(self, reader, writer):
        task = asyncio.current_task()
        if len(self.http_tasks) >= 32:
            writer.close()
            return
        self.http_tasks.add(task)
        status, content_type, body = 400, "application/json", b'{"error":"bad request"}'
        try:
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 3)
            if len(header) > 8192:
                raise ValueError("headers too large")
            lines = header.decode("ascii").split("\r\n")
            method, path, _ = lines[0].split(" ")
            headers = {}
            for line in lines[1:]:
                if line:
                    key, value = line.split(":", 1)
                    key = key.lower()
                    if key in headers:
                        raise ValueError("duplicate header")
                    headers[key] = value.strip()
            if headers.get("host") != f'127.0.0.1:{self.state["port"]}':
                status, body = 403, b'{"error":"loopback host required"}'
            elif method == "GET" and (path == "/demo" or path.startswith("/demo?")):
                status, content_type, body = 200, "text/html; charset=utf-8", PAGE
            elif method == "POST" and path == "/exchange":
                if not secrets.compare_digest(headers.get("authorization", ""), "Bearer " + self.state["token"]):
                    status, body = 401, b'{"error":"authentication required"}'
                elif "origin" in headers and not re.fullmatch(r"chrome-extension://[a-p]{32}", headers["origin"]):
                    status, body = 403, b'{"error":"extension origin required"}'
                elif "transfer-encoding" in headers:
                    raise ValueError("chunked transfer unsupported")
                else:
                    length = int(headers.get("content-length", "-1"))
                    if not 0 < length <= MAX_FRAME:
                        status, body = 413, b'{"error":"bounded body required"}'
                    else:
                        raw = await asyncio.wait_for(reader.readexactly(length), 3)
                        result = await self.exchange(json.loads(raw), raw)
                        status, body = 200, json.dumps(result, separators=(",", ":")).encode()
            else:
                status, body = 404, b'{"error":"unknown endpoint"}'
        except (ValueError, UnicodeError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError):
            pass
        except Exception as error:
            status, body = 503, json.dumps({"error": type(error).__name__, "message": str(error)[:300]}).encode()
        try:
            writer.write((f"HTTP/1.1 {status} Result\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\n"
                "Connection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n"
                "Content-Security-Policy: default-src 'self'; style-src 'unsafe-inline'\r\n\r\n").encode() + body)
            await asyncio.wait_for(writer.drain(), 3)
        except (ConnectionError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            self.http_tasks.discard(task)

    async def close(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if self.watchdog:
            self.watchdog.cancel()
            await asyncio.gather(self.watchdog, return_exceptions=True)
        for task in list(self.http_tasks):
            task.cancel()
        await asyncio.gather(*self.http_tasks, return_exceptions=True)
        if self.client:
            # Detach does not claim death of externally owned browser objects.
            await self.client.close()
            self.client = None


async def main_async(args):
    bridge = await BrowserBridge(args.socket, args.runtime).start()
    print(json.dumps({"demoUrl": bridge.origin + "/demo", "extension": str(bridge.runtime / "extension"), "mode": "attached"}), flush=True)
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        await bridge.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--runtime", required=True)
    asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
