"""Private Unix registry, with bounded independent writers."""
import asyncio
import contextlib
import fcntl
import os
import socket
import stat
import struct
import uuid
from pathlib import Path
from .protocol import MAX_FRAME, ProtocolError, decode, encode, require_string
from .registry import Registry


class Peer:
    def __init__(self, reader, writer):
        self.id = str(uuid.uuid4())
        self.reader, self.writer = reader, writer
        self.queue = asyncio.Queue(maxsize=32)

    def send(self, message):
        try:
            self.queue.put_nowait(encode(message))
            return True
        except (asyncio.QueueFull, ProtocolError):
            self.writer.close()
            return False

    async def write(self):
        try:
            while True:
                self.writer.write(await self.queue.get())
                await asyncio.wait_for(self.writer.drain(), 2)
        except (ConnectionError, TimeoutError):
            self.writer.close()


class Server:
    def __init__(self, runtime_dir):
        self.runtime_dir = Path(runtime_dir)
        self.socket_path = self.runtime_dir / 'zygon.sock'
        self.peers, self.tasks = {}, set()
        self.registry = self.listener = self.lock = self.timer = None

    async def start(self):
        self.runtime_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.runtime_dir.chmod(0o700)
        self.lock = open(self.runtime_dir / 'owner.lock', 'a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            self.lock = None
            raise RuntimeError('runtime directory already has a registry owner')
        try:
            if self.socket_path.exists():
                if not stat.S_ISSOCK(self.socket_path.lstat().st_mode):
                    raise RuntimeError('refusing to replace non-socket path')
                self.socket_path.unlink()
            self.registry = Registry(self.runtime_dir)
            self.listener = await asyncio.start_unix_server(self.accept, str(self.socket_path), limit=MAX_FRAME)
            self.socket_path.chmod(0o600)
            self.timer = asyncio.create_task(self.tick())
            return self
        except BaseException:
            if self.registry:
                self.registry.close()
                self.registry = None
            self.lock.close()
            self.lock = None
            raise

    async def tick(self):
        while True:
            await asyncio.sleep(.05)
            self.registry.expire()

    async def accept(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        peer = Peer(reader, writer)
        uid = struct.unpack('3i', writer.get_extra_info('socket').getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[1]
        if len(self.peers) >= 64 or uid != os.getuid():
            writer.close()
            self.tasks.discard(task)
            return
        self.peers[peer.id] = peer
        sending = asyncio.create_task(peer.write())
        try:
            while True:
                try:
                    payload = await reader.readline()
                except (ValueError, asyncio.LimitOverrunError):
                    peer.send({'version': 1, 'id': None, 'error': {'code': 'frame-too-large', 'message': '65536 byte limit'}})
                    await asyncio.sleep(0)
                    break
                if not payload:
                    break
                self.registry.wire(peer.id, payload)
                identifier = None
                try:
                    request = decode(payload)
                    identifier = require_string(request.get('id'), 'request id')
                    method = require_string(request.get('method'), 'method')
                    result, notification = self.registry.dispatch(method, request.get('params', {}), peer.id)
                    response = {'version': 1, 'id': identifier, 'result': result}
                    encode(response)
                    peer.send(response)
                    if notification:
                        owner, message = notification
                        provider = self.peers.get(owner)
                        if provider is None or not provider.send(message):
                            self.registry.disconnect(owner)
                except ProtocolError as e:
                    peer.send({'version': 1, 'id': identifier, 'error': {'code': e.code, 'message': e.message}})
                except (TypeError, ValueError, KeyError, RecursionError) as e:
                    peer.send({'version': 1, 'id': identifier, 'error': {'code': 'invalid-params', 'message': str(e)}})
        except (ConnectionError, BrokenPipeError):
            pass
        finally:
            self.registry.disconnect(peer.id)
            self.peers.pop(peer.id, None)
            sending.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sending
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            self.tasks.discard(task)

    async def close(self):
        if self.listener:
            self.listener.close()
            await self.listener.wait_closed()
        if self.timer:
            self.timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.timer
        for peer in list(self.peers.values()):
            peer.writer.close()
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)
        if self.registry:
            self.registry.close()
            self.registry = None
        if self.lock:
            self.socket_path.unlink(missing_ok=True)
            self.lock.close()
            self.lock = None
