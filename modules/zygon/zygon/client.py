"""Async client: reply routing never waits for an invocation handler."""
import asyncio
import contextlib
import uuid
from .protocol import MAX_FRAME, TERMINAL, ProtocolError, decode, encode


class Client:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer
        self.pending = {}
        self.on_invoke = self.on_cancel = None
        self.workers = set()
        self.send_lock = asyncio.Lock()
        self.reader_task = asyncio.create_task(self.read())

    @classmethod
    async def connect(cls, socket_path):
        reader, writer = await asyncio.open_unix_connection(str(socket_path), limit=MAX_FRAME)
        return cls(reader, writer)

    async def call(self, method, params=None, *, timeout=10):
        if self.writer.is_closing():
            raise ProtocolError('disconnected', 'connection is closed; no retry attempted')
        if len(self.pending) >= 64:
            raise ProtocolError('busy', '64 pending protocol calls already waiting')
        identifier = str(uuid.uuid4())
        payload = encode({'version': 1, 'id': identifier, 'method': method, 'params': params or {}})
        future = asyncio.get_running_loop().create_future()
        self.pending[identifier] = future
        try:
            async with self.send_lock:
                self.writer.write(payload)
                await asyncio.wait_for(self.writer.drain(), timeout)
            return await asyncio.wait_for(future, timeout)
        except TimeoutError as e:
            raise ProtocolError('outcome-unknown', 'reply wait timed out; do not retry arbitrary writes') from e
        finally:
            self.pending.pop(identifier, None)

    async def invoke_callback(self, method, invocation):
        callback = self.on_invoke if method == 'invoke' else self.on_cancel
        try:
            if callback:
                await callback(invocation)
            elif method == 'invoke':
                await self.call('result', {'invocationId': invocation['id'], 'target': invocation['target'],
                    'status': 'failed', 'value': {'reason': 'no-provider-handler'}})
        except Exception as e:
            with contextlib.suppress(Exception):
                await self.call('result', {'invocationId': invocation['id'], 'target': invocation['target'],
                    'status': 'outcome-unknown', 'value': {'reason': type(e).__name__, 'message': str(e)}})

    async def read(self):
        try:
            while True:
                line = await self.reader.readline()
                if not line:
                    break
                response = decode(line)
                if 'id' in response:
                    future = self.pending.get(response['id'])
                    if future is None or future.done():
                        continue
                    if 'error' in response:
                        e = response['error']
                        future.set_exception(ProtocolError(e['code'], e['message']))
                    else:
                        future.set_result(response['result'])
                elif response.get('method') in ('invoke', 'cancel'):
                    cap = 32 if response['method'] == 'cancel' else 16
                    if len(self.workers) >= cap:
                        self.writer.close()
                        break
                    worker = asyncio.create_task(self.invoke_callback(response['method'], response['params']))
                    self.workers.add(worker)
                    worker.add_done_callback(self.workers.discard)
        except (ValueError, ProtocolError, ConnectionError, KeyError):
            pass
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(ProtocolError('outcome-unknown', 'connection lost while waiting; no retry attempted'))

    async def wait_result(self, invocation_id, *, timeout=35, on_record=None, cursor='0'):
        async with asyncio.timeout(timeout):
            while True:
                page = await self.call('subscribe', {'cursor': cursor})
                for record in page['records']:
                    if on_record:
                        await on_record(record)
                    if record['kind'].startswith('invocation.'):
                        i = record['body']
                        if i.get('id') == invocation_id and i.get('status') in TERMINAL:
                            return dict(i, recordId=record['id'])
                cursor = page['cursor']
                if not page['more']:
                    await asyncio.sleep(.03)

    async def close(self):
        self.writer.close()
        with contextlib.suppress(ConnectionError):
            await self.writer.wait_closed()
        self.reader_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.reader_task
        # A provider is allowed to detach from its own callback. Joining that
        # callback here would await ourselves and create a cancellation cycle.
        workers = [worker for worker in self.workers if worker is not asyncio.current_task()]
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
