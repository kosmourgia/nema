import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import uuid
from .client import Client
from .protocol import ProtocolError, reference

MODULE = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME = MODULE.parents[1] / '.nema' / 'zygon'


def show(value):
    print(json.dumps(value, ensure_ascii=True, sort_keys=True), flush=True)


def render(snapshot):
    rows = {r['id']: r for r in snapshot['registrations']}
    print(f"zygon topology cursor={snapshot['cursor']}")
    def one(r, depth, seen):
        if r['id'] in seen:
            return
        seen.add(r['id'])
        print(f"{'  ' * depth}{r['id']} [{r['kind']} / {r['medium']} / {r['mode']}] {r['presence']}")
        print(f"{'  ' * (depth+1)}incarnation={r['incarnation']} operations={','.join(r['operations']) or '(none)'}")
        for child in rows.values():
            if child.get('parent') == reference(r):
                one(child, depth + 1, seen)
    seen = set()
    for r in rows.values():
        if not r.get('parent') or r['parent']['id'] not in rows:
            one(r, 0, seen)
    for r in rows.values():
        one(r, 0, seen)
    for i in snapshot['invocations']:
        print(f"  request {i['correlation']} -> {i['target']['id']}@{i['target']['incarnation']} {i['status']} record={i['recordId']}")


async def stopped(duration=None):
    event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, event.set)
    try:
        if duration is None:
            await event.wait()
        else:
            try:
                await asyncio.wait_for(event.wait(), duration)
            except TimeoutError:
                pass
    finally:
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(s)


async def register_observer(client, name='terminal'):
    r = {'id': name + '-' + uuid.uuid4().hex, 'incarnation': str(uuid.uuid4()),
         'kind': 'client', 'medium': 'unix-json', 'mode': 'observer', 'parent': None, 'operations': []}
    await client.call('register', {'registration': r})
    return reference(r)


async def run(args):
    socket_path = args.socket or args.runtime / 'zygon.sock'
    if args.command == 'serve':
        from .server import Server
        server = await Server(args.runtime).start()
        show({'socket': str(server.socket_path), 'registryIncarnation': server.registry.incarnation})
        try:
            await stopped()
        finally:
            await server.close()
        return
    if args.command == 'demo':
        from .demo import demo
        return await demo(args.runtime, with_flix=not args.no_flix)
    if args.command == 'replay':
        from .replay import replay
        show(replay(args.runtime))
        return
    if args.command == 'browser':
        from .browser import BrowserBridge
        bridge = await BrowserBridge(socket_path, args.runtime / 'browser').start()
        try:
            show({'demo': bridge.origin + '/demo', 'unpackedExtension': str(args.runtime / 'browser' / 'extension')})
            await stopped()
        finally:
            await bridge.close()
        return
    client = await Client.connect(socket_path)
    try:
        if args.command == 'inspect':
            snapshot = await client.call('inspect')
            show(snapshot) if args.json else render(snapshot)
        elif args.command == 'ipc':
            show(await client.call(args.method, json.loads(args.params)))
        elif args.command in ('watch', 'export'):
            cursor = args.cursor
            while True:
                page = await client.call('subscribe', {'cursor': cursor})
                for record in page['records']:
                    show(record)
                cursor = page['cursor']
                if not page['more']:
                    if args.command == 'export':
                        break
                    await asyncio.sleep(.2)
        elif args.command == 'invoke':
            requester = await register_observer(client)
            cursor = (await client.call('inspect'))['cursor']
            i = await client.call('invoke', {'requester': requester,
                'target': {'id': args.target, 'incarnation': args.incarnation},
                'operation': args.operation, 'arguments': json.loads(args.arguments),
                'correlation': args.correlation or str(uuid.uuid4()), 'timeoutMs': args.timeout_ms})
            show(i)
            result = await client.wait_result(i['id'], cursor=cursor, timeout=args.timeout_ms/1000 + 5)
            show(result)
            if result['status'] != 'completed':
                raise ProtocolError(result['status'], 'invocation did not complete successfully')
        elif args.command == 'launch':
            from .processes import OwnedProcess
            command = args.child or None
            if command and command[0] == '--':
                command = command[1:]
            process_key = hashlib.sha256(args.name.encode()).hexdigest()[:12]
            owned = await OwnedProcess.launch(client, args.runtime / 'processes' / process_key, command,
                participant_id=args.name, mode=args.mode, cooperative=command is None)
            show({'host': owned.ref, 'surfaces': owned.surfaces})
            stop = asyncio.create_task(stopped())
            exited = asyncio.create_task(owned.finished.wait())
            try:
                await asyncio.wait((stop, exited), return_when=asyncio.FIRST_COMPLETED)
            finally:
                stop.cancel()
                await asyncio.gather(stop, return_exceptions=True)
                await owned.close()
                await exited
            code = owned.process.returncode or 0
            return 74 if owned.spool.failure else (128 - code if code < 0 else code)
        elif args.command == 'attach':
            from .attached import AttachedCompanion
            endpoint_key = hashlib.sha256(str(args.endpoint.resolve()).encode()).hexdigest()[:12]
            attached = await AttachedCompanion.attach(client, args.endpoint, args.runtime / 'attachments' / endpoint_key)
            try:
                show({'attached': attached.ref, 'note': 'SIGINT detaches; external host remains alive'})
                await stopped(args.duration)
            finally:
                await attached.detach()
    finally:
        await client.close()


def main():
    os.environ['PYTHONPATH'] = str(MODULE) + (os.pathsep + os.environ['PYTHONPATH'] if os.environ.get('PYTHONPATH') else '')
    parser = argparse.ArgumentParser(description='Individually addressable process and browser companions')
    parser.add_argument('--runtime', type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument('--socket', type=Path)
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('serve', 'browser', 'replay'):
        sub.add_parser(name)
    p = sub.add_parser('inspect'); p.add_argument('--json', action='store_true')
    for name in ('watch', 'export'):
        p = sub.add_parser(name); p.add_argument('--cursor', default='0')
    p = sub.add_parser('ipc'); p.add_argument('method'); p.add_argument('params', nargs='?', default='{}')
    p = sub.add_parser('invoke')
    p.add_argument('target'); p.add_argument('incarnation'); p.add_argument('operation')
    p.add_argument('arguments', nargs='?', default='{}'); p.add_argument('--correlation')
    p.add_argument('--timeout-ms', type=int, default=30000)
    p = sub.add_parser('launch'); p.add_argument('--mode', choices=['pipe', 'pty'], default='pipe')
    p.add_argument('--name', default='process-demo'); p.add_argument('child', nargs=argparse.REMAINDER)
    p = sub.add_parser('attach'); p.add_argument('endpoint', type=Path); p.add_argument('--duration', type=float)
    p = sub.add_parser('demo'); p.add_argument('--no-flix', action='store_true')
    sub.add_parser('test'); sub.add_parser('check')
    p = sub.add_parser('demo-browser'); p.add_argument('--with-flix', action='store_true')
    args = parser.parse_args()
    if args.command in ('test', 'check', 'demo-browser'):
        env = dict(os.environ, PYTHONPATH=str(MODULE))
        if args.command == 'demo-browser':
            command = [sys.executable, '-m', 'zygon.demo_browser', '--runtime', str(args.runtime / ('b-' + uuid.uuid4().hex[:6]))]
            if args.with_flix:
                command.append('--with-flix')
            return subprocess.call(command, cwd=MODULE.parents[1], env=env)
        if args.command == 'check':
            result = subprocess.call([sys.executable, '-m', 'compileall', '-q', str(MODULE / 'zygon')], env=env)
        else:
            result = subprocess.call([sys.executable, '-m', 'unittest', 'discover', '-s', str(MODULE / 'tests'), '-v'], env=env)
        return result or subprocess.call([str(MODULE / 'flix' / 'run'), 'test' if args.command == 'test' else 'check'], env=env)
    try:
        result = asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except (ProtocolError, OSError, TimeoutError) as e:
        print(str(e), file=sys.stderr)
        return 1
    return result if isinstance(result, int) else 0


if __name__ == '__main__':
    raise SystemExit(main())
