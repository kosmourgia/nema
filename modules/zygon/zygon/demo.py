"""Real child, independent clients, Flix effect/CSP and retained observations."""
import asyncio
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path
from .client import Client
from .cli import MODULE, register_observer, render, show
from .protocol import TERMINAL, reference
from .server import Server


async def subprocess_output(command, *, env=None, timeout=120):
    child = await asyncio.create_subprocess_exec(*command, env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    try:
        output, _ = await asyncio.wait_for(child.communicate(), timeout)
    except BaseException:
        if child.returncode is None:
            child.kill()
        await child.wait()
        raise
    text = output.decode('utf-8', errors='replace')
    print(text, end='', flush=True)
    if child.returncode:
        raise RuntimeError(f'{command[0]} exited {child.returncode}')
    return text


async def invoke(client, requester, target, operation, arguments, correlation):
    return await client.call('invoke', {'requester': requester, 'target': target,
        'operation': operation, 'arguments': arguments, 'correlation': correlation})


async def demo(runtime, *, with_flix=True):
    from .processes import OwnedProcess
    from .attached import AttachedCompanion
    from .replay import replay
    run = Path(runtime) / 'demos' / uuid.uuid4().hex[:8]
    run.mkdir(parents=True, mode=0o700)
    server = await Server(run).start()
    clients, owned_instances = [], []
    external = attached = None
    report = {'schema': 'nema.zygon.demo/v1', 'runtime': str(run), 'checks': []}
    async def connect():
        client = await Client.connect(server.socket_path)
        clients.append(client)
        return client
    def passed(label, **data):
        report['checks'].append({'check': label, **data})
        show({'PASS': label, **data})
    try:
        provider = await connect()
        owner = await OwnedProcess.launch(provider, run / 'owned', participant_id='python-demo')
        owned_instances.append(owner)
        await asyncio.sleep(.08)  # Complete fixture birth bytes before same-stream interleaving.
        terminal = await connect()
        requester = await register_observer(terminal, 'demo-terminal')
        target = reference(owner.surfaces[0])
        await subprocess_output([sys.executable, str(MODULE / 'bin' / 'zygon'), '--socket', str(server.socket_path), 'inspect'])
        passed('independent terminal sees host and two surfaces', host=owner.ref)
        cursor = (await terminal.call('inspect'))['cursor']
        slow = await invoke(terminal, requester, target, 'demo.delay', {'seconds': .6, 'text': 'slow-answer'}, 'slow-original')
        fast = await invoke(terminal, requester, target, 'demo.echo', {'text': 'fast-answer'}, 'fast-original')
        fast_result = await terminal.wait_result(fast['id'], cursor=cursor)
        assert fast_result['status'] == 'completed'
        snapshot = await terminal.call('inspect')
        assert next(i for i in snapshot['invocations'] if i['id'] == slow['id'])['status'] not in TERMINAL
        observations = []
        async def observed(record):
            if record['kind'] == 'capture.chunk':
                observations.append(record['id'])
        slow_result = await terminal.wait_result(slow['id'], cursor=cursor, on_record=observed)
        assert slow_result['status'] == 'completed' and slow_result['correlation'] == 'slow-original' and observations
        passed('fast completes while slow correlation waits and bytes arrive',
               fastRecord=fast_result['recordId'], slowRecord=slow_result['recordId'], observations=len(observations))
        for n in range(3):
            i = await invoke(terminal, requester, target, 'demo.echo', {'text': f'repeat-{n}'}, f'repeat-{n}')
            assert (await terminal.wait_result(i['id']))['value']['echo'] == f'repeat-{n}'
        await owner.write_stdin(b'raw-input:\xff\n')
        await asyncio.sleep(.1)
        await owner.flush_observations()
        stdout, stderr = owner.spool.raw_bytes('stdout'), owner.spool.raw_bytes('stderr')
        assert b'birth:partial\xff\xfe\n' in stdout and b'diagnostic:\x80\n' in stderr
        assert b'stdin:raw-input:\xff\n' in stdout
        passed('lossless partial invalid UTF-8 and stdin preserved', stdoutSha256=hashlib.sha256(stdout).hexdigest(),
               stderrSha256=hashlib.sha256(stderr).hexdigest())
        if with_flix:
            env = dict(os.environ, ZYGON_SOCKET=str(server.socket_path), ZYGON_TARGET_ID=target['id'],
                ZYGON_TARGET_INCARNATION=target['incarnation'], ZYGON_OPERATION='demo.delay',
                ZYGON_ARGUMENTS=json.dumps({'seconds': .8, 'text': 'Flix-result'}), ZYGON_REQUIRE_OBSERVATION='1')
            await subprocess_output([str(MODULE / 'flix' / 'run')], env=env)
            snapshot = await terminal.call('inspect')
            results = [i for i in snapshot['invocations'] if i['operation'] == 'demo.delay' and i['arguments'].get('text') == 'Flix-result']
            assert len(results) == 1 and results[0]['status'] == 'completed'
            passed('actual Flix and terminal share retained result', recordId=results[0]['recordId'])
        await owner.close_stdin()
        assert await owner.wait() == 0
        passed('pipe EOF and exit code observed', exitCode=owner.process.returncode)
        old = owner.ref.copy()
        await owner.close()
        restarted = await OwnedProcess.launch(provider, run / 'owned', participant_id='python-demo')
        owned_instances.append(restarted)
        assert restarted.ref['id'] == old['id'] and restarted.ref['incarnation'] != old['incarnation']
        passed('restart preserves justified stable identity and changes incarnation', old=old, new=restarted.ref)
        pty = await OwnedProcess.launch(await connect(), run / 'pty', participant_id='pty-demo', mode='pty')
        owned_instances.append(pty)
        await asyncio.sleep(.1)
        assert pty.resize(31, 101) == {'rows': 31, 'cols': 101}
        pty.interrupt()
        await asyncio.sleep(.1)
        assert b'signal:SIGINT' in pty.spool.raw_bytes('pty')
        await pty.stop()
        passed('PTY merged bytes resize cooperative interrupt and stop', exitCode=pty.process.returncode)
        endpoint = run / 'external' / 'host.sock'
        endpoint.parent.mkdir(mode=0o700)
        external = await asyncio.create_subprocess_exec(sys.executable, '-m', 'zygon.demo_child', '--listen', str(endpoint),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        for _ in range(200):
            if endpoint.exists():
                break
            await asyncio.sleep(.01)
        assert endpoint.exists(), 'external endpoint failed to start'
        attached = await AttachedCompanion.attach(await connect(), endpoint, run / 'attached')
        i = await invoke(terminal, requester, reference(attached.surfaces[0]), 'demo.echo', {'text': 'attached-answer'}, 'attached-original')
        assert (await terminal.wait_result(i['id']))['status'] == 'completed'
        external_ref = attached.ref.copy()
        await attached.detach()
        attached = None
        assert external.returncode is None
        os.kill(external.pid, 0)
        passed('attach call and detach leave external host alive', host=external_ref)
        snapshot = await terminal.call('inspect')
        assert all(i['status'] in TERMINAL for i in snapshot['invocations'])
        render(snapshot)
        passed('every accepted invocation has an explicit terminal state')
    finally:
        if attached:
            await attached.detach()
        if external and external.returncode is None:
            external.terminate()  # Demo owns this fixture; attached companion does not.
            await asyncio.wait_for(external.wait(), 5)
        for owned in reversed(owned_instances):
            await owned.close()
        for client in reversed(clients):
            await client.close()
        await server.close()
    replayed = replay(run)
    passed('offline retained transitions reproduce snapshot', cursor=replayed['cursor'])
    (run / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    show({'report': str(run / 'report.json')})
    return report
