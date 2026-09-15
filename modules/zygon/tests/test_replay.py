import base64
import json
from pathlib import Path
import tempfile
import unittest
from zygon.protocol import ProtocolError
from zygon.registry import Registry
from zygon.replay import replay, reduce_records


class ReplayTests(unittest.TestCase):
    def test_replay_preserves_unknown_native_bytes_and_first_terminal(self):
        root = Path(__file__).resolve().parents[3] / '.nema' / 'replay-tests'
        root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=root) as runtime:
            reg = Registry(runtime)
            requester = {'id': 'terminal', 'incarnation': 'terminal-1'}
            target = {'id': 'host', 'incarnation': 'host-1'}
            for ref, peer in ((requester, 'requester'), (target, 'provider')):
                reg.dispatch('register', {'registration': {**ref, 'kind': 'host', 'medium': 'pipe',
                    'mode': 'owned', 'parent': None, 'operations': ['demo.echo'],
                    'unknownNative': {'nested': [1, None, 'future']}}}, peer)
            raw = b'partial\xff\xfe'
            wire = b'{"version":1,"futureUnknown":"x"}\n'
            reg.wire('native-connection', wire)
            reg.dispatch('observe', {'source': 'host', 'incarnation': 'host-1', 'kind': 'capture.chunk',
                'body': {'bytesBase64': base64.b64encode(raw).decode(), 'stream': 'stdout',
                         'streamSeq': '1', 'monotonicNs': '999'}}, 'provider')
            i, _ = reg.dispatch('invoke', {'requester': requester, 'target': target, 'operation': 'demo.echo',
                'arguments': {'futureArgument': True}, 'correlation': 'original'}, 'requester')
            reg.dispatch('result', {'invocationId': i['id'], 'target': target,
                'status': 'completed', 'value': {'first': True}}, 'provider')
            reg.dispatch('result', {'invocationId': i['id'], 'target': target,
                'status': 'failed', 'value': {'first': False}}, 'provider')
            records = [json.loads(r[0]) for r in reg.db.execute('SELECT data FROM records ORDER BY cursor')]
            self.assertEqual(reduce_records(records), reduce_records(records))
            capture = next(r for r in records if r['kind'] == 'capture.chunk')
            self.assertEqual(base64.b64decode(capture['body']['bytesBase64']), raw)
            self.assertEqual(reg.db.execute('SELECT payload FROM wire').fetchone()[0], wire)
            reg.close()
            snapshot = replay(runtime)
            self.assertTrue(snapshot['replayVerified'])
            self.assertEqual(snapshot['invocations'][0]['value'], {'first': True})
            self.assertEqual(snapshot['registrations'][0]['unknownNative'], {'nested': [1, None, 'future']})

    def test_invalid_registration_rollback_has_no_ghost_owner_or_sequence(self):
        root = Path(__file__).resolve().parents[3] / '.nema' / 'replay-tests'
        root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=root) as runtime:
            reg = Registry(runtime)
            cursor, sequences = reg.cursor(), reg.sequences.copy()
            with self.assertRaises(ProtocolError):
                reg.dispatch('register', {'registration': {'id': 'ghost', 'incarnation': 'one', 'kind': 'host',
                    'medium': 'pipe', 'mode': 'owned', 'operations': [], 'parent': None,
                    'metadata': {'large': 'x' * 50000}}}, 'bad-provider')
            self.assertEqual(reg.owners, {})
            self.assertEqual(reg.cursor(), cursor)
            self.assertEqual(reg.sequences, sequences)
            reg.close()
