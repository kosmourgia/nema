"""Serialized retained transitions; no process execution or retry in this owner."""
import base64
import contextlib
import hashlib
import json
import secrets
import sqlite3
import time
import uuid
from pathlib import Path
from .protocol import ProtocolError, TERMINAL, encode, reference, require_string


def compact(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


class Registry:
    def __init__(self, runtime_dir):
        self.runtime_dir = Path(runtime_dir)
        self.runtime_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.runtime_dir.chmod(0o700)
        self.db = sqlite3.connect(self.runtime_dir / 'registry.sqlite')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS records(cursor INTEGER PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS registrations(id TEXT PRIMARY KEY, data TEXT NOT NULL, token TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS invocations(id TEXT PRIMARY KEY, scope TEXT UNIQUE NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS wire(id INTEGER PRIMARY KEY, peer TEXT, monotonic_ns TEXT, payload BLOB);
        ''')
        self.incarnation = str(uuid.uuid4())
        self.owners, self.sequences = {}, {}
        for (data,) in self.db.execute('SELECT data FROM records'):
            r = json.loads(data)
            self.sequences[r['source'], r['incarnation']] = int(r['seq'])
        with self.db:
            for r in self.registrations():
                if r['active']:
                    r['presence'] = 'unknown'
                    self.save_registration(r, 'registration.presence')
            for i in self.invocations():
                if i['status'] not in TERMINAL:
                    self.transition(i, 'outcome-unknown', {'reason': 'registry-restarted'})
            self.record('registry.started', {'incarnation': self.incarnation})

    @contextlib.contextmanager
    def transaction(self):
        owners, sequences = self.owners.copy(), self.sequences.copy()
        try:
            with self.db:
                yield
        except BaseException:
            self.owners, self.sequences = owners, sequences
            raise

    def record(self, kind, body, *, source='zygon.registry', incarnation=None, parents=None):
        incarnation = incarnation or self.incarnation
        key = (source, incarnation)
        seq = self.sequences.get(key, 0) + 1
        self.sequences[key] = seq
        record = {'schema': 'nema.lab.record/v1', 'id': str(uuid.uuid4()),
                  'source': source, 'incarnation': incarnation, 'seq': str(seq),
                  'kind': kind, 'parents': parents or [], 'body': body}
        if len(compact(record)) > 48000:
            raise ProtocolError('record-too-large', 'record budget is 48000 ASCII bytes')
        self.db.execute('INSERT INTO records(data) VALUES (?)', (compact(record),))
        return record['id']

    def wire(self, peer, payload):
        with self.db:
            self.db.execute('INSERT INTO wire(peer,monotonic_ns,payload) VALUES (?,?,?)',
                            (peer, str(time.monotonic_ns()), payload))

    def registrations(self):
        return [json.loads(row[0]) for row in self.db.execute('SELECT data FROM registrations ORDER BY id')]

    def invocations(self):
        return [json.loads(row[0]) for row in self.db.execute('SELECT data FROM invocations ORDER BY id')]

    def get_registration(self, ref, *, peer=None, online=False):
        ref = reference(ref)
        row = self.db.execute('SELECT data FROM registrations WHERE id=?', (ref['id'],)).fetchone()
        if row is None:
            raise ProtocolError('unknown-target', 'participant is not registered')
        r = json.loads(row[0])
        if r['incarnation'] != ref['incarnation'] or not r['active']:
            raise ProtocolError('stale-incarnation', 'binding is no longer active')
        if peer is not None and self.owners.get(r['id']) != peer:
            raise ProtocolError('not-owner', 'connection does not own this participant')
        if online and r['presence'] != 'online':
            raise ProtocolError('unavailable', 'presence is unknown; no dispatch attempted')
        return r

    def save_registration(self, r, kind, parents=None):
        rid = self.record(kind, r.copy(), parents=parents)
        self.db.execute('UPDATE registrations SET data=? WHERE id=?', (compact(r), r['id']))
        return rid

    def settle_target(self, ref, reason):
        for i in self.invocations():
            if i['target'] == ref and i['status'] not in TERMINAL:
                self.transition(i, 'outcome-unknown', {'reason': reason})

    def register(self, p, peer):
        r = p.get('registration')
        if not isinstance(r, dict):
            raise ProtocolError('invalid-params', 'registration object required')
        r = json.loads(compact(r))
        reference(r)
        for key in ('kind', 'medium', 'mode'):
            require_string(r.get(key), key)
        if r['mode'] not in ('owned', 'attached', 'observer'):
            raise ProtocolError('invalid-params', 'invalid attachment mode')
        self.validate_operations(r.get('operations'))
        if r.get('parent') is not None:
            parent = reference(r['parent'])
            seen = {r['id']}
            ancestor = self.get_registration(parent)
            while ancestor:
                if ancestor['id'] in seen:
                    raise ProtocolError('invalid-parent', 'cyclic parent relationship')
                seen.add(ancestor['id'])
                ancestor = self.get_registration(ancestor['parent']) if ancestor.get('parent') else None
        old = self.db.execute('SELECT data,token FROM registrations WHERE id=?', (r['id'],)).fetchone()
        token = secrets.token_urlsafe(32)
        if old:
            previous, token = json.loads(old[0]), old[1]
            if self.owners.get(r['id']) != peer and not secrets.compare_digest(str(p.get('resumeToken', '')), token):
                raise ProtocolError('continuity-required', 'existing identity requires its resume token')
            if previous['incarnation'] == r['incarnation']:
                if not previous['active']:
                    raise ProtocolError('stale-incarnation', 'ended incarnation cannot be revived')
                for key in ('parent', 'mode', 'kind', 'medium'):
                    if previous.get(key) != r.get(key):
                        raise ProtocolError('identity-conflict', f'{key} changed without a new incarnation')
                if self.owners.get(r['id']) not in (None, peer):
                    self.settle_target(reference(previous), 'provider-connection-replaced')
            else:
                self.settle_target(reference(previous), 'incarnation-replaced')
                self.end_children(reference(previous), 'parent-incarnation-replaced')
        r.update(active=True, presence='online')
        encode({'version': 1, 'id': 'register', 'result': {'registration': r, 'resumeToken': token}})
        self.db.execute('INSERT OR REPLACE INTO registrations VALUES (?,?,?)', (r['id'], compact(r), token))
        self.owners[r['id']] = peer
        self.save_registration(r, 'registration.registered')
        return {'registration': r, 'resumeToken': token}

    @staticmethod
    def validate_operations(operations):
        if not isinstance(operations, list) or len(operations) > 32:
            raise ProtocolError('invalid-params', 'operations must be a list of at most 32 names')
        for operation in operations:
            require_string(operation, 'operation')
            if '.' not in operation:
                raise ProtocolError('invalid-params', 'native operation must be namespaced')

    def end_children(self, parent, reason):
        for r in self.registrations():
            if r.get('parent') == parent and r['active']:
                r.update(active=False, presence='ended')
                self.save_registration(r, 'registration.ended')
                self.settle_target(reference(r), reason)
                self.end_children(reference(r), reason)

    def invocation(self, identifier):
        require_string(identifier, 'invocationId')
        row = self.db.execute('SELECT data FROM invocations WHERE id=?', (identifier,)).fetchone()
        if not row:
            raise ProtocolError('unknown-invocation', 'invocation does not exist')
        return json.loads(row[0])

    def transition(self, i, status, value=None):
        parents = [i['recordId']] if i.get('recordId') else []
        i['status'] = status
        if value is not None:
            i['value'] = value
        rid = self.record('invocation.' + status, i.copy(), parents=parents)
        i['recordId'] = rid
        self.db.execute('UPDATE invocations SET data=? WHERE id=?', (compact(i), i['id']))
        return i

    def dispatch(self, method, p, peer):
        """Return (reply, optional(owner,notification)); send only after commit."""
        if not isinstance(p, dict):
            raise ProtocolError('invalid-params', 'params object required')
        notification = None
        with self.transaction():
            if method == 'register':
                result = self.register(p, peer)
            elif method == 'update':
                r = self.get_registration(p, peer=peer)
                patch = p.get('patch')
                if not isinstance(patch, dict) or set(patch) & {'id', 'incarnation', 'kind', 'mode', 'medium', 'parent', 'active', 'presence'}:
                    raise ProtocolError('invalid-params', 'patch changes immutable registration fields')
                if 'operations' in patch:
                    self.validate_operations(patch['operations'])
                r.update(patch)
                self.save_registration(r, 'registration.updated')
                result = r
            elif method == 'unregister':
                r = self.get_registration(p, peer=peer)
                r.update(active=False, presence='ended')
                self.save_registration(r, 'registration.ended')
                self.settle_target(reference(r), p.get('reason', 'unregistered'))
                self.end_children(reference(r), 'parent-unregistered')
                result = r
            elif method == 'inspect':
                result = {'cursor': self.cursor(), 'registrations': self.registrations(), 'invocations': self.invocations()}
            elif method == 'subscribe':
                result = self.subscribe(p)
            elif method == 'invoke':
                requester = self.get_registration(p.get('requester'), peer=peer, online=True)
                target_ref = reference(p.get('target'))
                operation = require_string(p.get('operation'), 'operation')
                correlation = require_string(p.get('correlation'), 'correlation')
                if not isinstance(p.get('arguments'), dict):
                    raise ProtocolError('invalid-params', 'arguments object required')
                timeout = p.get('timeoutMs', 30000)
                if type(timeout) is not int or not 1 <= timeout <= 300000:
                    raise ProtocolError('invalid-params', 'timeoutMs must be 1..300000')
                scope = compact([requester['id'], requester['incarnation'], correlation])
                old = self.db.execute('SELECT data FROM invocations WHERE scope=?', (scope,)).fetchone()
                if old:
                    result = json.loads(old[0])
                    if any(result[k] != p[k] for k in ('requester', 'target', 'operation', 'arguments', 'correlation')):
                        raise ProtocolError('correlation-conflict', 'correlation already names a different invocation')
                else:
                    target = self.get_registration(target_ref, online=True)
                    if operation not in target['operations']:
                        raise ProtocolError('unsupported-operation', 'target does not advertise this operation')
                    if sum(i['status'] not in TERMINAL for i in self.invocations()) >= 128:
                        raise ProtocolError('busy', '128 pending invocation budget exhausted')
                    result = {k: p[k] for k in ('requester', 'target', 'operation', 'arguments', 'correlation')}
                    result.update(id=str(uuid.uuid4()), status='accepted', deadlineNs=str(time.monotonic_ns() + timeout * 1000000))
                    self.db.execute('INSERT INTO invocations VALUES (?,?,?)', (result['id'], scope, compact(result)))
                    self.transition(result, 'accepted')
                    notification = (self.owners[target['id']], {'version': 1, 'method': 'invoke', 'params': result})
            elif method == 'result':
                result = self.invocation(p.get('invocationId'))
                if reference(p.get('target')) != result['target']:
                    raise ProtocolError('stale-incarnation', 'result target differs from original invocation')
                self.get_registration(p['target'], peer=peer)
                status = p.get('status')
                if status not in TERMINAL | {'started'}:
                    raise ProtocolError('invalid-params', 'invalid result status')
                if result['status'] in TERMINAL:
                    self.record('invocation.duplicate-result', p, parents=[result['recordId']])
                elif result['status'] != status:
                    self.transition(result, status, p.get('value'))
            elif method == 'cancel':
                result = self.invocation(p.get('invocationId'))
                self.get_registration(p.get('requester'), peer=peer)
                if reference(p['requester']) != result['requester']:
                    raise ProtocolError('not-owner', 'only original requester may cancel')
                if result['status'] not in TERMINAL:
                    result['cancelRequested'] = True
                    self.transition(result, result['status'])
                    owner = self.owners.get(result['target']['id'])
                    if owner:
                        notification = (owner, {'version': 1, 'method': 'cancel', 'params': result})
            elif method == 'observe':
                self.get_registration({'id': p.get('source'), 'incarnation': p.get('incarnation')}, peer=peer)
                kind = require_string(p.get('kind'), 'kind')
                if kind.startswith(('registration.', 'invocation.', 'registry.')):
                    raise ProtocolError('invalid-params', 'reserved transition namespace')
                parents = p.get('parents', [])
                if not isinstance(parents, list) or len(parents) > 32 or not all(isinstance(x, str) for x in parents):
                    raise ProtocolError('invalid-params', 'parents must be record IDs')
                body = p.get('body')
                if not isinstance(body, dict):
                    raise ProtocolError('invalid-params', 'observation body must be object')
                if kind == 'capture.chunk':
                    try:
                        raw = base64.b64decode(body['bytesBase64'], validate=True)
                    except (ValueError, KeyError, TypeError) as e:
                        raise ProtocolError('invalid-params', 'capture requires valid base64') from e
                    body = dict(body, byteCount=len(raw), sha256=hashlib.sha256(raw).hexdigest())
                result = {'recordId': self.record(kind, body, source=p['source'], incarnation=p['incarnation'], parents=parents)}
            else:
                raise ProtocolError('unknown-method', method)
        return result, notification

    def cursor(self):
        return str(self.db.execute('SELECT COALESCE(MAX(cursor),0) FROM records').fetchone()[0])

    def subscribe(self, p):
        cursor, limit = p.get('cursor', '0'), p.get('limit', 64)
        if not isinstance(cursor, str) or not cursor.isdecimal() or len(cursor) > 18:
            raise ProtocolError('invalid-cursor', 'cursor must be a decimal string')
        if int(cursor) > int(self.cursor()):
            raise ProtocolError('invalid-cursor', 'cursor exceeds this retained journal')
        if type(limit) is not int or not 1 <= limit <= 128:
            raise ProtocolError('invalid-params', 'limit must be 1..128')
        records, size = [], 0
        for seq, data in self.db.execute('SELECT cursor,data FROM records WHERE cursor>? ORDER BY cursor LIMIT ?', (int(cursor), limit)):
            if records and size + len(data) > 50000:
                break
            size += len(data)
            cursor = str(seq)
            records.append(json.loads(data))
        return {'cursor': cursor, 'records': records, 'more': int(cursor) < int(self.cursor())}

    def disconnect(self, peer):
        with self.transaction():
            for r in self.registrations():
                if self.owners.get(r['id']) == peer:
                    self.owners.pop(r['id'], None)
                    if r['active']:
                        r['presence'] = 'unknown'
                        self.save_registration(r, 'registration.presence')
                        self.settle_target(reference(r), 'provider-disconnected')

    def expire(self):
        with self.transaction():
            for i in self.invocations():
                if i['status'] not in TERMINAL and int(i['deadlineNs']) <= time.monotonic_ns():
                    self.transition(i, 'outcome-unknown', {'reason': 'operational-timeout'})

    def close(self):
        for peer in set(self.owners.values()):
            self.disconnect(peer)
        self.db.close()
