"""Read-only deterministic reduction; never starts a registry or host."""
import json
import sqlite3
from pathlib import Path
from .protocol import TERMINAL


def reduce_records(records):
    registrations, invocations = {}, {}
    for record in records:
        kind, body = record['kind'], record['body']
        if kind.startswith('registration.'):
            registrations[body['id']] = body
        elif kind in {'invocation.' + status for status in TERMINAL | {'accepted', 'started'}}:
            invocations[body['id']] = dict(body, recordId=record['id'])
    return {'registrations': sorted(registrations.values(), key=lambda r: r['id']),
            'invocations': sorted(invocations.values(), key=lambda i: i['id'])}


def replay(runtime):
    path = Path(runtime) / 'registry.sqlite'
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
    try:
        # One read transaction: a live registry may keep appending separately.
        db.execute('BEGIN')
        result = reduce_records(json.loads(r[0]) for r in db.execute('SELECT data FROM records ORDER BY cursor'))
        expected = {table: [json.loads(row[0]) for row in db.execute(f'SELECT data FROM {table} ORDER BY id')]
                    for table in ('registrations', 'invocations')}
        if result != expected:
            raise RuntimeError('retained transitions disagree with materialized snapshot')
        result['cursor'] = str(db.execute('SELECT COALESCE(MAX(cursor),0) FROM records').fetchone()[0])
        result['replayVerified'] = True
        return result
    finally:
        db.close()
