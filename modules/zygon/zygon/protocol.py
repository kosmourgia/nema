import json

MAX_FRAME = 65536
TERMINAL = frozenset({'completed', 'failed', 'cancelled', 'outcome-unknown'})


class ProtocolError(Exception):
    def __init__(self, code, message=None):
        self.code, self.message = code, message or code
        super().__init__(f'{self.code}: {self.message}')


def encode(value):
    data = json.dumps(value, ensure_ascii=True, separators=(',', ':'), allow_nan=False).encode() + b'\n'
    if len(data) > MAX_FRAME:
        raise ProtocolError('frame-too-large', 'maximum frame is 65536 bytes including LF')
    return data


def decode(data):
    if len(data) > MAX_FRAME or not data.endswith(b'\n'):
        raise ProtocolError('invalid-frame', 'bounded LF-terminated frame required')
    try:
        def pairs(items):
            result = {}
            for k, v in items:
                if k in result:
                    raise ValueError('duplicate JSON key')
                result[k] = v
            return result
        value = json.loads(data.decode('utf-8'), object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite number')))
    except (ValueError, RecursionError) as e:
        raise ProtocolError('invalid-json', str(e)) from e
    if not isinstance(value, dict) or type(value.get('version')) is not int or value['version'] != 1:
        raise ProtocolError('invalid-version', 'expected object with version 1')
    return value


def require_string(value, label):
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ProtocolError('invalid-params', f'{label} must be a nonempty string of at most 512 characters')
    return value


def reference(value):
    if not isinstance(value, dict):
        raise ProtocolError('invalid-params', 'identity reference must be an object')
    return {k: require_string(value.get(k), k) for k in ('id', 'incarnation')}
