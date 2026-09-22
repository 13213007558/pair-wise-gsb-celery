from unittest.mock import Mock

import pytest

from celery.exceptions import IncompatibleTaskProtocol
from celery.protocol import validate_before_decode, validate_payload


def message(protocol=2, content_type='application/json', **headers):
    message = Mock(name='message')
    message.headers = {
        'id': 'task-id', 'task': 'task.name', 'protocol': protocol,
        **headers,
    }
    message.content_type = content_type
    message.content_encoding = 'utf-8'
    message.properties = {'correlation_id': 'task-id'}
    return message


def test_accepts_explicit_protocol_3_when_enabled():
    msg = message(
        protocol=3, root_id='task-id', argsrepr='()', kwargsrepr='{}',
        stamped_headers=[], stamps={},
    )

    assert validate_before_decode(msg, (1, 2, 3), ('json',)) == 3


def test_rejects_unsupported_protocol_before_decode():
    msg = message(protocol=3)
    msg.decode.side_effect = AssertionError('body must not be decoded')

    with pytest.raises(IncompatibleTaskProtocol) as exc_info:
        validate_before_decode(msg, (1, 2), ('json',))

    assert exc_info.value.reason == 'protocol-unsupported'
    assert exc_info.value.protocol == 3
    msg.decode.assert_not_called()


def test_rejects_disallowed_serializer_before_decode():
    msg = message(protocol=3, content_type='application/x-msgpack')

    with pytest.raises(IncompatibleTaskProtocol) as exc_info:
        validate_before_decode(msg, (1, 2, 3), ('json',))

    assert exc_info.value.reason == 'serializer-disallowed'


def test_rejects_unknown_or_unencoded_serializer_before_decode():
    msg = message(content_type='application/x-custom')

    with pytest.raises(IncompatibleTaskProtocol) as exc_info:
        validate_before_decode(msg, (1, 2), ('application/x-custom',))
    assert exc_info.value.reason == 'serializer-unknown'

    msg = message()
    msg.content_encoding = None
    with pytest.raises(IncompatibleTaskProtocol) as exc_info:
        validate_before_decode(msg, (1, 2), ('json',))
    assert exc_info.value.reason == 'content-encoding-missing'


@pytest.mark.parametrize((
    'headers', 'reason'
), (
    ({'id': None, 'protocol': 3}, 'required-field-missing'),
    ({'protocol': 3, 'root_id': None}, 'required-field-missing'),
    (
        {'protocol': 3, 'root_id': 'task-id', 'argsrepr': '()',
         'kwargsrepr': '{}', 'stamped_headers': [], 'stamps': {},
         'custom': 'value'},
        'extra-field',
    ),
    (
        {'protocol': 3, 'root_id': 'task-id', 'argsrepr': '()',
         'kwargsrepr': '{}', 'stamped_headers': ['custom'],
         'stamps': {}},
        'required-field-missing',
    ),
))
def test_rejects_invalid_protocol_3_envelope(headers, reason):
    msg = message(**headers)

    with pytest.raises(IncompatibleTaskProtocol) as exc_info:
        validate_before_decode(msg, (1, 2, 3), ('json',))

    assert exc_info.value.reason == reason


def test_announced_stamped_headers_are_allowed():
    msg = message(
        protocol=3, root_id='task-id', argsrepr='()', kwargsrepr='{}',
        stamped_headers=['custom'], stamps={'custom': 'value'},
        custom='value',
    )

    assert validate_before_decode(msg, (1, 2, 3), ('json',)) == 3


@pytest.mark.parametrize(('payload', 'reason'), (
    (((), {}), 'body-shape'),
    (((), {}, {}, 'extra'), 'body-shape'),
    (('args', {}, {}), 'args-type'),
    (((), [], {}), 'kwargs-type'),
    (((), {}, {'unknown': True}), 'extra-field'),
))
def test_rejects_invalid_protocol_3_payload_without_partial_args(payload, reason):
    with pytest.raises(IncompatibleTaskProtocol) as exc_info:
        validate_payload(3, payload)

    assert exc_info.value.reason == reason


def test_v1_payload_requires_task_identity_and_arguments():
    with pytest.raises(IncompatibleTaskProtocol) as exc_info:
        validate_payload(1, {'args': (), 'kwargs': {}})

    assert exc_info.value.reason == 'required-field-missing'


def test_hybrid_v1_headers_remain_protocol_one():
    msg = message(content_type='application/x-python-serialize')
    msg.headers = {'custom': 'value'}

    assert validate_before_decode(msg, (1, 2), ('pickle',)) == 1


def test_invalid_protocol_header_type_rejected():
    msg = message(protocol='three')

    with pytest.raises(IncompatibleTaskProtocol) as exc_info:
        validate_before_decode(msg, (1, 2, 3), ('json',))

    assert exc_info.value.reason == 'protocol-invalid'
