"""Task message protocol negotiation and validation."""

from collections.abc import Mapping, Sequence

from kombu.serialization import TRUSTED_CONTENT, registry

from celery.exceptions import TaskProtocolError

TASK_PROTOCOL_HEADER = 'task_protocol'
SUPPORTED_TASK_PROTOCOLS = frozenset((1, 2, 3))
LATEST_TASK_PROTOCOL = 3

V2_REQUIRED_HEADERS = frozenset(('id', 'task'))
V3_REQUIRED_HEADERS = frozenset((
    'id', 'task', 'root_id', 'argsrepr', 'kwargsrepr',
))
V2_KNOWN_HEADERS = frozenset((
    'lang', 'task', 'id', 'shadow', 'eta', 'expires', 'group',
    'group_index', 'retries', 'timelimit', 'root_id', 'parent_id',
    'argsrepr', 'kwargsrepr', 'origin', 'ignore_result',
    'replaced_task_nesting', 'stamped_headers', 'stamps', 'meth',
    'compression', TASK_PROTOCOL_HEADER,
))
V3_KNOWN_HEADERS = V2_KNOWN_HEADERS
PROTOCOL3_BODY_MARKER = 'celery_protocol3'
PROTOCOL3_EMBED_KEYS = frozenset((
    'callbacks', 'errbacks', 'chain', 'chord',
))


def task_protocol_from_headers(headers):
    if not isinstance(headers, Mapping):
        return None
    if 'task' not in headers:
        return 1
    return headers.get(TASK_PROTOCOL_HEADER, 2)


def validate_headers_before_decode(headers, accepted_protocols, strict_headers=False):
    if headers is None or not isinstance(headers, Mapping):
        if 1 not in accepted_protocols:
            raise TaskProtocolError(
                'version_mismatch',
                'protocol 1 is not accepted by this worker',
                protocol=1, stage='decode',
            )
        return 1

    if 'task' in headers:
        protocol = headers.get(TASK_PROTOCOL_HEADER, 2)
        if not isinstance(protocol, int) or isinstance(protocol, bool):
            raise TaskProtocolError(
                'invalid_version', f'{TASK_PROTOCOL_HEADER}={protocol!r}',
                protocol=protocol, stage='decode',
            )
        if protocol not in SUPPORTED_TASK_PROTOCOLS:
            raise TaskProtocolError(
                'unsupported_version', f'protocol {protocol} is not supported',
                protocol=protocol, stage='decode',
            )
        if protocol not in accepted_protocols:
            raise TaskProtocolError(
                'version_mismatch',
                f'protocol {protocol} is not accepted by this worker',
                protocol=protocol, stage='decode',
            )
        _validate_common_headers(headers, protocol, strict_headers)
        return protocol
    if 1 not in accepted_protocols:
        raise TaskProtocolError(
            'version_mismatch',
            'protocol 1 is not accepted by this worker',
            protocol=1, stage='decode',
        )
    return 1


def validate_content_type_before_decode(message, protocol, accepted_protocols=None):
    content_type = message.content_type or 'application/data'
    accept = message.accept
    if (
        isinstance(accept, (set, frozenset, list, tuple))
        and
        accept is not None
        and content_type not in TRUSTED_CONTENT
        and content_type not in accept
    ):
        raise TaskProtocolError(
            'serializer_not_allowed',
            f'content type {content_type!r} is not in {sorted(accept)!r}',
            stage='decode',
        )
    if (
        content_type not in TRUSTED_CONTENT
        and content_type not in registry._decoders
    ):
        raise TaskProtocolError(
            'serializer_unknown',
            f'no decoder installed for content type {content_type!r}',
            stage='decode',
        )
    if protocol == 3 and not message.content_encoding:
        raise TaskProtocolError(
            'missing_content_encoding',
            'content_encoding is required for task messages',
            stage='decode',
        )


def validate_decoded_body(body, protocol, strict_headers=False):
    if protocol == 1:
        if not isinstance(body, Mapping):
            raise TaskProtocolError(
                'invalid_body', 'protocol 1 body must be a mapping',
                protocol=1, stage='decode',
            )
        _require_body_fields(body, ('id', 'task', 'args', 'kwargs'), 1)
        return body

    if not isinstance(body, Sequence) or isinstance(body, (str, bytes)):
        raise TaskProtocolError(
            'invalid_body', f'protocol {protocol} body must be a sequence',
            protocol=protocol, stage='decode',
        )
    if protocol == 3:
        if len(body) != 4:
            raise TaskProtocolError(
                'invalid_body',
                f'protocol 3 body must contain four items, got {len(body)}',
                protocol=3, stage='decode',
            )
        marker, args, kwargs, embed = body
        if marker != PROTOCOL3_BODY_MARKER:
            raise TaskProtocolError(
                'unsupported_version',
                f'missing protocol 3 body marker {PROTOCOL3_BODY_MARKER!r}',
                protocol=3, stage='decode',
            )
        _validate_args_kwargs_embed(args, kwargs, embed, 3, strict_headers)
        return args, kwargs, embed

    if len(body) != 3:
        raise TaskProtocolError(
            'invalid_body',
            f'protocol 2 body must contain three items, got {len(body)}',
            protocol=2, stage='decode',
        )
    args, kwargs, embed = body
    _validate_args_kwargs_embed(args, kwargs, embed, 2, strict_headers)
    return body


def _validate_common_headers(headers, protocol, strict_headers):
    required = V3_REQUIRED_HEADERS if protocol == 3 else V2_REQUIRED_HEADERS
    missing = sorted(name for name in required if headers.get(name) in (None, ''))
    if missing:
        raise TaskProtocolError(
            'missing_field', f'missing required headers: {", ".join(missing)}',
            protocol=protocol, stage='decode',
        )

    stamped = headers.get('stamped_headers')
    if stamped is not None:
        if not isinstance(stamped, Sequence) or isinstance(stamped, (str, bytes)):
            raise TaskProtocolError(
                'invalid_field', 'stamped_headers must be a sequence',
                protocol=protocol, stage='decode',
            )
        stamps = headers.get('stamps')
        if not isinstance(stamps, Mapping):
            raise TaskProtocolError(
                'invalid_field', 'stamps must be a mapping when stamped_headers is set',
                protocol=protocol, stage='decode',
            )
        missing_stamps = sorted(name for name in stamped if name not in stamps)
        if missing_stamps:
            raise TaskProtocolError(
                'missing_field',
                f'missing stamped header values: {", ".join(missing_stamps)}',
                protocol=protocol, stage='decode',
            )

    if strict_headers or protocol == 3:
        allowed = set(V3_KNOWN_HEADERS if protocol == 3 else V2_KNOWN_HEADERS)
        allowed.update(stamped or ())
        extra = sorted(name for name in headers
                       if name not in allowed and not name.startswith('x-'))
        if extra:
            raise TaskProtocolError(
                'extra_field', f'unknown task headers: {", ".join(extra)}',
                protocol=protocol, stage='decode',
            )


def _require_body_fields(body, fields, protocol):
    missing = sorted(name for name in fields if name not in body)
    if missing:
        raise TaskProtocolError(
            'missing_field',
            f'missing required body fields: {", ".join(missing)}',
            protocol=protocol, stage='decode',
        )


def _validate_args_kwargs_embed(args, kwargs, embed, protocol, strict_headers):
    if not isinstance(args, Sequence) or isinstance(args, (str, bytes)):
        raise TaskProtocolError(
            'invalid_field', 'args must be a sequence',
            protocol=protocol, stage='decode',
        )
    if not isinstance(kwargs, Mapping):
        raise TaskProtocolError(
            'invalid_field', 'kwargs must be a mapping',
            protocol=protocol, stage='decode',
        )
    if embed is not None and not isinstance(embed, Mapping):
        raise TaskProtocolError(
            'invalid_field', 'embedded task metadata must be a mapping',
            protocol=protocol, stage='decode',
        )
    if embed is not None:
        extra = sorted(set(embed) - PROTOCOL3_EMBED_KEYS)
        if extra:
            raise TaskProtocolError(
                'extra_field',
                f'unknown embedded fields: {", ".join(extra)}',
                protocol=protocol, stage='decode',
            )
