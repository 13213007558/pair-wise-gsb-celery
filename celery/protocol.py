"""Task message protocol negotiation and compatibility policy."""

from collections.abc import Mapping

from kombu.serialization import prepare_accept_content, registry

from celery.exceptions import IncompatibleTaskProtocol

PROTOCOL_HEADER = 'protocol'

V1_REQUIRED_FIELDS = frozenset({'task', 'id', 'args', 'kwargs'})
V2_REQUIRED_HEADERS = frozenset({'task', 'id'})
V3_REQUIRED_HEADERS = V2_REQUIRED_HEADERS | {
    'root_id', 'argsrepr', 'kwargsrepr', 'stamped_headers', 'stamps',
}
V2_EMBED_FIELDS = frozenset({'callbacks', 'errbacks', 'chain', 'chord'})
RESERVED_HEADERS = {
    'lang', 'task', 'id', 'shadow', 'eta', 'expires', 'group',
    'group_index', 'retries', 'timelimit', 'root_id', 'parent_id',
    'argsrepr', 'kwargsrepr', 'origin', 'ignore_result',
    'replaced_task_nesting', 'stamped_headers', 'stamps', 'meth',
    'compression', PROTOCOL_HEADER,
}
BROKER_HEADERS = {
    'x-death', 'x-first-death-exchange', 'x-first-death-queue',
    'x-first-death-reason', 'x-last-death-exchange', 'x-last-death-queue',
    'x-last-death-reason',
}


def protocol_from_message(message):
    """Return the explicit protocol version, or infer legacy v2."""
    headers = message.headers
    if not isinstance(headers, Mapping):
        raise IncompatibleTaskProtocol(
            'headers-mapping', protocol=None,
            headers_type=type(headers).__name__,
        )
    if not headers:
        return 1
    if PROTOCOL_HEADER in headers:
        protocol = headers[PROTOCOL_HEADER]
        if isinstance(protocol, bool):
            raise IncompatibleTaskProtocol(
                'protocol-invalid', protocol=protocol,
            )
        if not isinstance(protocol, int):
            try:
                protocol = int(protocol)
            except (TypeError, ValueError):
                raise IncompatibleTaskProtocol(
                    'protocol-invalid', protocol=protocol,
                )
        return protocol
    if 'task' in headers and 'id' in headers:
        return 2
    return 1


def validate_before_decode(message, supported_versions, accepted_content):
    version = protocol_from_message(message)
    if version not in set(supported_versions):
        raise IncompatibleTaskProtocol(
            'protocol-unsupported', protocol=version,
            supported_versions=tuple(supported_versions),
        )

    content_type = message.content_type or 'application/data'
    if content_type not in prepare_accept_content(accepted_content):
        raise IncompatibleTaskProtocol(
            'serializer-disallowed', protocol=version,
            content_type=content_type,
            accepted_content=tuple(accepted_content),
        )
    if content_type not in registry.type_to_name:
        raise IncompatibleTaskProtocol(
            'serializer-unknown', protocol=version,
            content_type=content_type,
        )
    if not message.content_encoding:
        raise IncompatibleTaskProtocol(
            'content-encoding-missing', protocol=version,
        )

    if version == 1:
        return version

    headers = message.headers
    required = V3_REQUIRED_HEADERS if version >= 3 else V2_REQUIRED_HEADERS
    missing = sorted(name for name in required if headers.get(name) is None)
    if missing:
        raise IncompatibleTaskProtocol(
            'required-field-missing', protocol=version,
            fields=missing,
        )

    correlation_id = message.properties.get('correlation_id')
    if not correlation_id or correlation_id != headers.get('id'):
        raise IncompatibleTaskProtocol(
            'correlation-id-mismatch', protocol=version,
            correlation_id=correlation_id, task_id=headers.get('id'),
        )

    if version >= 3:
        stamped_headers = headers.get('stamped_headers') or ()
        if not isinstance(stamped_headers, (list, tuple)):
            raise IncompatibleTaskProtocol(
                'stamped-headers-invalid', protocol=version,
                stamped_headers_type=type(stamped_headers).__name__,
            )
        stamps = headers.get('stamps')
        if not isinstance(stamps, Mapping):
            raise IncompatibleTaskProtocol(
                'stamps-invalid', protocol=version,
                stamps_type=type(stamps).__name__,
            )
        missing_stamps = sorted(
            [name for name in stamped_headers if name not in stamps],
        )
        if missing_stamps:
            raise IncompatibleTaskProtocol(
                'required-field-missing', protocol=version,
                fields=[f'stamps.{name}' for name in missing_stamps],
            )
        allowed = RESERVED_HEADERS | BROKER_HEADERS | set(stamped_headers)
        extra = sorted(name for name in headers if name not in allowed)
        if extra:
            raise IncompatibleTaskProtocol(
                'extra-field', protocol=version, fields=extra,
            )

    return version


def validate_payload(version, payload):
    if version == 1:
        if not isinstance(payload, Mapping):
            raise IncompatibleTaskProtocol(
                'body-type', protocol=version, body_type=type(payload).__name__,
            )
        missing = sorted(
            [name for name in V1_REQUIRED_FIELDS if name not in payload],
        )
        if missing:
            raise IncompatibleTaskProtocol(
                'required-field-missing', protocol=version, fields=missing,
            )
        args = payload.get('args')
        kwargs = payload.get('kwargs')
    else:
        if not isinstance(payload, tuple) or len(payload) != 3:
            raise IncompatibleTaskProtocol(
                'body-shape', protocol=version,
                body_type=type(payload).__name__,
            )
        args, kwargs, embed = payload
        if not isinstance(embed, Mapping):
            raise IncompatibleTaskProtocol(
                'embed-type', protocol=version,
                embed_type=type(embed).__name__,
            )
        if version >= 3:
            extra = sorted(
                [name for name in embed if name not in V2_EMBED_FIELDS],
            )
            if extra:
                raise IncompatibleTaskProtocol(
                    'extra-field', protocol=version,
                    fields=[f'embed.{name}' for name in extra],
                )

    if not isinstance(args, (list, tuple)):
        raise IncompatibleTaskProtocol(
            'args-type', protocol=version, args_type=type(args).__name__,
        )
    if not isinstance(kwargs, Mapping):
        raise IncompatibleTaskProtocol(
            'kwargs-type', protocol=version,
            kwargs_type=type(kwargs).__name__,
        )
