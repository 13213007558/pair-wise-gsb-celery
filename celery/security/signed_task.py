"""Signed task message format (``signed-task``, version 1).

This module implements an explicitly opt-in signed task message format.
Unlike the legacy ``auth`` serializer -- which signs only the serialized
message body -- this format binds the *execution semantics* of a task
message into the signature:

* the task name and task id (and every other task header),
* the message properties (correlation id, reply-to),
* the serialized argument body,
* the content type and content encoding of that body, and
* the ``callbacks``/``errbacks``/``chain``/``chord`` embed (part of the
  body) that controls how execution continues after the task runs.

The signed payload is a JSON envelope with an explicit ``format`` marker
and ``version`` number.  The bytes that are signed are the canonical JSON
encoding (keys sorted, no insignificant whitespace, UTF-8) of the
envelope without the ``signature`` field, so the encoding is unambiguous
and independent of the producer's JSON library settings.

A message of this format is only ever dispatched after the signature has
been verified; verification failure is final and never falls back to
treating the message as an unsigned or legacy one.

Note:
    The signature protects message *integrity* and the *identity of the
    signer*.  It does not, by itself, prevent *replay* of a validly
    signed message: detecting a re-delivered task id requires additional
    state (e.g. a deduplication store, result backend bookkeeping, or
    idempotent task design), which is out of scope for this layer.
"""
import json

from kombu.serialization import dumps, loads, registry
from kombu.utils.encoding import ensure_bytes

from celery.exceptions import SecurityError
from celery.utils.serialization import b64decode, b64encode

__all__ = (
    'SIGNED_TASK_SERIALIZER',
    'SIGNED_TASK_FORMAT',
    'SIGNED_TASK_VERSION',
    'SIGNED_TASK_CONTENT_TYPE',
    'SIGNED_TASK_CONTENT_ENCODING',
    'SignedTaskSerializer',
    'canonical_dumps',
    'register_signed_task',
)

#: Name of the serializer as registered in the Kombu serializer registry
#: and used for the ``task_serializer``/``accept_content`` settings.
SIGNED_TASK_SERIALIZER = 'signed-task'

#: Value of the ``format`` field in the signed envelope.
SIGNED_TASK_FORMAT = 'celery-signed-task'

#: Only version of the envelope understood by this implementation.
SIGNED_TASK_VERSION = 1

#: Content type used on the wire for signed task messages.
SIGNED_TASK_CONTENT_TYPE = 'application/vnd.celery.signed-task.v1+json'

#: Content encoding used on the wire for signed task messages.
SIGNED_TASK_CONTENT_ENCODING = 'utf-8'

#: Fields every signed task envelope must contain.
_ENVELOPE_FIELDS = frozenset({
    'format', 'version', 'signer',
    'content_type', 'content_encoding',
    'headers', 'properties', 'body',
})


def canonical_dumps(obj):
    """Return the canonical byte encoding of ``obj``.

    Canonical JSON: keys sorted, no insignificant whitespace, UTF-8.
    This makes the signed byte representation unambiguous and independent
    of the JSON library defaults of producer and consumer.
    """
    return json.dumps(
        obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
    ).encode('utf-8')


class SignedTaskSerializer:
    """Serializer signing whole task messages (headers, properties, body).

    Arguments:
        key (~celery.security.key.PrivateKey): Key used to sign messages.
        cert (~celery.security.certificate.Certificate): Certificate of
            the signer, referenced by id in the envelope.
        cert_store (~celery.security.certificate.CertStore): Store used
            to look up signer certificates when verifying.
        digest (str): Digest algorithm used for signing (e.g. ``sha256``).
        serializer (str): Serializer used for the task body inside the
            envelope (default ``json``).
    """

    def __init__(self, key=None, cert=None, cert_store=None,
                 digest=None, serializer='json'):
        from .utils import get_digest_algorithm
        self._key = key
        self._cert = cert
        self._cert_store = cert_store
        self._digest = get_digest_algorithm(digest or 'sha256')
        self._serializer = serializer

    def serialize(self, data):
        """Serialize and sign a task message structure.

        ``data`` must be a mapping with ``headers`` (task headers,
        including ``task`` and ``id``), ``properties`` (message
        properties) and ``body`` (the protocol 2 task body).
        """
        assert self._key is not None
        assert self._cert is not None
        try:
            headers = data['headers']
            properties = data['properties']
            body = data['body']
        except (TypeError, KeyError) as exc:
            raise SecurityError(
                'Signed task messages require headers, properties '
                f'and body: {exc!r}')
        if not isinstance(headers, dict) or not isinstance(properties, dict):
            raise SecurityError(
                'Signed task headers and properties must be mappings')
        if 'task' not in headers or 'id' not in headers:
            raise SecurityError(
                "Signed task headers must contain 'task' and 'id'")

        content_type, content_encoding, raw_body = dumps(
            body, serializer=self._serializer)
        # What we sign is the serialized body, not the body itself,
        # so the receiver verifies before decoding (avoiding potential
        # flaws in the decoding step).  Unlike the legacy auth
        # serializer, the content type/encoding of that body and the
        # task headers are part of the signed payload.
        envelope = {
            'format': SIGNED_TASK_FORMAT,
            'version': SIGNED_TASK_VERSION,
            'signer': self._cert.get_id(),
            'content_type': content_type,
            'content_encoding': content_encoding,
            'headers': headers,
            'properties': properties,
            'body': b64encode(ensure_bytes(raw_body)),
        }
        signature = self._key.sign(canonical_dumps(envelope), self._digest)
        return canonical_dumps(
            dict(envelope, signature=b64encode(signature))).decode('utf-8')

    def deserialize(self, data):
        """Verify a signed task message and return its contents.

        Returns a mapping with verified ``headers``, ``properties`` and
        the deserialized ``body``.  Raises
        :exc:`~celery.exceptions.SecurityError` for any format, version
        or signature mismatch -- callers must not fall back to treating
        the message as unsigned.
        """
        assert self._cert_store is not None
        try:
            envelope = json.loads(data)
        except (TypeError, ValueError) as exc:
            raise SecurityError(f'Invalid signed task message: {exc!r}')
        if not isinstance(envelope, dict):
            raise SecurityError('Signed task message is not an object')
        if envelope.get('format') != SIGNED_TASK_FORMAT:
            raise SecurityError(
                f"Not a signed task message: {envelope.get('format')!r}")
        if envelope.get('version') != SIGNED_TASK_VERSION:
            raise SecurityError(
                'Unsupported signed task message version: '
                f"{envelope.get('version')!r}")
        try:
            signature = b64decode(envelope.pop('signature'))
        except KeyError:
            raise SecurityError('Signed task message has no signature')
        except (TypeError, ValueError) as exc:
            raise SecurityError(f'Invalid task message signature: {exc!r}')
        missing = _ENVELOPE_FIELDS - envelope.keys()
        if missing:
            raise SecurityError(
                f'Signed task message misses fields: {sorted(missing)!r}')

        signed_bytes = canonical_dumps(envelope)
        self._cert_store[envelope['signer']].verify(
            signed_bytes, signature, self._digest)

        headers = envelope['headers']
        properties = envelope['properties']
        if not isinstance(headers, dict) or not isinstance(properties, dict):
            raise SecurityError(
                'Signed task headers and properties must be mappings')
        try:
            raw_body = b64decode(envelope['body'])
        except (TypeError, ValueError) as exc:
            raise SecurityError(f'Invalid signed task body: {exc!r}')
        body = loads(raw_body, envelope['content_type'],
                     envelope['content_encoding'], force=True)
        return {
            'headers': headers,
            'properties': properties,
            'body': body,
        }


def register_signed_task(key=None, key_password=None, cert=None, store=None,
                         digest='sha256', serializer='json'):
    """Register the ``signed-task`` serializer in the Kombu registry."""
    from .certificate import Certificate, FSCertStore
    from .key import PrivateKey
    s = SignedTaskSerializer(key and PrivateKey(key, password=key_password),
                             cert and Certificate(cert),
                             store and FSCertStore(store),
                             digest, serializer=serializer)
    registry.register(SIGNED_TASK_SERIALIZER, s.serialize, s.deserialize,
                      content_type=SIGNED_TASK_CONTENT_TYPE,
                      content_encoding=SIGNED_TASK_CONTENT_ENCODING)
