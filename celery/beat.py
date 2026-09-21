"""The periodic task scheduler."""

import copy
import dbm
import errno
import heapq
import os
import shelve
import sys
import time
import traceback
from calendar import timegm
from collections import namedtuple
from functools import total_ordering
from pickle import UnpicklingError
from threading import Event, Thread

from billiard import ensure_multiprocessing
from billiard.common import reset_signals
from billiard.context import Process
from kombu.utils.functional import maybe_evaluate, reprcall
from kombu.utils.objects import cached_property

from . import __version__, platforms, signals
from .exceptions import reraise
from .schedules import crontab, maybe_schedule
from .utils.functional import is_numeric_value
from .utils.imports import load_extension_class_names, symbol_by_name
from .utils.log import get_logger, iter_open_logger_fds
from .utils.time import humanize_seconds, maybe_make_aware

__all__ = (
    'SchedulingError', 'ScheduleEntry', 'Scheduler',
    'PersistentScheduler', 'Service', 'EmbeddedService',
)

event_t = namedtuple(
    'event_t', ('time', 'priority', 'entry', 'generation', 'version')
)
event_t.__new__.__defaults__ = (None, None)

dispatch_t = namedtuple('dispatch_t', ('entry', 'attempts', 'sent'))
dispatch_t.__new__.__defaults__ = (0, False)

logger = get_logger(__name__)
debug, info, error, warning = (logger.debug, logger.info,
                               logger.error, logger.warning)

DEFAULT_MAX_INTERVAL = 300  # 5 minutes
DISPATCH_RETRY_MAX_INTERVAL = 300


class SchedulingError(Exception):
    """An error occurred while scheduling a task."""


class BeatLazyFunc:
    """A lazy function declared in 'beat_schedule' and called before sending to worker.

    Example:

        beat_schedule = {
            'test-every-5-minutes': {
                'task': 'test',
                'schedule': 300,
                'kwargs': {
                    "current": BeatCallBack(datetime.datetime.now)
                }
            }
        }

    """

    def __init__(self, func, *args, **kwargs):
        self._func = func
        self._func_params = {
            "args": args,
            "kwargs": kwargs
        }

    def __call__(self):
        return self.delay()

    def delay(self):
        return self._func(*self._func_params["args"], **self._func_params["kwargs"])


@total_ordering
class ScheduleEntry:
    """An entry in the scheduler.

    Arguments:
        name (str): see :attr:`name`.
        schedule (~celery.schedules.schedule): see :attr:`schedule`.
        args (Tuple): see :attr:`args`.
        kwargs (Dict): see :attr:`kwargs`.
        options (Dict): see :attr:`options`.
        last_run_at (~datetime.datetime): see :attr:`last_run_at`.
        total_run_count (int): see :attr:`total_run_count`.
        relative (bool): Is the time relative to when the server starts?
    """

    #: The task name
    name = None

    #: The schedule (:class:`~celery.schedules.schedule`)
    schedule = None

    #: Positional arguments to apply.
    args = None

    #: Keyword arguments to apply.
    kwargs = None

    #: Task execution options.
    options = None

    #: The time and date of when this task was last scheduled.
    last_run_at = None

    #: Total number of times this task has been scheduled.
    total_run_count = 0

    #: Generation of the entry definition.
    generation = 1

    #: Version of the entry's scheduling state.
    version = 0

    def __init__(self, name=None, task=None, last_run_at=None,
                 total_run_count=None, schedule=None, args=(), kwargs=None,
                 options=None, relative=False, app=None, generation=None,
                 version=None):
        self.app = app
        self.name = name
        self.task = task
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.options = options if options else {}
        self.schedule = maybe_schedule(schedule, relative, app=self.app)
        self.last_run_at = last_run_at or self.default_now()
        self.total_run_count = total_run_count or 0
        self.generation = generation if generation is not None else 1
        self.version = (
            version if version is not None else self.total_run_count
        )

    def default_now(self):
        return self.schedule.now() if self.schedule else self.app.now()
    _default_now = default_now  # compat

    def _next_instance(self, last_run_at=None):
        """Return new instance, with date and count fields updated."""
        return self.__class__(**dict(
            self,
            last_run_at=last_run_at or self.default_now(),
            total_run_count=self.total_run_count + 1,
            version=self.version + 1,
        ))
    __next__ = next = _next_instance  # for 2to3

    def __reduce__(self):
        return self.__class__, (
            self.name, self.task, self.last_run_at, self.total_run_count,
            self.schedule, self.args, self.kwargs, self.options,
            False, None, self.generation, self.version,
        )

    def update(self, other):
        """Update values from another entry.

        Will only update "editable" fields:
            ``task``, ``schedule``, ``args``, ``kwargs``, ``options``.
        """
        self.__dict__.update({
            'task': other.task, 'schedule': other.schedule,
            'args': other.args, 'kwargs': other.kwargs,
            'options': other.options,
        })
        self.generation += 1

    def is_due(self):
        """See :meth:`~celery.schedules.schedule.is_due`."""
        return self.schedule.is_due(self.last_run_at)

    def __iter__(self):
        return iter(vars(self).items())

    def __repr__(self):
        return '<{name}: {0.name} {call} {0.schedule}'.format(
            self,
            call=reprcall(self.task, self.args or (), self.kwargs or {}),
            name=type(self).__name__,
        )

    def __lt__(self, other):
        if isinstance(other, ScheduleEntry):
            # How the object is ordered doesn't really matter, as
            # in the scheduler heap, the order is decided by the
            # preceding members of the tuple ``(time, priority, entry)``.
            #
            # If all that's left to order on is the entry then it can
            # just as well be random.
            return id(self) < id(other)
        return NotImplemented

    def editable_fields_equal(self, other):
        for attr in ('task', 'args', 'kwargs', 'options', 'schedule'):
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __eq__(self, other):
        """Test schedule entries equality.

        Will only compare "editable" fields:
        ``task``, ``schedule``, ``args``, ``kwargs``, ``options``.
        """
        return self.editable_fields_equal(other)


def _evaluate_entry_args(entry_args):
    if not entry_args:
        return []
    return [
        v() if isinstance(v, BeatLazyFunc) else v
        for v in entry_args
    ]


def _evaluate_entry_kwargs(entry_kwargs):
    if not entry_kwargs:
        return {}
    return {
        k: v() if isinstance(v, BeatLazyFunc) else v
        for k, v in entry_kwargs.items()
    }


class Scheduler:
    """Scheduler for periodic tasks.

    The :program:`celery beat` program may instantiate this class
    multiple times for introspection purposes, but then with the
    ``lazy`` argument set.  It's important for subclasses to
    be idempotent when this argument is set.

    Arguments:
        schedule (~celery.schedules.schedule): see :attr:`schedule`.
        max_interval (int): see :attr:`max_interval`.
        lazy (bool): Don't set up the schedule.
    """

    Entry = ScheduleEntry

    #: The schedule dict/shelve.
    schedule = None

    #: Maximum time to sleep between re-checking the schedule.
    max_interval = DEFAULT_MAX_INTERVAL

    #: How often to sync the schedule (3 minutes by default)
    sync_every = 3 * 60

    #: How many tasks can be called before a sync is forced.
    sync_every_tasks = None

    _last_sync = None
    _tasks_since_sync = 0

    logger = logger  # compat

    def __init__(self, app, schedule=None, max_interval=None,
                 Producer=None, lazy=False, sync_every_tasks=None, **kwargs):
        self.app = app
        self.data = maybe_evaluate({} if schedule is None else schedule)
        self.max_interval = (max_interval or
                             app.conf.beat_max_loop_interval or
                             self.max_interval)
        self.Producer = Producer or app.amqp.Producer
        self._heap = None
        self.old_schedulers = None
        self._schedule_snapshot = None
        self._dispatches = {}
        self._dispatch_retried_at = {}
        self.sync_every_tasks = (
            app.conf.beat_sync_every if sync_every_tasks is None
            else sync_every_tasks)
        if not lazy:
            self.setup_schedule()

    def install_default_entries(self, data):
        entries = {}
        if self.app.conf.result_expires and \
                not self.app.backend.supports_autoexpire:
            if 'celery.backend_cleanup' not in data:
                entries['celery.backend_cleanup'] = {
                    'task': 'celery.backend_cleanup',
                    'schedule': crontab('0', '4', '*'),
                    'options': {'expires': 12 * 3600}}
        self.update_from_dict(entries)

    def apply_entry(self, entry, producer=None):
        info('Scheduler: Sending due task %s (%s)', entry.name, entry.task)
        try:
            result = self._send_entry(entry, producer=producer)
        except Exception as exc:  # pylint: disable=broad-except
            error('Message Error: %s\n%s',
                  exc, traceback.format_stack(), exc_info=True)
            return False
        else:
            if result and hasattr(result, 'id'):
                debug('%s sent. id->%s', entry.task, result.id)
            else:
                debug('%s sent.', entry.task)
            return True

    def adjust(self, n, drift=-0.010):
        if n and n > 0:
            return n + drift
        return n

    def is_due(self, entry):
        return entry.is_due()

    def _when(self, entry, next_time_to_run, mktime=timegm):
        """Return a utc timestamp, make sure heapq in correct order."""
        adjust = self.adjust

        as_now = maybe_make_aware(entry.default_now())

        return (mktime(as_now.utctimetuple()) +
                as_now.microsecond / 1e6 +
                (adjust(next_time_to_run) or 0))

    def populate_heap(self, event_t=event_t, heapify=heapq.heapify):
        """Populate the heap with the data contained in the schedule."""
        priority = 5
        self._heap = []
        for entry in self.schedule.values():
            is_due, next_call_delay = entry.is_due()
            self._heap.append(event_t(
                self._when(
                    entry,
                    0 if is_due else next_call_delay
                ) or 0,
                priority, entry, entry.generation, entry.version
            ))
        heapify(self._heap)

    def _entry_generation(self, entry):
        return getattr(entry, 'generation', None)

    def _entry_version(self, entry):
        return getattr(
            entry, 'version', getattr(entry, 'total_run_count', None),
        )

    def _event_is_current(self, event):
        entry = event.entry
        current = self.schedule.get(entry.name)
        if event.generation is None and event.version is None:
            return current is entry
        return (
            current is entry and
            event.generation == self._entry_generation(current) and
            event.version == self._entry_version(current)
        )

    def _entry_state(self, entry):
        return (
            entry.generation,
            entry.version,
            entry.last_run_at,
            entry.total_run_count,
            entry.task,
            entry.args,
            tuple(entry.kwargs.items()),
            tuple(entry.options.items()),
            entry.schedule,
        )

    def _schedule_signature(self):
        return (
            self.app.conf.timezone,
            self.app.conf.enable_utc,
            tuple(
                (name, id(entry)) + self._entry_state(entry)
                for name, entry in sorted(self.schedule.items())
            ),
        )

    def _ensure_entry_identity(self, entry):
        if not hasattr(entry, 'generation'):
            entry.generation = 1
        if not hasattr(entry, 'version'):
            entry.version = getattr(entry, 'total_run_count', 0)

    def _bump_entry_generation(self, entry):
        self._ensure_entry_identity(entry)
        entry.generation += 1

    def _refresh_schedule(self):
        timezone_changed = False
        if self._schedule_snapshot is not None:
            old_timezone, old_utc_enabled = self._schedule_snapshot[:2]
            timezone_changed = (
                old_timezone != self.app.conf.timezone or
                old_utc_enabled != self.app.conf.enable_utc
            )

        for entry in self.schedule.values():
            self._ensure_entry_identity(entry)
            if timezone_changed:
                self._bump_entry_generation(entry)

        signature = self._schedule_signature()
        if (
            self._heap is not None and
            self.old_schedulers is self.schedule and
            (
                self._schedule_snapshot is None or
                self._schedule_snapshot == signature
            )
        ):
            return

        if self._heap is None or self._schedule_snapshot != signature:
            if self._schedule_snapshot is not None:
                old_entries = {
                    item[0]: item[2:]
                    for item in self._schedule_snapshot[2]
                }
                for name, entry in self.schedule.items():
                    old = old_entries.get(name)
                    if old is None:
                        continue
                    new_state = self._entry_state(entry)
                    old_version = old[1]
                    old_identity = old[2:]
                    new_version = new_state[1]
                    new_identity = new_state[2:]
                    if (
                        old[0] == new_state[0] and
                        (
                            (old_identity != new_identity) or
                            (
                                old_identity == new_identity and
                                old_version != new_version
                            )
                        )
                    ):
                        self._bump_entry_generation(entry)
                signature = self._schedule_signature()

            self.old_schedulers = copy.copy(self.schedule)
            self._schedule_snapshot = signature
            self.populate_heap()

    def _remove_stale_events(self, heappop=heapq.heappop):
        heap = self._heap
        while heap:
            event = heap[0]
            if (
                event.entry.name not in self._dispatches and
                self._event_is_current(event)
            ):
                break
            heappop(heap)

    def _dispatch_backoff(self, attempts):
        return min(2 ** attempts, DISPATCH_RETRY_MAX_INTERVAL)

    def _dispatch_retry_delay(self, key, now=None):
        now = now if now is not None else time.monotonic()
        attempts = self._dispatches[key].attempts
        delay = self._dispatch_backoff(attempts)
        self._dispatch_retried_at[key] = now + delay
        return delay

    def _dispatch_ready(self, key, now=None):
        now = now if now is not None else time.monotonic()
        return self._dispatch_retried_at.get(key, 0) <= now

    def _persist_dispatch_state(self):
        """Persist reservation and unacknowledged dispatch state."""

    def _persist_dispatches(self):
        """Persist unacknowledged dispatch records."""

    def _reserve_dispatch(self, entry):
        current = self.schedule.get(entry.name)
        if current is not entry:
            return None

        dispatch = dispatch_t(entry=entry)
        self._dispatches[entry.name] = dispatch

        try:
            self._persist_dispatches()
        except Exception:
            self._dispatches.pop(entry.name, None)
            self._schedule_snapshot = None
            raise

        next_entry = next(entry)
        self.schedule[entry.name] = next_entry
        self._schedule_snapshot = self._schedule_signature()
        try:
            self._persist_dispatch_state()
        except Exception:
            self.schedule[entry.name] = entry
            self._schedule_snapshot = None
            raise
        return dispatch, next_entry

    def _send_entry(self, entry, producer=None):
        entry_args = _evaluate_entry_args(entry.args)
        entry_kwargs = _evaluate_entry_kwargs(entry.kwargs)

        options = entry.options.copy()
        headers = options.setdefault('headers', {})
        headers['celery_beat_task'] = True
        headers.setdefault('celery_beat_generation', entry.generation)
        headers.setdefault('celery_beat_version', entry.version)

        task = self.app.tasks.get(entry.task)
        if task:
            return task.apply_async(entry_args, entry_kwargs,
                                    producer=producer, **options)
        return self.send_task(entry.task, entry_args, entry_kwargs,
                              producer=producer, **options)

    def _acknowledge_dispatch(self, key):
        dispatch = self._dispatches.get(key)
        if dispatch is None:
            return True
        if not dispatch.sent:
            return False

        self._persist_dispatch_state()
        self._dispatches.pop(key, None)
        self._dispatch_retried_at.pop(key, None)
        return True

    def _send_dispatch(self, key, dispatch):
        if not dispatch.sent:
            result = self.apply_entry(
                dispatch.entry, producer=self.producer,
            )
            if result is None:
                result = True
            if not result:
                dispatch = dispatch._replace(attempts=dispatch.attempts + 1)
                self._dispatches[key] = dispatch
                try:
                    self._persist_dispatches()
                except Exception:
                    pass
                return False
            dispatch = dispatch._replace(sent=True)
            self._dispatches[key] = dispatch
            self._tasks_since_sync += 1
            try:
                self._persist_dispatch_state()
            except Exception:
                pass
            if self.should_sync():
                try:
                    self._do_sync()
                except Exception:
                    pass

        try:
            return self._acknowledge_dispatch(key)
        except Exception:
            return False

    def _flush_dispatches(self, now=None):
        now = now if now is not None else time.monotonic()
        delays = []
        for key, dispatch in tuple(self._dispatches.items()):
            if not self._dispatch_ready(key, now):
                delays.append(self._dispatch_retried_at[key] - now)
                continue

            if self._send_dispatch(key, dispatch):
                continue
            delays.append(self._dispatch_retry_delay(key, now))

        if not self._dispatches:
            return None
        if delays:
            return min(max(min(delays), 0), self.max_interval)
        return 0

    # pylint disable=redefined-outer-name
    def tick(self, event_t=event_t, min=min, heappop=heapq.heappop,
             heappush=heapq.heappush):
        """Run a tick - one iteration of the scheduler.

        Executes one due task per call.

        Returns:
            float: preferred delay in seconds for next call.
        """
        max_interval = self.max_interval

        adjust = self.adjust
        monotonic_now = time.monotonic()
        self._refresh_schedule()
        self._remove_stale_events()

        H = self._heap
        retry_delay = self._flush_dispatches(monotonic_now)

        if not H:
            return retry_delay if retry_delay is not None else max_interval

        event = H[0]
        entry = event[2]
        if not self._event_is_current(event):
            return 0

        now = self._when(entry, 0)
        if event[0] > now:
            delay = min(event[0] - now, max_interval)
            if retry_delay is not None:
                return min(delay, retry_delay)
            return delay

        if entry.name in self._dispatches:
            verify = heappop(H)
            if verify is event:
                heappush(H, event)
            else:
                heappush(H, verify)
            return retry_delay if retry_delay is not None else 0

        is_due, next_time_to_run = self.is_due(entry)
        if is_due:
            verify = heappop(H)
            if verify is event:
                try:
                    reservation = self._reserve_dispatch(entry)
                except Exception:
                    heappush(H, event)
                    return min(self._dispatch_backoff(0), max_interval)
                if reservation is None:
                    return 0

                dispatch, next_entry = reservation
                heappush(H, event_t(
                    self._when(next_entry, next_time_to_run),
                    event[1], next_entry,
                    next_entry.generation, next_entry.version,
                ))
                if not self._send_dispatch(entry.name, dispatch):
                    delay = self._dispatch_backoff(
                        self._dispatches.get(
                            entry.name, dispatch,
                        ).attempts,
                    )
                    if entry.name in self._dispatches:
                        self._dispatch_retried_at[entry.name] = (
                            monotonic_now + delay
                        )
                return 0
            else:
                heappush(H, verify)
                return min(verify[0] - self._when(verify[2], 0), max_interval)

        # Heap says this entry should be ready by now, but the entry requests
        # to retry later.  Reheap it at that retry time, otherwise it just
        # sits on top and the entries behind it never get their turn.
        # https://github.com/celery/celery/issues/7649
        reschedule_delay = next_time_to_run if is_numeric_value(next_time_to_run) else max_interval
        verify = heappop(H)
        if verify is event:
            heappush(H, event_t(self._when(entry, reschedule_delay), event[1],
                                 entry, entry.generation, entry.version))
            # If another entry is now at the top, run it immediately.
            return 0 if H and H[0][2] is not entry else min(adjust(reschedule_delay), max_interval)
        else:
            heappush(H, verify)
            return min(verify[0] - self._when(verify[2], 0), max_interval)

    def schedules_equal(self, old_schedules, new_schedules):
        if old_schedules is new_schedules is None:
            return True
        if old_schedules is None or new_schedules is None:
            return False
        if set(old_schedules.keys()) != set(new_schedules.keys()):
            return False
        for name, old_entry in old_schedules.items():
            new_entry = new_schedules.get(name)
            if not new_entry:
                return False
            if new_entry != old_entry:
                return False
        return True

    def should_sync(self):
        return (
            (not self._last_sync or
             (time.monotonic() - self._last_sync) > self.sync_every) or
            (self.sync_every_tasks and
             self._tasks_since_sync >= self.sync_every_tasks)
        )

    def reserve(self, entry):
        current = self.schedule.get(entry.name)
        if current is not None and current is not entry:
            return current
        new_entry = self.schedule[entry.name] = next(entry)
        self._schedule_snapshot = None
        return new_entry

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        # Update time-stamps and run counts before we actually execute,
        # so we have that done if an exception is raised (doesn't schedule
        # forever.)
        entry = self.reserve(entry) if advance else entry

        try:
            return self._send_entry(entry, producer=producer)
        except Exception as exc:  # pylint: disable=broad-except
            reraise(SchedulingError, SchedulingError(
                "Couldn't apply scheduled task {0.name}: {exc}".format(
                    entry, exc=exc)), sys.exc_info()[2])
        finally:
            self._tasks_since_sync += 1
            if self.should_sync():
                self._do_sync()

    def send_task(self, *args, **kwargs):
        return self.app.send_task(*args, **kwargs)

    def setup_schedule(self):
        self.install_default_entries(self.data)
        self.merge_inplace(self.app.conf.beat_schedule)

    def _do_sync(self):
        debug('beat: Synchronizing schedule...')
        self.sync()
        self._last_sync = time.monotonic()
        self._tasks_since_sync = 0

    def sync(self):
        pass

    def close(self):
        self.sync()

    def add(self, **kwargs):
        entry = self.Entry(app=self.app, **kwargs)
        self._ensure_entry_identity(entry)
        if entry.name in self.schedule:
            self._bump_entry_generation(entry)
        self.schedule[entry.name] = entry
        self._schedule_snapshot = None
        return entry

    def _maybe_entry(self, name, entry):
        if isinstance(entry, self.Entry):
            entry.app = self.app
            self._ensure_entry_identity(entry)
            return entry
        return self.Entry(**dict(entry, name=name, app=self.app))

    def update_from_dict(self, dict_):
        self.schedule.update({
            name: self._maybe_entry(name, entry)
            for name, entry in dict_.items()
        })
        self._schedule_snapshot = None

    def merge_inplace(self, b):
        schedule = self.schedule
        A, B = set(schedule), set(b)

        # Remove items from disk not in the schedule anymore.
        for key in A ^ B:
            schedule.pop(key, None)

        # Update and add new items in the schedule
        for key in B:
            raw_entry = b[key]
            if isinstance(b[key], self.Entry):
                entry = raw_entry
                entry.app = self.app
                self._ensure_entry_identity(entry)
            else:
                entry = self.Entry(**dict(b[key], name=key, app=self.app))
            if schedule.get(key):
                current = schedule[key]
                self._ensure_entry_identity(current)
                changed = not current.editable_fields_equal(entry)
                if isinstance(raw_entry, self.Entry):
                    if current.last_run_at != raw_entry.last_run_at:
                        current.last_run_at = raw_entry.last_run_at
                        current.version += 1
                        changed = True
                    if raw_entry.total_run_count:
                        current.total_run_count = raw_entry.total_run_count
                        changed = True
                        current.version = max(
                            getattr(current, 'version', 0),
                            raw_entry.total_run_count,
                        )
                else:
                    if raw_entry.get('last_run_at') is not None:
                        new_last_run_at = raw_entry['last_run_at']
                        if current.last_run_at != new_last_run_at:
                            current.last_run_at = new_last_run_at
                            current.version += 1
                            changed = True
                    if raw_entry.get('total_run_count'):
                        current.total_run_count = raw_entry['total_run_count']
                        changed = True
                        current.version = max(
                            getattr(current, 'version', 0),
                            raw_entry['total_run_count'],
                        )
                current.task = entry.task
                current.schedule = entry.schedule
                current.args = entry.args
                current.kwargs = entry.kwargs
                current.options = entry.options
                if changed:
                    self._bump_entry_generation(current)
            else:
                schedule[key] = entry
        self._schedule_snapshot = None

    def _ensure_connected(self):
        # callback called for each retry while the connection
        # can't be established.
        def _error_handler(exc, interval):
            error('beat: Connection error: %s. '
                  'Trying again in %s seconds...', exc, interval)

        return self.connection.ensure_connection(
            _error_handler, self.app.conf.broker_connection_max_retries
        )

    def get_schedule(self):
        return self.data

    def set_schedule(self, schedule):
        self.data = schedule
    schedule = property(get_schedule, set_schedule)

    @cached_property
    def connection(self):
        return self.app.connection_for_write()

    @cached_property
    def producer(self):
        return self.Producer(self._ensure_connected(), auto_declare=False)

    @property
    def info(self):
        return ''


class PersistentScheduler(Scheduler):
    """Scheduler backed by :mod:`shelve` database."""

    persistence = shelve
    known_suffixes = ('', '.db', '.dat', '.bak', '.dir')

    _store = None

    def __init__(self, *args, **kwargs):
        self.schedule_filename = kwargs.get('schedule_filename')
        super().__init__(*args, **kwargs)

    def _remove_db(self):
        for suffix in self.known_suffixes:
            with platforms.ignore_errno(errno.ENOENT):
                os.remove(self.schedule_filename + suffix)

    def _open_schedule(self):
        return self.persistence.open(self.schedule_filename, writeback=True)

    def _destroy_open_corrupted_schedule(self, exc):
        error('Removing corrupted schedule file %r: %r',
              self.schedule_filename, exc, exc_info=True)
        self._remove_db()
        return self._open_schedule()

    def setup_schedule(self):
        try:
            self._store = self._open_schedule()
            # In some cases there may be different errors from a storage
            # backend for corrupted files.  Example - DBPageNotFoundError
            # exception from bsddb.  In such case the file will be
            # successfully opened but the error will be raised on first key
            # retrieving.
            self._store.keys()
        except Exception as exc:  # pylint: disable=broad-except
            self._store = self._destroy_open_corrupted_schedule(exc)

        self._create_schedule()
        self._dispatches = self._store.get('pending_dispatches', {}) or {}

        tz = self.app.conf.timezone
        stored_tz = self._store.get('tz')
        tz_changed = stored_tz is not None and stored_tz != tz
        if stored_tz is not None and stored_tz != tz:
            warning('Timezone changed from %r to %r', stored_tz, tz)
        utc = self.app.conf.enable_utc
        stored_utc = self._store.get('utc_enabled')
        utc_changed = stored_utc is not None and stored_utc != utc
        if stored_utc is not None and stored_utc != utc:
            choices = {True: 'enabled', False: 'disabled'}
            warning('UTC changed from %s to %s',
                    choices[stored_utc], choices[utc])
        entries = self._store.setdefault('entries', {})
        self.merge_inplace(self.app.conf.beat_schedule)
        self.install_default_entries(self.schedule)
        if tz_changed or utc_changed:
            for entry in self.schedule.values():
                self._bump_entry_generation(entry)
        for dispatch in self._dispatches.values():
            self._ensure_entry_identity(dispatch.entry)
        for key, dispatch in tuple(self._dispatches.items()):
            current = self.schedule.get(key)
            if current is None:
                continue
            if self._entry_version(current) <= dispatch.entry.version:
                current.last_run_at = next(dispatch.entry).last_run_at
                current.total_run_count = dispatch.entry.total_run_count + 1
                current.version = dispatch.entry.version + 1
        self._store.update({
            '__version__': __version__,
            'tz': tz,
            'utc_enabled': utc,
        })
        self._store['pending_dispatches'] = self._dispatches
        self.sync()
        debug('Current schedule:\n' + '\n'.join(
            repr(entry) for entry in entries.values()))

    def _create_schedule(self):
        for _ in (1, 2):
            try:
                self._store['entries']
            except (KeyError, UnicodeDecodeError, TypeError, UnpicklingError):
                # new schedule db
                try:
                    self._store['entries'] = {}
                except (KeyError, UnicodeDecodeError, TypeError, UnpicklingError) + dbm.error as exc:
                    self._store = self._destroy_open_corrupted_schedule(exc)
                    continue
            else:
                if '__version__' not in self._store:
                    warning('DB Reset: Account for new __version__ field')
                    self._store.clear()   # remove schedule at 2.2.2 upgrade.
                elif 'tz' not in self._store:
                    warning('DB Reset: Account for new tz field')
                    self._store.clear()   # remove schedule at 3.0.8 upgrade
                elif 'utc_enabled' not in self._store:
                    warning('DB Reset: Account for new utc_enabled field')
                    self._store.clear()   # remove schedule at 3.0.9 upgrade
            break

    def get_schedule(self):
        return self._store['entries']

    def set_schedule(self, schedule):
        self._store['entries'] = schedule
    schedule = property(get_schedule, set_schedule)

    def _persist_dispatches(self):
        self._store['pending_dispatches'] = self._dispatches
        self.sync()

    def _persist_dispatch_state(self):
        self._store['entries'] = self.schedule
        self._store['pending_dispatches'] = self._dispatches
        self.sync()

    def sync(self):
        if self._store is not None:
            self._store['pending_dispatches'] = self._dispatches
            self._store.sync()

    def close(self):
        self.sync()
        self._store.close()

    @property
    def info(self):
        return f'    . db -> {self.schedule_filename}'


class Service:
    """Celery periodic task service."""

    scheduler_cls = PersistentScheduler

    def __init__(self, app, max_interval=None, schedule_filename=None,
                 scheduler_cls=None):
        self.app = app
        self.max_interval = (max_interval or
                             app.conf.beat_max_loop_interval)
        self.scheduler_cls = scheduler_cls or self.scheduler_cls
        self.schedule_filename = (
            schedule_filename or app.conf.beat_schedule_filename)

        self._is_shutdown = Event()
        self._is_stopped = Event()

    def __reduce__(self):
        return self.__class__, (self.app, self.max_interval,
                                self.schedule_filename, self.scheduler_cls)

    def start(self, embedded_process=False):
        info('beat: Starting...')
        debug('beat: Ticking with max interval->%s',
              humanize_seconds(self.scheduler.max_interval))

        signals.beat_init.send(sender=self)
        if embedded_process:
            signals.beat_embedded_init.send(sender=self)
            platforms.set_process_title('celery beat')

        try:
            while not self._is_shutdown.is_set():
                interval = self.scheduler.tick()
                if interval and interval > 0.0:
                    debug('beat: Waking up %s.',
                          humanize_seconds(interval, prefix='in '))
                    time.sleep(interval)
                    if self.scheduler.should_sync():
                        self.scheduler._do_sync()
        except (KeyboardInterrupt, SystemExit):
            self._is_shutdown.set()
        finally:
            self.sync()

    def sync(self):
        self.scheduler.close()
        self._is_stopped.set()

    def stop(self, wait=False):
        info('beat: Shutting down...')
        self._is_shutdown.set()
        wait and self._is_stopped.wait()  # block until shutdown done.

    def get_scheduler(self, lazy=False,
                      extension_namespace='celery.beat_schedulers'):
        filename = self.schedule_filename
        aliases = dict(load_extension_class_names(extension_namespace))
        return symbol_by_name(self.scheduler_cls, aliases=aliases)(
            app=self.app,
            schedule_filename=filename,
            max_interval=self.max_interval,
            lazy=lazy,
        )

    @cached_property
    def scheduler(self):
        return self.get_scheduler()


class _Threaded(Thread):
    """Embedded task scheduler using threading."""

    def __init__(self, app, **kwargs):
        super().__init__()
        self.app = app
        self.service = Service(app, **kwargs)
        self.daemon = True
        self.name = 'Beat'

    def run(self):
        self.app.set_current()
        self.service.start()

    def stop(self):
        self.service.stop(wait=True)


try:
    ensure_multiprocessing()
except NotImplementedError:     # pragma: no cover
    _Process = None
else:
    class _Process(Process):

        def __init__(self, app, **kwargs):
            super().__init__()
            self.app = app
            self.service = Service(app, **kwargs)
            self.name = 'Beat'

        def run(self):
            reset_signals(full=False)
            platforms.close_open_fds([
                sys.__stdin__, sys.__stdout__, sys.__stderr__,
            ] + list(iter_open_logger_fds()))
            self.app.set_default()
            self.app.set_current()
            self.service.start(embedded_process=True)

        def stop(self):
            self.service.stop()
            self.terminate()


def EmbeddedService(app, max_interval=None, **kwargs):
    """Return embedded clock service.

    Arguments:
        thread (bool): Run threaded instead of as a separate process.
            Uses :mod:`multiprocessing` by default, if available.
    """
    if kwargs.pop('thread', False) or _Process is None:
        # Need short max interval to be able to stop thread
        # in reasonable time.
        return _Threaded(app, max_interval=1, **kwargs)
    return _Process(app, max_interval=max_interval, **kwargs)
