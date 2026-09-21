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

event_t = namedtuple('event_t', ('time', 'priority', 'entry'))

logger = get_logger(__name__)
debug, info, error, warning = (logger.debug, logger.info,
                               logger.error, logger.warning)

DEFAULT_MAX_INTERVAL = 300  # 5 minutes


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
        generation (int): Monotonic version of this entry's definition,
            used to discard stale heap events after a schedule reload.
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

    #: Version of this entry's definition. ``0`` means "unassigned" until
    #: the scheduler hands out a strictly positive generation on creation or
    #: reload. Already reserved/dispatched messages keep their old generation.
    generation = 0

    def __init__(self, name=None, task=None, last_run_at=None,
                 total_run_count=None, schedule=None, args=(), kwargs=None,
                 options=None, relative=False, app=None, generation=0):
        self.app = app
        self.name = name
        self.task = task
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.options = options if options else {}
        self.schedule = maybe_schedule(schedule, relative, app=self.app)
        self.last_run_at = last_run_at or self.default_now()
        self.total_run_count = total_run_count or 0
        self.generation = generation

    def default_now(self):
        return self.schedule.now() if self.schedule else self.app.now()
    _default_now = default_now  # compat

    def _next_instance(self, last_run_at=None):
        """Return new instance, with date and count fields updated."""
        return self.__class__(**dict(
            self,
            last_run_at=last_run_at or self.default_now(),
            total_run_count=self.total_run_count + 1,
        ))
    __next__ = next = _next_instance  # for 2to3

    def __reduce__(self):
        return self.__class__, (
            self.name, self.task, self.last_run_at, self.total_run_count,
            self.schedule, self.args, self.kwargs, self.options,
            False, None, self.generation,
        )

    def update(self, other):
        """Update values from another entry.

        Will only update "editable" fields:
            ``task``, ``schedule``, ``args``, ``kwargs``, ``options``.

        Returns:
            bool: :const:`True` when at least one field actually changed.
            A real definition change bumps :attr:`generation`, so heap
            events reserved under the old definition become stale.
        """
        changed = not self.editable_fields_equal(other)
        self.__dict__.update({
            'task': other.task, 'schedule': other.schedule,
            'args': other.args, 'kwargs': other.kwargs,
            'options': other.options,
        })
        if changed:
            self.generation += 1
        return changed

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

    #: Heap priority (constant; mirrors the historical literal ``5``).
    entry_priority = 5

    #: Delay (seconds) before retrying a due entry whose broker send failed.
    send_retry_delay = 1.0

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
        # Per-name generations and live-entry identities make up the
        # generation ledger shared by heap, reserve and reload tracks.
        self._generations = {}
        self._live_entries = {}
        self._generation_seq = 0
        # Due entries already reserved (advanced) but not yet acknowledged by
        # the broker. Keyed by (name, generation) so an entry is reserved at
        # most once for any due moment / definition generation.
        self._pending = {}
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
            result = self.apply_async(entry, producer=producer, advance=False)
        except Exception as exc:  # pylint: disable=broad-except
            error('Message Error: %s\n%s',
                  exc, traceback.format_stack(), exc_info=True)
            # The entry stays reserved and is retried from _pending; without
            # this return value a broker outage would drop the due moment
            # forever instead of delivering it exactly once later.
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
        priority = self.entry_priority
        pending_keys = set(self._pending)
        self._heap = []
        for entry in self.schedule.values():
            # Entries already reserved (awaiting broker acknowledgement)
            # must not be heaped again, or the same due moment would be sent
            # twice while a failed send is being retried.
            if (entry.name, entry.generation) in pending_keys:
                continue
            is_due, next_call_delay = entry.is_due()
            self._heap.append(event_t(
                self._when(
                    entry,
                    0 if is_due else next_call_delay
                ) or 0,
                priority, entry
            ))
        heapify(self._heap)

    # pylint disable=redefined-outer-name
    def tick(self, event_t=event_t, min=min, heappop=heapq.heappop,
             heappush=heapq.heappush):
        """Run a tick - one iteration of the scheduler.

        Executes one due task per call.

        Returns:
            float: preferred delay in seconds for next call.
        """
        adjust = self.adjust
        max_interval = self.max_interval

        if (self._heap is None or self._generation_drift() or
                self._schedule_is_lagging() or
                not self.schedules_equal(self.old_schedulers, self.schedule)):
            self.old_schedulers = copy.copy(self.schedule)
            self._reconcile()
            self.populate_heap()

        H = self._heap

        # Persistence track: a reserved entry whose send failed stays
        # reserved and gets exactly one bounded retry attempt per tick until
        # the broker accepts it. It is never re-reserved, so a broken broker
        # cannot multiply the same due moment.
        if self._pending:
            self._flush_pending()

        if not H:
            return self.send_retry_delay if self._pending else max_interval

        while H:
            event = H[0]
            entry = event[2]

            # Old generation track: a reload (same-name update, pause,
            # deletion, tz/last_run_at change) retired this event. Drop it
            # instead of dispatching from a stale definition. A same-object
            # in-place update (merge_inplace) shows up as generation drift and
            # is healed by rebuilding the whole heap.
            if self._event_is_stale(entry):
                heappop(H)
                if self._generation_drift():
                    self._reconcile()
                    self.populate_heap()
                    H = self._heap
                continue

            now = self._when(entry, 0)
            if event[0] > now:
                delay = min(event[0] - now, max_interval)
                if self._pending:
                    # Broker still rejecting a reserved entry: back off
                    # instead of spinning the service loop without sleeping.
                    return min(delay, self.send_retry_delay)
                return delay

            is_due, next_time_to_run = self.is_due(entry)
            if is_due:
                verify = heappop(H)
                if verify is not event:
                    # Heap mutated under us; put the intruder back and let the
                    # next tick decide with a fresh clock.
                    heappush(H, verify)
                    return min(verify[0] - self._when(verify[2], 0),
                               max_interval)

                # New generation track: reserve *first* (advancing
                # last_run_at/count), then send. A crash in between may
                # duplicate this single due moment after restart (bounded
                # at-least-once); a send failure is retried in-process via
                # _pending, which prevents both unbounded repeats and loss.
                next_entry = self.reserve(entry)
                sent = self.apply_entry(entry, producer=self.producer)
                if not sent:
                    self._pending[(entry.name, entry.generation)] = entry
                    return 0
                heappush(H, event_t(
                    self._when(next_entry, next_time_to_run),
                    event[1], next_entry))
                return 0

            # Heap says this entry should be ready by now, but the entry
            # requests to retry later. Reheap it at that retry time, otherwise
            # it just sits on top and entries behind it never get their turn.
            # https://github.com/celery/celery/issues/7649
            reschedule_delay = (
                next_time_to_run if is_numeric_value(next_time_to_run)
                else max_interval)
            verify = heappop(H)
            if verify is not event:
                heappush(H, verify)
                return min(verify[0] - self._when(verify[2], 0),
                           max_interval)
            heappush(H, event_t(
                self._when(entry, reschedule_delay), event[1], entry))
            # If another entry is now at the top, run it immediately.
            if H and H[0][2] is not entry:
                return 0
            return min(adjust(reschedule_delay), max_interval)

        return self.send_retry_delay if self._pending else max_interval

    def _new_generation(self, entry):
        """Assign the next monotonic generation to ``entry``."""
        self._generation_seq += 1
        entry.generation = self._generation_seq
        self._generations[entry.name] = entry.generation
        self._live_entries[entry.name] = entry
        return entry.generation

    def _adopt_generation(self, entry):
        """Register an entry under its existing (persisted) generation."""
        gen = entry.generation or 0
        if gen:
            self._generations[entry.name] = gen
            self._generation_seq = max(self._generation_seq, gen)
        self._live_entries[entry.name] = entry
        return gen

    def _generation_drift(self):
        """True when live entries mutated their generation outside the ledger."""
        return any(
            entry.generation != self._generations.get(name)
            for name, entry in self.schedule.items()
            if isinstance(entry, self.Entry)
        )

    def _schedule_is_lagging(self):
        """True when the reload source moved runtime state backwards.

        Definitions may compare equal (same interval/cron) while the source
        snapshot still carries a pre-reservation ``last_run_at`` /
        ``total_run_count``. Syncing that snapshot would replay the same due
        moment after a restart, so it must be reconciled away just like a
        generation change.
        """
        for name, entry in self.schedule.items():
            live = self._live_entries.get(name)
            if (
                isinstance(entry, self.Entry)
                and live is not None
                and not self._runtime_not_behind(live, entry)
            ):
                return True
        return False

    def _event_is_stale(self, entry):
        """Heap event belongs to a retired generation or a deleted entry."""
        return (
            self._live_entries.get(entry.name) is not entry or
            entry.generation != self._generations.get(entry.name)
        )

    def _reconcile(self):
        """Split a reload into old / new / persistence generations.

        * Old generation: same-name entries with an unchanged definition keep
          their generation (and their reservation), so a reserved entry is
          dispatched at most once for the due moment it was reserved for.
        * New generation: any real definition change (interval/cron, pause,
          args/kwargs/options/task, explicit last_run_at move) or a newly
          added name gets a fresh, strictly larger generation; old heap
          events are then recognizably stale.
        * Deleted names retire their generation and are dropped from the
          ledger, so they can never resurface from the heap.

        Runtime state (``last_run_at``/``total_run_count``) never moves
        backwards within a generation; that rule is what makes sync counts
        and last_run_at explainable across reloads and restarts.
        """
        old_schedule = self._live_entries
        old_generations = self._generations
        new_schedule = {}

        for name, new_entry in self.schedule.items():
            if not isinstance(new_entry, self.Entry):
                new_schedule[name] = new_entry
                continue
            live_entry = old_schedule.get(name)
            old_generation = old_generations.get(name)

            if live_entry is None:
                # First time the ledger sees this entry (startup, restart
                # with persisted generations, or a brand-new name).
                if new_entry.generation:
                    self._adopt_generation(new_entry)
                else:
                    self._new_generation(new_entry)
                new_schedule[name] = new_entry
                continue

            if live_entry.editable_fields_equal(new_entry):
                # Same definition. Whether the reload source agrees with the
                # live reservation or lags behind it (a disk/db snapshot
                # taken before the in-memory reservation), keep the live
                # object, its generation and its reservation: rolling
                # last_run_at back would re-send the same due moment. This
                # is the old-generation track that makes "reserved then
                # reloaded" send exactly once.
                new_schedule[name] = live_entry
            elif self._runtime_not_behind(live_entry, new_entry):
                # Real definition change with fresh runtime state: take the
                # new object and a strictly larger generation.
                entry = new_entry
                entry.generation = old_generation + 1
                self._generation_seq = max(
                    self._generation_seq, entry.generation)
                self._generations[name] = entry.generation
                self._live_entries[name] = entry
                new_schedule[name] = entry
            else:
                # Real definition change, but the reload source lags behind
                # the live reservation. Adopt the new *definition* on the
                # live object (keeping monotonic runtime state). The
                # generation was already bumped when the definition changed
                # during merge_inplace(); make sure it is strictly greater
                # than the retired one even if that bump was skipped.
                changed = live_entry.update(new_entry)
                if not changed:
                    live_entry.generation = old_generation + 1
                self._generation_seq = max(
                    self._generation_seq, live_entry.generation)
                self._generations[name] = live_entry.generation
                new_schedule[name] = live_entry

        # Retire generations for deleted names and let any still-pending send
        # finish, but never allow them back into the heap.
        for name in old_schedule.keys() - new_schedule.keys():
            self._generations.pop(name, None)
            self._live_entries.pop(name, None)

        # Persistence track: the reconciled view is what gets synced. Compare
        # by identity, not ==, because ScheduleEntry.__eq__ ignores runtime
        # state and generations (a stale snapshot could otherwise look
        # "equal" to the reconciled reservation and never be corrected on
        # disk).
        if (
            set(new_schedule) != set(self.data)
            or any(
                new_schedule[name] is not self.data.get(name)
                for name in new_schedule)
        ):
            self.data = new_schedule

    @staticmethod
    def _runtime_not_behind(live_entry, new_entry):
        """Runtime state of a reloaded entry isn't older than the reservation."""
        if new_entry.total_run_count > live_entry.total_run_count:
            return True
        if new_entry.total_run_count < live_entry.total_run_count:
            return False
        live_last = maybe_make_aware(live_entry.last_run_at)
        new_last = maybe_make_aware(new_entry.last_run_at)
        return new_last >= live_last

    def _flush_pending(self):
        """Retry reserved-but-unacknowledged entries exactly once per tick."""
        for key in list(self._pending):
            name, generation = key
            entry = self._pending[key]
            if self.apply_entry(entry, producer=self.producer):
                self._pending.pop(key, None)
                next_entry = self.schedule.get(name)
                # Reheap only if the reservation is still current. An entry
                # deleted/redefined while the send was failing is done: the
                # message has been delivered once and that is the end of its
                # old generation.
                if (
                    next_entry is not None
                    and next_entry.generation == generation
                ):
                    _, next_time_to_run = self.is_due(next_entry)
                    heapq.heappush(
                        self._heap,
                        event_t(self._when(next_entry, next_time_to_run),
                                self.entry_priority, next_entry))

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
        """Advance an entry to its next due moment.

        The reserved occurrence keeps the same generation, so the heap event
        and the message in flight always carry the definition they were
        created for. Only the current live entry is written back: reserving a
        stale (superseded/deleted) occurrence can never overwrite the latest
        definition, which also prevents a reload racing this tick from
        resurrecting a deleted task.
        """
        new_entry = next(entry)
        live = self._live_entries.get(entry.name)
        if live is None or live is entry:
            self.schedule[entry.name] = new_entry
            self._live_entries[entry.name] = new_entry
            self._generations[entry.name] = new_entry.generation
        return new_entry

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        # Update time-stamps and run counts before we actually execute,
        # so we have that done if an exception is raised (doesn't schedule
        # forever.)
        entry = self.reserve(entry) if advance else entry
        task = self.app.tasks.get(entry.task)

        try:
            entry_args = _evaluate_entry_args(entry.args)
            entry_kwargs = _evaluate_entry_kwargs(entry.kwargs)

            # Add custom header to identify tasks from Celery Beat
            options = entry.options.copy()
            options.setdefault('headers', {})['celery_beat_task'] = True

            if task:
                return task.apply_async(entry_args, entry_kwargs,
                                        producer=producer,
                                        **options)
            else:
                return self.send_task(entry.task, entry_args, entry_kwargs,
                                      producer=producer,
                                      **options)
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
        try:
            debug('beat: Synchronizing schedule...')
            self.sync()
        except Exception as exc:  # pylint: disable=broad-except
            # A failed sync must not look like a successful one: keep both
            # the timestamp pending and the task counter so the very next
            # tick retries the persistence instead of letting reservations
            # accumulate unsynced (which is what loses a due moment when the
            # process is killed before the next successful sync).
            error('beat: Failed to synchronize schedule: %s', exc,
                  exc_info=True)
        else:
            self._last_sync = time.monotonic()
            self._tasks_since_sync = 0

    def sync(self):
        pass

    def close(self):
        self.sync()

    def add(self, **kwargs):
        entry = self.Entry(app=self.app, **kwargs)
        self._new_generation(entry)
        self.schedule[entry.name] = entry
        return entry

    def _maybe_entry(self, name, entry):
        if isinstance(entry, self.Entry):
            entry.app = self.app
            return entry
        return self.Entry(**dict(entry, name=name, app=self.app))

    def update_from_dict(self, dict_):
        self.schedule.update({
            name: self._maybe_entry(name, entry)
            for name, entry in dict_.items()
        })

    def merge_inplace(self, b):
        schedule = self.schedule
        A, B = set(schedule), set(b)

        # Remove items from disk not in the schedule anymore. The ledger
        # retirement happens in _reconcile(); removing the key here is what
        # makes a deleted entry unable to reappear from the old heap.
        for key in A - B:
            schedule.pop(key, None)

        # Update and add new items in the schedule
        for key in B:
            entry = self.Entry(**dict(b[key], name=key, app=self.app))
            current = schedule.get(key)
            if current is not None:
                # update() mutates in place and only bumps the generation
                # when a field truly changed. The subsequent tick then sees
                # the generation drift and rebuilds the heap, so the old
                # interval/cron event can never fire under the new schedule.
                current.update(entry)
            else:
                self._new_generation(entry)
                schedule[key] = entry

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

        tz = self.app.conf.timezone
        stored_tz = self._store.get('tz')
        if stored_tz is not None and stored_tz != tz:
            warning('Reset: Timezone changed from %r to %r', stored_tz, tz)
            self._store.clear()   # Timezone changed, reset db!
        utc = self.app.conf.enable_utc
        stored_utc = self._store.get('utc_enabled')
        if stored_utc is not None and stored_utc != utc:
            choices = {True: 'enabled', False: 'disabled'}
            warning('Reset: UTC changed from %s to %s',
                    choices[stored_utc], choices[utc])
            self._store.clear()   # UTC setting changed, reset db!
        entries = self._store.setdefault('entries', {})
        self.merge_inplace(self.app.conf.beat_schedule)
        self.install_default_entries(self.schedule)
        self._store.update({
            '__version__': __version__,
            'tz': tz,
            'utc_enabled': utc,
        })
        # Adopt persisted generations now, before the first tick: a restart
        # must continue the monotonic per-name generations (and never replay
        # a due moment whose reservation was synced to disk).
        self._reconcile()
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

    def sync(self):
        if self._store is not None:
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
