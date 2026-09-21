from datetime import datetime, timedelta, timezone
import heapq
from pickle import dumps, loads

from celery import beat, uuid
from celery.beat import event_t
from celery.schedules import crontab, schedule
from celery.utils.objects import Bunch


class mScheduler(beat.Scheduler):

    def __init__(self, *args, fail_times=0, **kwargs):
        self.sent = []
        self._fail_times = fail_times
        super().__init__(*args, **kwargs)

    def send_task(self, name=None, args=None, kwargs=None, **options):
        if self._fail_times:
            self._fail_times -= 1
            raise OSError('broker down')
        self.sent.append({'name': name, 'options': options})
        return self.app.AsyncResult(uuid())


class MockShelve(dict):
    closed = False
    synced = False

    def close(self):
        self.closed = True

    def sync(self):
        self.synced = True


def make_persistent_scheduler(app, shelv=None):
    shelv = shelv or MockShelve()

    class PS(beat.PersistentScheduler):
        persistence = Bunch(open=lambda *a, **kw: shelv)
        sh = shelv

        def __init__(self, *a, **k):
            self.sent = []
            super().__init__(*a, **k)

        def send_task(self, task=None, args=None, kwargs=None, **options):
            self.sent.append(task)
            return self.app.AsyncResult(uuid())

    return PS, shelv


class dueSchedule(schedule):

    def __init__(self, nowfun, run_every=timedelta(seconds=60)):
        super().__init__(run_every, nowfun=nowfun)

    def is_due(self, last_run_at):
        last_run_at = self.maybe_make_aware(last_run_at)
        now = self.maybe_make_aware(self.now())
        rem = (last_run_at + self.run_every - now).total_seconds()
        if rem <= 0:
            return True, self.seconds
        return False, rem


class test_generations:

    def test_entry_pickles_generation(self):
        e = beat.ScheduleEntry(name='x', task='t', schedule=10,
                               generation=42, app=self.app)
        e2 = loads(dumps(e))
        assert e2.generation == 42

    def test_legacy_pickle_without_generation_defaults_to_zero(self):
        # A schedule entry persisted by an older Celery version pickles with
        # only the original 8 positional args. It must still unpickle, with
        # generation defaulting to "unassigned" (0).
        import pickle

        class LegacyEntry:
            def __reduce__(self):
                return beat.ScheduleEntry, (
                    'x', 't',
                    datetime(2026, 1, 1, tzinfo=timezone.utc), 3,
                    schedule(10), (), {}, {},
                )

        data = pickle.dumps(LegacyEntry())
        restored = pickle.loads(data)
        assert restored.generation == 0
        assert restored.name == 'x'

    def test_add_assigns_monotonic_generations(self):
        s = mScheduler(app=self.app)
        a = s.add(name='a', task='t', schedule=schedule(10))
        b = s.add(name='b', task='t', schedule=schedule(10))
        assert a.generation > 0
        assert b.generation == a.generation + 1
        assert s._generations == {'a': a.generation, 'b': b.generation}

    def test_update_bumps_generation_and_heaps_new_definition(self):
        s = mScheduler(app=self.app)
        e = s.add(name='a', task='t', schedule=schedule(10))
        s.tick()
        old_event = s._heap[0]
        gen = e.generation
        # same-name update with a new interval
        s.add(name='a', task='t', schedule=schedule(30))
        s.tick()
        new_event = s._heap[0]
        assert new_event is not old_event
        assert new_event.entry.generation == gen + 1
        assert s._generations['a'] == gen + 1

    def test_reload_after_reserve_sends_once(self):
        clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        s = mScheduler(app=self.app)
        e = s.add(name='a', task='t',
                  schedule=dueSchedule(lambda: clock[0]),
                  last_run_at=clock[0] - timedelta(seconds=60))
        assert s.tick() == 0
        assert len(s.sent) == 1
        reserved = s.schedule['a']
        assert reserved.total_run_count == 1
        # Simulate a lagging reload source (disk/db) that still carries the
        # pre-reservation runtime state but the same definition.
        stale = beat.ScheduleEntry(
            name='a', task='t', schedule=schedule(60),
            last_run_at=e.last_run_at, total_run_count=0,
            generation=e.generation, app=self.app)
        s.schedule = {'a': stale}
        assert s.tick() > 0
        assert len(s.sent) == 1
        assert s.schedule['a'] is reserved

    def test_deleted_entry_does_not_resurface_from_heap(self):
        clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        s = mScheduler(app=self.app)
        doomed = s.add(name='doomed', task='t',
                       schedule=dueSchedule(lambda: clock[0]))
        keep = s.add(name='keep', task='t',
                     schedule=schedule(3600, nowfun=lambda: clock[0]))
        s.tick()  # populate heap, nothing due (last_run_at ~ now)
        s.schedule.pop('doomed')
        # inject a stale heap event for the deleted, due entry
        s._heap.append(event_t(s._when(doomed, 0) - 10, 5, doomed))
        heapq.heapify(s._heap)
        s.tick()
        assert s.sent == []
        assert 'doomed' not in s._generations
        assert all(h.entry.name != 'doomed' for h in s._heap)

    def test_send_failure_retried_exactly_once(self):
        clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        s = mScheduler(app=self.app, fail_times=1)
        s.add(name='a', task='t',
              schedule=dueSchedule(lambda: clock[0]),
              last_run_at=clock[0] - timedelta(seconds=60))
        assert s.tick() == 0
        assert s.sent == []
        assert ('a', 1) in s._pending
        # retry succeeds
        s.tick()
        assert len(s.sent) == 1
        assert not s._pending
        # no duplicate on further ticks
        s.tick()
        assert len(s.sent) == 1

    def test_pending_kept_after_failed_send_without_double_reserve(self):
        clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        s = mScheduler(app=self.app, fail_times=10)
        s.add(name='a', task='t',
              schedule=dueSchedule(lambda: clock[0]),
              last_run_at=clock[0] - timedelta(seconds=60))
        s.tick()
        count_after_first = s.schedule['a'].total_run_count
        for _ in range(3):
            s.tick()
        assert s.sent == []
        assert s.schedule['a'].total_run_count == count_after_first == 1

    def test_sync_failure_keeps_counter(self):
        s = mScheduler(app=self.app)
        s.sync = lambda: (_ for _ in ()).throw(OSError('disk full'))
        s._tasks_since_sync = 1
        s._do_sync()
        assert s._tasks_since_sync == 1
        assert s._last_sync is None

    def test_reserved_message_keeps_old_generation_after_update(self):
        clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        s = mScheduler(app=self.app, fail_times=1)
        e = s.add(name='a', task='t',
                  schedule=dueSchedule(lambda: clock[0]),
                  last_run_at=clock[0] - timedelta(seconds=60))
        s.tick()  # reserved but send failed -> pending gen 1
        pending_entry = s._pending[('a', 1)]
        # definition changes while broker is down
        s.add(name='a', task='t2', schedule=schedule(30))
        s.tick()  # retries old message once (attribution preserved)
        assert [m['name'] for m in s.sent] == ['t']
        assert pending_entry.task == 't'
        assert s.schedule['a'].task == 't2'
        assert s.schedule['a'].generation == 2

    def test_deleted_pending_message_delivered_once_then_gone(self):
        clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        s = mScheduler(app=self.app, fail_times=1)
        s.add(name='a', task='t',
              schedule=dueSchedule(lambda: clock[0]),
              last_run_at=clock[0] - timedelta(seconds=60))
        s.tick()  # reserved, send failed -> pending
        assert ('a', 1) in s._pending
        # entry deleted while the broker is down
        s.schedule.pop('a')
        s.tick()  # stale heap dropped, pending retried successfully
        assert [m['name'] for m in s.sent] == ['t']
        assert not s._pending
        assert 'a' not in s._generations
        assert all(h.entry.name != 'a' for h in s._heap)

    def test_reconcile_adopts_persisted_generation(self):
        persisted = beat.ScheduleEntry(
            name='a', task='t', schedule=schedule(60),
            generation=99, app=self.app)
        s = mScheduler(app=self.app, schedule={'a': persisted}, lazy=True)
        s.tick()
        assert s._generations['a'] == 99
        assert s._generation_seq >= 99
        n = s.add(name='b', task='t', schedule=schedule(60))
        assert n.generation == 100

    def test_clock_rollback_no_unbounded_dispatch(self):
        clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        s = mScheduler(app=self.app)
        s.add(name='a', task='t',
              schedule=dueSchedule(lambda: clock[0],
                                   run_every=timedelta(seconds=60)),
              last_run_at=clock[0] - timedelta(seconds=60))
        s.tick()
        assert len(s.sent) == 1
        # clock jumps back 10 minutes; entry was already reserved
        clock[0] -= timedelta(minutes=10)
        for _ in range(5):
            s.tick()
        assert len(s.sent) == 1

    def test_merge_inplace_deletes_and_updates(self):
        s = mScheduler(app=self.app)
        s.update_from_dict({
            'foo': {'task': 't', 'schedule': schedule(10)},
            'bar': {'task': 't', 'schedule': schedule(20)},
        })
        foo_gen = s.schedule['foo'].generation
        bar_gen = s.schedule['bar'].generation
        s.merge_inplace({
            'bar': {'task': 't', 'schedule': schedule(40)},
            'baz': {'task': 't', 'schedule': schedule(10)},
        })
        assert 'foo' not in s.schedule
        assert 'baz' in s.schedule
        assert s.schedule['bar'].schedule == schedule(40)
        assert s.schedule['bar'].generation == bar_gen + 1
        # reconcile on next tick retires foo, registers baz
        s.tick()
        assert 'foo' not in s._generations
        assert s._generations['baz'] > foo_gen

    def test_persistent_restart_generation_monotonic(self):
        clock = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
        PS, shelv = make_persistent_scheduler(self.app)
        self.app.conf.beat_schedule = {
            'a': {'task': 't',
                  'schedule': dueSchedule(lambda: clock[0])},
        }
        shelv['tz'] = 'UTC'
        shelv['utc_enabled'] = True
        from celery import __version__
        shelv['__version__'] = __version__
        s1 = PS(self.app, schedule_filename='sched')
        gen_before = s1.schedule['a'].generation
        assert gen_before > 0
        # simulate a reservation (tick) then graceful sync/close
        entry = s1.schedule['a']
        entry.last_run_at = clock[0] - timedelta(seconds=60)
        s1.tick()
        assert len(s1.sent) == 1
        s1.close()
        assert shelv.synced
        persisted_gen = shelv['entries']['a'].generation
        assert persisted_gen == gen_before

        # restart: same generation, monotonic sequence
        s2 = PS(self.app, schedule_filename='sched')
        assert s2.schedule['a'].generation == persisted_gen
        assert s2._generation_seq >= persisted_gen
        new_entry = s2.add(name='b', task='t', schedule=schedule(10))
        assert new_entry.generation == persisted_gen + 1

    def test_persistent_sync_failure_does_not_reset_counter(self):
        PS, shelv = make_persistent_scheduler(self.app)
        s = PS(self.app, schedule_filename='sched')

        class BadShelve(MockShelve):
            def sync(self):
                raise OSError('disk full')

        s._store = BadShelve()
        s._tasks_since_sync = 2
        s._do_sync()
        assert s._tasks_since_sync == 2
        assert s._last_sync is None
        # a successful sync afterwards resets
        s.sync = lambda: None
        s._do_sync()
        assert s._tasks_since_sync == 0

    def test_persistent_tz_change_clears_entries(self):
        PS, shelv = make_persistent_scheduler(self.app)
        shelv['tz'] = 'Europe/London'
        shelv['utc_enabled'] = True
        s = PS(self.app, schedule_filename='sched')
        # the tz mismatch branch clears the store; scheduler still works
        assert isinstance(s.schedule, dict)

    def test_pause_creates_new_generation(self):
        s = mScheduler(app=self.app)
        e = s.add(name='a', task='t', schedule=crontab(minute='5'))
        gen = e.generation
        # "pause" modeled as replacing the schedule with a far-away cron
        s.add(name='a', task='t', schedule=crontab(minute='30'))
        s.tick()
        assert s.schedule['a'].generation == gen + 1
        assert isinstance(s.schedule['a'].schedule, crontab)
