from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from celery.worker import state as worker_state


class FakeClock:

    def __init__(self, value=0.0):
        self.value = value

    def __call__(self):
        return self.value


class FakeEntry:

    def __init__(self, timer, when, callback):
        self.timer = timer
        self.when = when
        self.callback = callback
        self.cancelled = False

    def cancel(self):
        self.cancelled = True
        self.timer.entries.discard(self)


class FakeTimer:

    def __init__(self):
        self.entries = set()

    def call_after(self, delay, callback, args=(), priority=None):
        def run():
            self.entries.discard(entry)
            callback(*args)

        entry = FakeEntry(self, delay, run)
        self.entries.add(entry)
        return entry


class FakeQueueConsumer:

    def __init__(self):
        self.cancelled = []
        self.added = []
        self.consumed = 0

    def cancel_by_queue(self, name):
        self.cancelled.append(name)

    def add_queue(self, queue):
        self.added.append(queue)

    def consume(self):
        self.consumed += 1


def make_app():
    return SimpleNamespace(
        conf=SimpleNamespace(worker_rate_limit_scopes={}),
        amqp=SimpleNamespace(queues=SimpleNamespace(
            consume_from={'queue-a': object(), 'queue-b': object()},
        )),
    )


class FakeRequest:

    def __init__(self, name='task.a', request_id='1', queue='queue-a',
                 retries=0, rate_limit='1/s', scope=None):
        self.id = request_id
        self.type = name
        self.task = SimpleNamespace(
            rate_limit=rate_limit, rate_limit_scope=scope,
        )
        self.request_dict = {'retries': retries}
        self.delivery_info = {'routing_key': queue}
        self.reject = Mock()


def make_request(name='task.a', request_id='1', queue='queue-a', retries=0,
                 rate_limit='1/s'):
    request = FakeRequest(
        name=name, request_id=request_id, queue=queue, retries=retries,
        rate_limit=rate_limit,
    )
    return request


def make_limiter(app=None, clock=None, **kwargs):
    from celery.worker.rate_limit import FairRateLimiter

    dispatched = []
    limiter = FairRateLimiter(
        app if app is not None else make_app(),
        FakeTimer(),
        dispatched.append,
        clock=clock or FakeClock(),
        **kwargs,
    )
    return limiter, dispatched, limiter.clock


@pytest.fixture(autouse=True)
def reset_worker_state():
    yield
    worker_state.reset_state()


def test_burst_uses_fixed_capacity_and_monotonic_times():
    from celery.worker.rate_limit import FairTokenBucket

    clock = FakeClock()
    bucket = FairTokenBucket(1.0, capacity=2, now=clock)

    assert [bucket.reserve() for _ in range(4)] == [0.0, 0.0, 1.0, 1.0]


def test_continuous_updates_produce_stable_allowed_series():
    from celery.worker.rate_limit import FairTokenBucket

    clock = FakeClock()
    bucket = FairTokenBucket(1.0, now=clock)
    allowed = []

    allowed.append(bucket.reserve())
    clock.value = 0.9
    allowed.append(bucket.reserve())
    clock.value = 1.0
    allowed.append(bucket.reserve())
    clock.value = 2.0
    allowed.append(bucket.reserve())

    assert allowed == [0.0, 1.0, 2.0, 3.0]


def test_reset_and_reconnect_preserve_issued_debt():
    limiter, dispatched, clock = make_limiter(
        pause_queues=False, max_pending=2,
    )
    first = make_request(request_id='1')
    second = make_request(request_id='2')
    limiter.limit(first, first.task)
    limiter.limit(second, second.task)
    first_bucket = limiter.buckets[first.type]

    limiter.reset_rate_limits({'task.a': second.task})
    assert limiter.buckets[first.type] is first_bucket

    assert [item.id for item in dispatched] == ['1']
    pending_entry = next(iter(limiter.timer.entries))
    assert pending_entry.when == 1.0

    limiter.clear_pending()
    reconnected = make_request(request_id='3')
    limiter.limit(reconnected, reconnected.task)
    assert [item.id for item in dispatched] == ['1']
    assert next(iter(limiter.timer.entries)).when == 2.0


def test_eta_retry_and_attempt_use_independent_reservations():
    clock = FakeClock()
    limiter, dispatched, _ = make_limiter(clock=clock, pause_queues=False)
    first = make_request(request_id='same-id')

    limiter.limit(first, first.task, ready_at=10.0)
    entry = next(iter(limiter.timer.entries))
    assert entry.when == 10.0
    clock.value = 10.0
    entry.callback()
    assert [item.id for item in dispatched] == ['same-id']

    retry = make_request(request_id='same-id', retries=1)
    limiter.limit(retry, retry.task, ready_at=10.1)
    retry_entry = next(iter(limiter.timer.entries))
    assert retry_entry.when == 1.0
    assert limiter.buckets[retry.type].reservations[-1][0] == 11.0


def test_dynamic_reset_updates_explicit_task_scope():
    first_task = SimpleNamespace(
        rate_limit='1/s', rate_limit_scope='tenant',
    )
    second_task = SimpleNamespace(
        rate_limit='2/s', rate_limit_scope='tenant',
    )
    limiter, dispatched, _ = make_limiter(pause_queues=False)
    first = make_request(request_id='1')
    first.task = first_task
    second = make_request(name='task.b', request_id='2')
    second.task = second_task
    second.request_dict['rate_limit_scope'] = 'tenant'

    limiter.limit(first, first_task)
    assert limiter.buckets[('task.a', 'tenant')].interval == 1.0

    limiter.reset_rate_limits({
        'task.a': first_task,
        'task.b': second_task,
    })
    limiter.limit(second, second_task)
    assert limiter.buckets[('task.b', 'tenant')].interval == 0.5
    assert [request.id for request in dispatched] == ['1', '2']


def test_explicit_scopes_share_and_isolate_buckets():
    app = make_app()
    app.conf.worker_rate_limit_scopes = {'tenant': '1/s'}
    limiter, _, _ = make_limiter(app=app, pause_queues=False)

    first = FakeRequest(name='task.a', request_id='a', rate_limit=None)
    second = FakeRequest(name='task.b', request_id='b', rate_limit=None)
    first.request_dict['rate_limit_scope'] = 'tenant'
    second.request_dict['rate_limit_scope'] = 'tenant'
    first.task.rate_limit = None
    second.task.rate_limit = None

    limiter.limit(first, first.task)
    limiter.limit(second, second.task)

    assert (limiter.bucket_for(first, first.task)
            is limiter.bucket_for(second, second.task))
    assert next(iter(limiter.timer.entries)).when == 1.0


def test_local_pending_is_bounded():
    limiter, dispatched, _ = make_limiter(
        pause_queues=False, max_pending=1,
    )
    first = make_request(request_id='1')
    second = make_request(request_id='2')
    third = make_request(request_id='3')

    limiter.limit(first, first.task)
    limiter.limit(second, second.task)
    limiter.limit(third, third.task)

    assert [item.id for item in dispatched] == ['1']
    assert third.reject.call_args.kwargs == {'requeue': True}
    assert len(limiter._entries[limiter.buckets['task.a']]) == 1


def test_requeue_pauses_only_source_queue_until_token_is_due():
    limiter, dispatched, _ = make_limiter()
    consumer = SimpleNamespace(
        task_consumer=FakeQueueConsumer(), qos=Mock(),
    )
    limiter.bind_consumer(consumer)

    first = make_request(request_id='1', queue='queue-a')
    second = make_request(request_id='2', queue='queue-a')
    other = make_request(name='task.b', request_id='3', queue='queue-b')

    limiter.limit(first, first.task)
    limiter.limit(second, second.task)
    limiter.limit(other, other.task)

    assert consumer.task_consumer.cancelled == ['queue-a']
    assert second.reject.call_args.kwargs == {'requeue': True}
    assert [item.id for item in dispatched] == ['1', '3']

    entry = next(iter(limiter.timer.entries))
    assert entry.when == 1.0
    entry.callback()
    assert consumer.task_consumer.added == [
        limiter.app.amqp.queues.consume_from['queue-a'],
    ]
    assert consumer.task_consumer.consumed == 1


def test_cancel_releases_local_grant():
    limiter, dispatched, _ = make_limiter(
        pause_queues=False, max_pending=2,
    )
    pending = make_request(request_id='future')
    replacement = make_request(request_id='now')

    immediate = make_request(request_id='immediate')
    limiter.limit(immediate, immediate.task)
    limiter.limit(pending, pending.task)
    entry = next(iter(limiter.timer.entries))
    assert limiter.cancel(pending) is True
    assert entry.cancelled

    limiter.limit(replacement, replacement.task)
    limiter.clock.value = 1.0
    next(iter(limiter.timer.entries)).callback()
    assert [item.id for item in dispatched] == ['immediate', 'now']


def test_cancel_removes_revoked_pending_request_state():
    limiter, dispatched, _ = make_limiter(
        pause_queues=False, max_pending=2,
    )
    immediate = make_request(request_id='immediate')
    pending = make_request(request_id='future')
    limiter.limit(immediate, immediate.task)
    limiter.limit(pending, pending.task)
    worker_state.requests[pending.id] = pending
    worker_state.scheduled_requests.add(pending)

    assert limiter.cancel(pending) is True

    assert pending.id not in worker_state.requests
    assert pending not in worker_state.scheduled_requests
    worker_state.reset_state()


def test_shutdown_cancels_timers_but_keeps_bucket_debt():
    limiter, dispatched, _ = make_limiter(
        pause_queues=False, max_pending=2,
    )
    request = make_request(request_id='future')
    immediate = make_request(request_id='immediate')
    limiter.limit(immediate, immediate.task)
    limiter.limit(request, request.task)
    bucket = limiter.buckets[request.type]
    entry = next(iter(limiter.timer.entries))

    limiter.clear_pending()

    assert entry.cancelled
    assert not limiter._entries[bucket]
    assert limiter.buckets[request.type] is bucket
