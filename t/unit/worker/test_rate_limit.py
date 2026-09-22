from types import SimpleNamespace
from unittest.mock import Mock, call

from kombu import Queue
from kombu.utils.limits import TokenBucket

from celery import Celery
from celery.worker.consumer.consumer import Consumer
from celery.worker.consumer.rate_limit import FairRateLimiter, FairTokenBucket


class FakeClock:
    def __init__(self, value=0.0):
        self.value = value

    def __call__(self):
        return self.value


class test_FairTokenBucket:
    def test_continuous_admission_stays_spaced_without_real_wait(self):
        clock = FakeClock()
        bucket = FairTokenBucket(1.0, clock=clock)

        assert bucket.reserve() is None
        assert bucket.reserve() == 1.0

        clock.value = 1.0
        assert bucket.reserve() is None

        # A late timer must not release a batch.
        clock.value = 5.0
        assert bucket.reserve() is None
        assert round(bucket.reserve(), 6) == 1.0

    def test_burst_only_spends_configured_idle_credit(self):
        clock = FakeClock()
        bucket = FairTokenBucket(1.0, capacity=3, clock=clock)

        assert bucket.reserve() is None
        assert bucket.reserve() is None
        assert bucket.reserve() is None
        assert round(bucket.reserve(), 6) == 1.0

        clock.value = 10.0
        assert bucket.reserve() is None
        assert bucket.reserve() is None
        assert bucket.reserve() is None
        assert round(bucket.reserve(), 6) == 1.0

    def test_rate_update_preserves_scheduled_grant(self):
        clock = FakeClock()
        bucket = FairTokenBucket(1.0, clock=clock)
        assert bucket.reserve() is None

        bucket.update_rate(2.0)
        assert round(bucket.expected_time(), 6) == 0.5

        clock.value = 0.5
        assert bucket.reserve() is None

        bucket.update_rate(0.5)
        assert round(bucket.expected_time(), 6) == 2.0


class test_FairRateLimiter:
    def setup_method(self):
        self.app = Celery(set_as_current=False)
        self.app.conf.worker_rate_limit_burst = 1

        @self.app.task(name='fast', rate_limit='1/s')
        def fast():
            pass

        @self.app.task(name='slow', rate_limit='1/s')
        def slow():
            pass

        @self.app.task(
            name='shared-a', rate_limit='1/s', rate_limit_scope='tenant')
        def shared_a():
            pass

        @self.app.task(
            name='shared-b', rate_limit='1/s', rate_limit_scope='tenant')
        def shared_b():
            pass

        self.fast = fast
        self.slow = slow
        self.shared_a = shared_a
        self.shared_b = shared_b

    def test_explicit_scope_is_shared_by_task_type(self):
        clock = FakeClock()
        limiter = FairRateLimiter(self.app, clock=clock)

        first = limiter.bucket_for(self.shared_a)
        second = limiter.bucket_for(self.shared_b)

        assert first is second
        assert first.reserve() is None
        assert second.reserve() == 1.0

    def test_request_header_scope_overrides_task_scope(self):
        clock = FakeClock()
        limiter = FairRateLimiter(self.app, clock=clock)
        request = SimpleNamespace(
            request_dict={'rate_limit_scope': 'other-tenant'})

        bucket = limiter.bucket_for(self.shared_a, request)

        assert limiter.scope_for(self.shared_a, request) == 'other-tenant'
        assert bucket is not limiter.bucket_for(self.shared_a)

    def test_reset_and_reconnect_keep_granted_quota(self):
        clock = FakeClock()
        limiter = FairRateLimiter(self.app, clock=clock)
        bucket = limiter.bucket_for(self.fast)
        assert bucket.reserve() is None
        scheduled_at = bucket.next_available

        limiter.reset()
        reconnected = limiter.bucket_for(self.fast)

        assert reconnected is bucket
        assert reconnected.next_available == scheduled_at
        assert reconnected.reserve() == 1.0

    def test_multiple_workers_have_independent_equivalent_buckets(self):
        clock = FakeClock()
        first_worker = FairRateLimiter(self.app, clock=clock)
        second_worker = FairRateLimiter(self.app, clock=clock)

        first_bucket = first_worker.bucket_for(self.fast)
        second_bucket = second_worker.bucket_for(self.fast)

        assert first_bucket.reserve() is None
        assert second_bucket.reserve() is None
        assert first_bucket.reserve() == 1.0
        assert second_bucket.reserve() == 1.0

        clock.value = 1.0
        assert first_bucket.reserve() is None
        assert second_bucket.reserve() is None

    def test_reset_only_resumes_scopes_whose_rate_changed(self):
        clock = FakeClock()
        limiter = FairRateLimiter(self.app, clock=clock)
        fast_bucket = limiter.bucket_for(self.fast)
        slow_bucket = limiter.bucket_for(self.slow)
        fast_bucket.reserve()
        slow_bucket.reserve()

        assert limiter.reset() == set()

        self.fast.rate_limit = '2/s'
        assert limiter.reset() == {'fast'}
        assert round(fast_bucket.expected_time(), 6) == 1.0
        assert round(slow_bucket.expected_time(), 6) == 1.0


class test_FairConsumer:
    def get_consumer(self, app):
        app.conf.worker_rate_limit_scheduler = 'fair'
        consumer = Consumer(
            on_task_request=Mock(),
            pool=Mock(),
            app=app,
            timer=Mock(),
            controller=Mock(),
            hub=None,
        )
        consumer.qos = Mock()
        consumer.task_consumer = Mock()
        consumer.task_consumer._active_tags = {
            'high': 'tag-high',
            'other': 'tag-other',
        }
        consumer.app.amqp.queues = {
            'high': Queue('high'),
            'other': Queue('other'),
        }
        return consumer

    def test_over_limit_message_requeues_and_pauses_only_source_queue(self):
        app = Celery(set_as_current=False)
        app.conf.worker_rate_limit_scheduler = 'fair'

        @app.task(name='high', rate_limit='1/s')
        def high():
            pass

        consumer = self.get_consumer(app)
        first = Mock(name='first')
        first.task = high
        first.request_dict = {'rate_limit_scope': None}
        first.message.no_ack = False
        second = Mock(name='second')
        second.task = high
        second.request_dict = {'rate_limit_scope': None}
        second.message.no_ack = False
        second.message.delivery_info = {'consumer_tag': 'tag-high'}
        second.delivery_info = {'routing_key': 'high'}
        consumer.task_consumer.consuming_from.return_value = True

        consumer._limit_task(first, consumer.get_task_bucket(high, first), 1)
        first.reject.assert_not_called()
        consumer.on_task_request.assert_called_once_with(first)

        manager = Mock()
        manager.attach_mock(consumer.task_consumer.cancel_by_queue, 'cancel')
        manager.attach_mock(second.reject, 'reject')
        consumer._limit_task(second, consumer.get_task_bucket(high, second), 1)

        second.reject.assert_called_once_with(requeue=True)
        consumer.task_consumer.cancel_by_queue.assert_called_once_with('high')
        assert manager.mock_calls[0] == call.cancel('high')
        assert 'high' in consumer.paused_rate_limit_queues
        assert 'other' not in consumer.paused_rate_limit_queues
        assert consumer.task_consumer.consuming_from('other') is True

    def test_different_scopes_do_not_share_token_grants(self):
        app = Celery(set_as_current=False)
        app.conf.worker_rate_limit_scheduler = 'fair'

        @app.task(name='high', rate_limit='1/s')
        def high():
            pass

        @app.task(name='normal', rate_limit='1/s')
        def normal():
            pass

        consumer = self.get_consumer(app)
        high_request = Mock()
        high_request.task = high
        high_request.request_dict = {'rate_limit_scope': None}
        high_request.message.no_ack = False
        normal_request = Mock()
        normal_request.task = normal
        normal_request.request_dict = {'rate_limit_scope': None}
        normal_request.message.no_ack = False

        consumer._limit_task(
            high_request, consumer.get_task_bucket(high), 1)
        consumer._limit_task(
            normal_request, consumer.get_task_bucket(normal), 1)

        assert consumer.on_task_request.call_args_list == [
            call(high_request), call(normal_request),
        ]
        high_request.reject.assert_not_called()
        normal_request.reject.assert_not_called()

    def test_no_ack_request_uses_legacy_bucket(self):
        app = Celery(set_as_current=False)
        app.conf.worker_rate_limit_scheduler = 'fair'

        @app.task(name='fire-and-forget', rate_limit='1/s')
        def fire_and_forget():
            pass

        consumer = self.get_consumer(app)
        request = Mock()
        request.task = fire_and_forget
        request.message.channel.no_ack_consumers = {'tag-high'}
        request.message.delivery_info = {'consumer_tag': 'tag-high'}
        bucket = consumer.get_task_bucket(fire_and_forget, request)

        assert isinstance(bucket, TokenBucket)

        consumer._limit_task(request, bucket, 1)

        request.reject.assert_not_called()
        consumer.on_task_request.assert_called_once_with(request)

    def test_post_eta_consumes_token_after_wall_clock_eta(self):
        app = Celery(set_as_current=False)
        app.conf.worker_rate_limit_scheduler = 'fair'

        @app.task(name='retry', rate_limit='1/s')
        def retry_task():
            pass

        consumer = self.get_consumer(app)
        request = Mock()
        request.task = retry_task
        request.message.no_ack = False

        consumer._limit_post_eta(request, consumer.get_task_bucket(retry_task), 1)

        consumer.qos.decrement_eventually.assert_called_once_with()
        consumer.on_task_request.assert_called_once_with(request)

    def test_on_close_keeps_token_state_across_reconnect(self):
        app = Celery(set_as_current=False)
        app.conf.worker_rate_limit_scheduler = 'fair'

        @app.task(name='durable', rate_limit='1/s')
        def durable():
            pass

        consumer = self.get_consumer(app)
        request = Mock()
        request.task = durable
        request.request_dict = {'rate_limit_scope': None}
        request.message.no_ack = False
        request.message.delivery_info = {'consumer_tag': 'tag-high'}
        bucket = consumer.get_task_bucket(durable, request)

        consumer._limit_task(request, bucket, 1)
        second = Mock()
        second.task = durable
        second.request_dict = {'rate_limit_scope': None}
        second.message.no_ack = False
        second.message.delivery_info = {'consumer_tag': 'tag-high'}
        consumer.task_consumer.consuming_from.return_value = True
        consumer._limit_task(
            second, consumer.get_task_bucket(durable, second), 1)
        scheduled_at = bucket.next_available
        consumer.on_close()

        assert consumer.fair_rate_limiter.bucket_for(durable, request) is bucket
        assert bucket.next_available == scheduled_at
        assert not consumer.paused_rate_limit_queues
        consumer.timer.cancel.assert_called()
