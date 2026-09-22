"""Optional fair per-scope rate limiter for worker consumers."""

from __future__ import annotations

from collections import defaultdict, deque
from itertools import count
from time import monotonic

from celery.utils.time import rate

__all__ = ('FairRateLimiter', 'FairTokenBucket')


class FairTokenBucket:
    """Reserve monotonically increasing start times using a monotonic clock."""

    def __init__(self, fill_rate, capacity=1, now=monotonic):
        self.fill_rate = float(fill_rate)
        self.capacity = float(capacity)
        self.now = now
        self.interval = 1.0 / self.fill_rate
        self.reservations = []
        self.contents = deque()

    def reserve(self, amount=1, ready_at=None):
        return self.reserve_request(None, amount, ready_at)

    def reserve_request(self, request_id, amount=1, ready_at=None):
        now = self.now()
        earliest = now if ready_at is None else max(now, ready_at)
        if request_id is not None:
            for when, reserved_id in self.reservations:
                if reserved_id == request_id:
                    return when
        self.reservations = [
            (when, reserved_id)
            for when, reserved_id in self.reservations
            if when > earliest - self.interval
        ]
        allowed_at = earliest
        while True:
            active = [
                when for when, _ in self.reservations
                if allowed_at - self.interval < when <= allowed_at
            ]
            if len(active) < self.capacity * amount:
                break
            allowed_at = min(active) + self.interval
        self.reservations.append((allowed_at, None))
        self.reservations.sort()
        if request_id is not None:
            for position, (when, reserved_id) in enumerate(self.reservations):
                if reserved_id is None and when == allowed_at:
                    self.reservations[position] = (when, request_id)
                    break
        return allowed_at

    def cancel_reservation(self, when, request_id):
        for position, reservation in enumerate(self.reservations):
            reserved_at, reserved_id = reservation
            if reserved_at == when and reserved_id == request_id:
                del self.reservations[position]
                return

    def update_rate(self, fill_rate):
        self.fill_rate = float(fill_rate)
        self.interval = 1.0 / self.fill_rate

    def add(self, item):
        self.contents.append(item)

    def pop(self):
        return self.contents.popleft()

    def clear_pending(self):
        self.contents.clear()


class FairRateLimiter:
    """Bounded fair rate limiter.

    Token debt is kept in each bucket instead of in a timer callback.  A
    rejected message is returned to its broker queue, and its source queue is
    paused until the next token for that scope is available.  Consequently
    rejected traffic applies broker prefetch backpressure rather than growing
    an unbounded local queue.
    """

    def __init__(self, app, timer, dispatch, *, clock=monotonic,
                 max_pending=None, pause_queues=True, burst=1):
        self.app = app
        self.timer = timer
        self.dispatch = dispatch
        self.clock = clock
        self.max_pending = max_pending
        self.pause_queues = pause_queues
        self.burst = burst
        self.buckets = {}
        self._inactive_buckets = {}
        self._entries = defaultdict(deque)
        self._scheduled = {}
        self._paused_queues = {}
        self._resume_entries = {}
        self._entry_ids = count()

    def create_bucket(self, task):
        limit = rate(getattr(task, 'rate_limit', None))
        if limit:
            return FairTokenBucket(
                limit, capacity=self.burst, now=self.clock,
            )
        return None

    def get_or_create_scope_bucket(self, scope, configured):
        bucket = self.buckets.get(scope)
        if configured:
            inactive = self._inactive_buckets.pop(scope, None)
            if bucket is None and inactive is not None:
                inactive.update_rate(configured)
                bucket = inactive
                self.buckets[scope] = bucket
            elif bucket is None:
                bucket = FairTokenBucket(
                    configured, capacity=self.burst, now=self.clock,
                )
                self.buckets[scope] = bucket
            else:
                bucket.update_rate(configured)
        elif bucket is not None:
            self.buckets[scope] = None
            self._inactive_buckets[scope] = bucket
        else:
            self.buckets[scope] = None
        return self.buckets.get(scope)

    def reset_rate_limits(self, tasks):
        for name, task in tasks.items():
            limit = rate(getattr(task, 'rate_limit', None))
            bucket = self.buckets.get(name)
            if not limit:
                if bucket is not None:
                    self.buckets[name] = None
                    self._inactive_buckets[name] = bucket
                continue
            inactive = self._inactive_buckets.pop(name, None)
            if bucket is None:
                if inactive is None:
                    bucket = FairTokenBucket(
                        limit, capacity=self.burst, now=self.clock,
                    )
                else:
                    bucket = inactive
                    bucket.update_rate(limit)
                self.buckets[name] = bucket
            else:
                bucket.update_rate(limit)
            explicit = getattr(task, 'rate_limit_scope', None)
            if explicit is None:
                continue
            configured = self.app.conf.worker_rate_limit_scopes.get(explicit)
            if configured is not None:
                configured = rate(configured)
                self.get_or_create_scope_bucket(
                    explicit, configured,
                )
                continue
            if limit:
                scope = (name, str(explicit))
                scope_bucket = self.buckets.get(scope)
                if scope_bucket is not None:
                    scope_bucket.update_rate(limit)

    def scope_for(self, request, task):
        explicit = request.request_dict.get('rate_limit_scope')
        if explicit is None:
            explicit = getattr(task, 'rate_limit_scope', None)
        if explicit is None:
            return request.type

        configured = self.app.conf.worker_rate_limit_scopes.get(explicit)
        if configured is not None:
            configured = rate(configured)
            self.get_or_create_scope_bucket(explicit, configured)
            return explicit

        task_limit = rate(getattr(task, 'rate_limit', None))
        if not task_limit:
            return request.type

        scope = (request.type, str(explicit))
        bucket = self.buckets.get(scope)
        if bucket is None:
            bucket = self.create_bucket(task)
            self.buckets[scope] = bucket
        else:
            bucket.update_rate(task_limit)
        return scope

    def bucket_for(self, request, task=None):
        if task is None:
            task = request.task
        scope = self.scope_for(request, task)
        bucket = self.buckets.get(scope)
        if bucket is None and scope == request.type:
            bucket = self.create_bucket(task)
            self.buckets[scope] = bucket
        if bucket is None:
            bucket = self.buckets.get(request.type)
        return bucket

    def limit(self, request, task, ready_at=None, release_prefetch=False,
              callbacks=()):
        bucket = self.bucket_for(request, task)
        if bucket is None:
            self._dispatch(request, callbacks)
            return request

        request_key = (
            getattr(request, 'id', id(request)),
            request.request_dict.get('retries', 0),
        )
        allowed_at = bucket.reserve_request(request_key, ready_at=ready_at)
        now = self.clock()
        if allowed_at <= now:
            self._dispatch(request, callbacks)
            return request

        pending = self._entries[bucket]
        capacity = self._pending_capacity()
        if self.pause_queues or len(pending) >= capacity:
            requeued = self._requeue(request, bucket, allowed_at)
            if requeued and release_prefetch:
                self._decrement_prefetch()
            return None if requeued else request

        entry_id = next(self._entry_ids)
        entry = (entry_id, request, allowed_at, callbacks)
        pending.append(entry)
        self._track_pending(request)
        self._schedule(bucket, allowed_at)
        return request

    def _pending_capacity(self):
        if self.max_pending is not None:
            return self.max_pending
        return 1

    def _schedule(self, bucket, when=None):
        if self._scheduled.get(bucket) is not None:
            return
        if when is None:
            entries = self._entries.get(bucket)
            if not entries:
                return
            when = entries[0][2]
        delay = max(0.0, when - self.clock())
        self._scheduled[bucket] = self.timer.call_after(
            delay, self._due, (bucket,), priority=6,
        )

    def _due(self, bucket):
        self._scheduled.pop(bucket, None)
        now = self.clock()
        while self._entries.get(bucket):
            _, request, allowed_at, callbacks = self._entries[bucket][0]
            if allowed_at > now:
                self._schedule(bucket, allowed_at)
                return
            self._entries[bucket].popleft()
            self._dispatch(request, callbacks)

    def cancel(self, request):
        for bucket, entries in self._entries.items():
            for position, entry in enumerate(entries):
                if entry[1] is request or entry[1].id == request.id:
                    del entries[position]
                    scheduled = self._scheduled.pop(bucket, None)
                    if scheduled is not None:
                        scheduled.cancel()
                    bucket.cancel_reservation(entry[2], (
                        entry[1].id,
                        entry[1].request_dict.get('retries', 0),
                    ))
                    from celery.worker.state import requests, scheduled_requests

                    scheduled_requests.discard(entry[1])
                    requests.pop(entry[1].id, None)
                    if entries:
                        self._schedule(bucket, entries[0][2])
                    return True
        return False

    def _dispatch(self, request, callbacks=()):
        from celery.worker.state import task_reserved

        task_reserved(request)
        for callback in callbacks:
            callback(request)
        self.dispatch(request)

    def _track_pending(self, request):
        from celery.worker.state import requests, task_scheduled

        requests[request.id] = request
        task_scheduled(request)

    def _requeue(self, request, bucket, when):
        queue_name = self._queue_name(request)
        paused = bool(
            self.pause_queues and queue_name
            and self._pause_queue(queue_name, when)
        )
        if not paused and self.pause_queues:
            pending = self._entries[bucket]
            if len(pending) < self._pending_capacity():
                request_key = (
                    request.id,
                    request.request_dict.get('retries', 0),
                )
                entry_id = next(self._entry_ids)
                pending.append(
                    (entry_id, request, when, ()),
                )
                bucket.reservations.append((when, request_key))
                bucket.reservations.sort()
                self._track_pending(request)
                self._schedule(bucket, when)
                return False
        request.reject(requeue=True)
        return True

    def _queue_name(self, request):
        task_consumer = getattr(self, 'task_consumer', None)
        delivery = request.delivery_info or {}
        consumer_tag = delivery.get('consumer_tag')
        if task_consumer is not None and consumer_tag is not None:
            for name, tag in getattr(task_consumer, '_active_tags', {}).items():
                if tag == consumer_tag:
                    return name
        return delivery.get('routing_key') or delivery.get('exchange')

    def _pause_queue(self, queue_name, when):
        if queue_name not in self._paused_queues:
            task_consumer = getattr(self, 'task_consumer', None)
            queue = self.app.amqp.queues.consume_from.get(queue_name)
            if task_consumer is None or queue is None:
                return False
            try:
                task_consumer.cancel_by_queue(queue_name)
            except Exception:  # pragma: no cover - broker dependent
                return False
            self._paused_queues[queue_name] = queue

        delay = max(0.0, when - self.clock())
        old_entry = self._resume_entries.get(queue_name)
        if old_entry is not None:
            old_entry.cancel()
        self._resume_entries[queue_name] = self.timer.call_after(
            delay, self._resume_queue, (queue_name,), priority=6,
        )
        return True

    def _resume_queue(self, queue_name):
        self._resume_entries.pop(queue_name, None)
        queue = self._paused_queues.pop(queue_name, None)
        task_consumer = getattr(self, 'task_consumer', None)
        if queue is None or task_consumer is None:
            return
        task_consumer.add_queue(queue)
        task_consumer.consume()

    def _decrement_prefetch(self):
        consumer = getattr(self, 'consumer', None)
        if consumer is not None and getattr(consumer, 'qos', None) is not None:
            consumer.qos.decrement_eventually()

    def bind_consumer(self, consumer):
        self.consumer = consumer
        self.task_consumer = consumer.task_consumer

    def clear_pending(self):
        for bucket, entry in self._scheduled.items():
            if entry is not None:
                entry.cancel()
        self._scheduled.clear()
        for entries in self._entries.values():
            entries.clear()
        for entry in self._resume_entries.values():
            entry.cancel()
        self._resume_entries.clear()
        # Queues are re-established from the normal app queue set when the
        # consumer reconnects.  Token state intentionally remains.
        self._paused_queues.clear()
