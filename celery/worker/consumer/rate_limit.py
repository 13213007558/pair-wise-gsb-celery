"""Fair per-task rate limiting for workers."""
from collections import OrderedDict
from time import monotonic

from celery.utils.time import rate

__all__ = ('FairRateLimiter', 'FairTokenBucket')


class FairTokenBucket:
    """Monotonic token bucket with a virtual admission clock.

    Elapsed real time can refill idle credit up to ``capacity`` intervals.
    Once a continuous stream starts, admitted work stays one interval apart,
    regardless of how late the scheduler runs.
    """

    def __init__(self, fill_rate, capacity=1, clock=monotonic):
        self.fill_rate = float(fill_rate)
        self.capacity = max(1, int(capacity))
        self.clock = clock
        self.next_available = clock() - self.capacity / fill_rate

    @property
    def interval(self):
        return 1.0 / self.fill_rate

    def update_rate(self, fill_rate):
        fill_rate = float(fill_rate)
        if fill_rate == self.fill_rate:
            return
        last_admission = self.next_available - self.interval
        self.fill_rate = fill_rate
        self.next_available = last_admission + self.interval

    def reserve(self, tokens=1):
        """Reserve one token and return its delay, or ``None`` when ready."""
        now = self.clock()
        if self.next_available > now:
            return self.next_available - now

        idle_credit = (self.capacity - 1) * self.interval
        self.next_available = max(
            self.next_available, now - idle_credit) + self.interval
        return None

    def expected_time(self, tokens=1):
        return max(0.0, self.next_available - self.clock())


class FairRateLimiter:
    """Registry of durable, scope-keyed monotonic token buckets."""

    def __init__(self, app, clock=monotonic):
        self.app = app
        self.clock = clock
        self.burst = max(1, int(app.conf.worker_rate_limit_burst or 1))
        self.buckets = OrderedDict()
        self.static_scopes = set()

    def scope_for(self, task, request=None):
        scope = None
        if request is not None:
            scope = request.request_dict.get('rate_limit_scope')
        if scope is None:
            scope = getattr(task, 'rate_limit_scope', None)
            if callable(scope) and request is not None:
                scope = scope(request)
        if scope is None:
            scope = self.app.conf.worker_rate_limit_default_scope
        return task.name if scope is None else str(scope)

    def bucket_for(self, task, request=None):
        return self.get_bucket(
            task.name, getattr(task, 'rate_limit', None),
            self.scope_for(task, request))

    def get_bucket(self, task_name, limit, scope=None):
        scope = task_name if scope is None else str(scope)
        fill_rate = rate(limit)
        if not fill_rate:
            return self.buckets.pop(scope, None)

        bucket = self.buckets.get(scope)
        if bucket is None:
            self.prune_reclaimable()
            bucket = FairTokenBucket(
                fill_rate, capacity=self.burst, clock=self.clock)
            self.buckets[scope] = bucket
        else:
            self.buckets.move_to_end(scope)
            bucket.update_rate(fill_rate)
            bucket.capacity = self.burst
        return bucket

    def reset(self):
        before = {scope: bucket.next_available
                  for scope, bucket in self.buckets.items()}
        limits_by_scope = {}
        for task in self.app.tasks.values():
            fill_rate = rate(getattr(task, 'rate_limit', None))
            if not fill_rate:
                continue
            scope = getattr(task, 'rate_limit_scope', None)
            if callable(scope):
                continue
            if scope is None:
                scope = self.app.conf.worker_rate_limit_default_scope
            if scope is None:
                scope = task.name
            limits_by_scope.setdefault(str(scope), fill_rate)

        removed_scopes = self.static_scopes - set(limits_by_scope)
        self.static_scopes = set(limits_by_scope)
        for scope, fill_rate in limits_by_scope.items():
            self.get_bucket(scope, fill_rate, scope)
        for scope in removed_scopes:
            self.buckets.pop(scope, None)
        self.prune_reclaimable()
        changed_scopes = {
            scope for scope, scheduled_at in before.items()
            if scope not in self.buckets
            or self.buckets[scope].next_available != scheduled_at
        }
        return changed_scopes | removed_scopes

    def prune_reclaimable(self):
        for scope, bucket in tuple(self.buckets.items()):
            if scope not in self.static_scopes and bucket.expected_time() <= 0:
                self.buckets.pop(scope, None)
