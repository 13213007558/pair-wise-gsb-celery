import re

import pytest

from celery.worker.revocation import (
    EXACT, REGEX, GenericPersistentRevocationBackend, RevocationRegistry,
    RevocationRule,
)


class FakeKVBackend:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)

    def expire(self, key, value):
        pass


class FakeRedisClient:
    def __init__(self):
        self.values = {}
        self.scores = {}

    def pipeline(self):
        return self

    def setex(self, key, ttl, value):
        self.values[key] = value
        return self

    def set(self, key, value):
        self.values[key] = value
        return self

    def zadd(self, key, mapping):
        self.scores.setdefault(key, {}).update(mapping)
        return self

    def zcard(self, key):
        return len(self.scores.get(key, {}))

    def zremrangebyrank(self, key, start, end):
        ordered = sorted(self.scores.get(key, {}), key=self.scores[key].get)
        for rule_id in ordered[start:end + 1 if end >= 0 else None]:
            self.scores[key].pop(rule_id, None)
            self.values.pop(f'celery-task-revoked-headers-{rule_id}', None)
        return self

    def zrem(self, key, *members):
        for member in members:
            self.scores.get(key, {}).pop(member, None)
        return self

    def execute(self):
        return []

    def zrange(self, key, start, end):
        return sorted(self.scores.get(key, {}),
                      key=lambda item: self.scores[key][item])

    def mget(self, keys):
        return [self.values.get(key) for key in keys]

    def get(self, key):
        return self.values.get(key)

    def zrangebyscore(self, key, minimum, maximum):
        return [item for item, score in self.scores.get(key, {}).items()
                if minimum <= score <= maximum]

    def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)

    def zremrangebyscore(self, key, minimum, maximum):
        for item in self.zrangebyscore(key, minimum, maximum):
            self.scores[key].pop(item, None)


class test_revocation_rule:
    def test_exact_match_requires_all_headers(self):
        rule = RevocationRule({'tenant': 't1', 'deployment': 'd1'})
        assert rule.matches({'tenant': 't1'}) is None
        assert rule.matches({'tenant': ['x', 't1'], 'deployment': 'd1'}) == {
            'tenant': ['t1'], 'deployment': ['d1'],
        }

    def test_regex_match(self):
        rule = RevocationRule({'task': 'tenant-42-.*'}, match=REGEX)
        assert rule.matches({'task': 'tenant-42-eta'}) == {'task': ['tenant-42-eta']}
        assert rule.matches({'task': 'tenant-43'}) is None

    def test_invalid_regex_is_reported(self):
        rule = RevocationRule({'task': '['}, match=REGEX)
        with pytest.raises(re.error):
            rule.matches({'task': 'task'})

    def test_stable_reason(self):
        rule = RevocationRule({'deployment': 'd1', 'tenant': 't1'})
        assert rule.reason == (
            'revoked by stamped headers '
            '(exact: deployment=d1, tenant=t1)'
        )


class test_persistent_revocation_backend:
    def test_restart_loads_rules_and_expired_entries_are_cleaned(self):
        backend = FakeKVBackend()
        now = 100.0
        store = GenericPersistentRevocationBackend(backend, now=lambda: now)
        active = RevocationRule({'tenant': 'active'}, expires_at=now + 10, created_at=now)
        expired = RevocationRule({'tenant': 'expired'}, expires_at=now - 1, created_at=now)
        store.save(active)
        store.save(expired)

        registry = RevocationRegistry()
        registry.persistent = GenericPersistentRevocationBackend(backend, now=lambda: now)
        registry.load_persistent(now=now)

        assert registry.match(['tenant'], {'tenant': 'active'}, now=now)[0] is not None
        assert registry.match(['tenant'], {'tenant': 'expired'}, now=now) == (None, None)
        assert expired.id not in backend.data[store.index_key()]

    def test_concurrent_rules_do_not_overwrite_each_other(self):
        backend = FakeKVBackend()
        first = GenericPersistentRevocationBackend(backend)
        second = GenericPersistentRevocationBackend(backend)
        first.save(RevocationRule({'tenant': 'one'}))
        second.save(RevocationRule({'tenant': 'two'}))

        loaded = RevocationRegistry()
        loaded.persistent = GenericPersistentRevocationBackend(backend)
        loaded.load_persistent()

        assert loaded.match(['tenant'], {'tenant': 'one'})[0] is not None
        assert loaded.match(['tenant'], {'tenant': 'two'})[0] is not None

    def test_max_rules_is_enforced_for_generic_backend(self):
        backend = FakeKVBackend()
        store = GenericPersistentRevocationBackend(backend, max_rules=1)
        old = RevocationRule({'tenant': 'old'}, created_at=1)
        new = RevocationRule({'tenant': 'new'}, created_at=2)
        store.save(old)
        store.save(new)

        registry = RevocationRegistry()
        registry.persistent = GenericPersistentRevocationBackend(backend, max_rules=1)
        registry.load_persistent()

        assert registry.match(['tenant'], {'tenant': 'new'})[0] is not None
        assert registry.match(['tenant'], {'tenant': 'old'}) == (None, None)

    def test_redis_style_ttl_index_cleanup(self):
        from celery.worker.revocation import RedisPersistentRevocationBackend

        RedisBackend = type('RedisBackend', (), {'__module__': 'celery.backends.redis'})
        redis_backend = RedisBackend()
        redis_backend.client = FakeRedisClient()
        store = RedisPersistentRevocationBackend(redis_backend, now=lambda: 10.0)
        rule = RevocationRule({'tenant': 't1'}, expires_at=20.0)
        store.save(rule)

        store.cleanup(now=19)
        assert store.load(now=19)[0]['id'] == rule.id
        redis_backend.client.delete(store.rule_key(rule.id))
        store.cleanup(now=20)
        assert store.load(now=20) == []


def test_registry_merges_snapshot_idempotently():
    rule = RevocationRule({'tenant': 't1'})
    first = RevocationRegistry()
    first.add(rule)
    second = RevocationRegistry()
    second.merge(first.snapshot())
    second.merge(first.snapshot())

    assert second.snapshot() == first.snapshot()
    assert second.match(['tenant'], {'tenant': 't1'})[0].id == rule.id


def test_two_workers_reject_first_matching_message_after_restart():
    backend = FakeKVBackend()
    now = 100.0
    first_store = GenericPersistentRevocationBackend(backend, now=lambda: now)
    second_store = GenericPersistentRevocationBackend(backend, now=lambda: now)
    worker_one = RevocationRegistry()
    worker_two = RevocationRegistry()
    worker_one.persistent = first_store
    worker_two.persistent = second_store

    worker_one.load_persistent(now=now)
    worker_two.load_persistent(now=now)
    rule = RevocationRule(
        {'tenant_id': 'tenant-1', 'deployment_id': 'deployment-1'},
        expires_at=now + 300, created_at=now,
    )
    first_store.save(rule, ttl=300)
    worker_one.load_persistent(now=now)
    worker_two.load_persistent(now=now)

    # Simulate worker two restarting after both workers had received the rule.
    restarted_worker = RevocationRegistry()
    restarted_worker.persistent = GenericPersistentRevocationBackend(
        backend, now=lambda: now,
    )
    restarted_worker.load_persistent(now=now)

    stamps = {'tenant_id': 'tenant-1', 'deployment_id': 'deployment-1'}
    expected = {
        'tenant_id': ['tenant-1'],
        'deployment_id': ['deployment-1'],
    }
    matched, matched_stamps = restarted_worker.match(
        ['tenant_id', 'deployment_id'], stamps, now=now,
    )
    assert matched.id == rule.id
    assert matched_stamps == expected
    assert restarted_worker.match(
        ['tenant_id', 'deployment_id'],
        {'tenant_id': 'tenant-1', 'deployment_id': 'deployment-2'},
        now=now,
    ) == (None, None)


def test_registry_purges_expired_rules():
    registry = RevocationRegistry()
    registry.add(RevocationRule({'tenant': 'old'}, expires_at=20, created_at=1), now=10)
    registry.add(RevocationRule({'tenant': 'new'}, expires_at=30, created_at=2), now=10)

    assert registry.match(['tenant'], {'tenant': 'old'}, now=10)[0] is not None
    assert registry.match(['tenant'], {'tenant': 'old'}, now=25) == (None, None)
    assert registry.match(['tenant'], {'tenant': 'new'}, now=25)[0] is not None
