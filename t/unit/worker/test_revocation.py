"""Tests for :mod:`celery.worker.revocation`."""
import json
from time import time
from unittest.mock import Mock

import pytest

from celery.backends.base import BaseKeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured
from celery.worker.revocation import (BackendRevokedStampsStore, RevokedStamps, create_revocation_store,
                                      matching_stamp_values)


class MemoryKeyValueBackend(BaseKeyValueStoreBackend):
    """In-memory key-value result backend for store tests."""

    def __init__(self, *args, **kwargs):
        self._data = {}
        super().__init__(*args, **kwargs)

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value):
        self._data[key] = value

    def delete(self, key):
        self._data.pop(key, None)


@pytest.fixture
def kv_backend(app):
    return MemoryKeyValueBackend(app=app)


@pytest.fixture
def store(kv_backend):
    return BackendRevokedStampsStore(Mock(backend=kv_backend))


class test_RevokedStamps_mapping:

    def test_set_get_scalar_and_list(self):
        revoked = RevokedStamps()
        revoked['tenant_id'] = 't-1'
        assert revoked['tenant_id'] == ['t-1']
        revoked['tenant_id'] = ['t-1', 't-2']
        assert revoked['tenant_id'] == ['t-1', 't-2']
        assert 'tenant_id' in revoked
        assert len(revoked) == 1
        assert list(revoked) == ['tenant_id']
        del revoked['tenant_id']
        assert 'tenant_id' not in revoked

    def test_update_from_plain_dict(self):
        revoked = RevokedStamps()
        revoked.update({'tenant_id': ['t-1', 't-2'], 'deployment_id': 'd-1'})
        assert revoked['tenant_id'] == ['t-1', 't-2']
        assert revoked['deployment_id'] == ['d-1']

    def test_clear(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', 't-1')
        revoked.clear()
        assert not revoked
        assert revoked.match({'tenant_id': 't-1'}) is None


class test_RevokedStamps_matching:

    def test_exact_match(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', ['t-1', 't-2'])
        assert revoked.match({'tenant_id': 't-1'}) == {'tenant_id': 't-1'}
        assert revoked.match({'tenant_id': 't-2'}) == {'tenant_id': 't-2'}
        assert revoked.match({'tenant_id': 't-3'}) is None

    def test_match_is_or_over_multiple_stamps(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', 't-1')
        revoked.add('deployment_id', 'web')
        # any single matching stamp revokes the task
        assert revoked.match({'tenant_id': 't-1', 'other': 'x'})
        assert revoked.match({'deployment_id': 'web'})
        assert revoked.match({'tenant_id': 'no', 'deployment_id': 'no'}) is None

    def test_match_list_valued_task_stamp(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', 't-2')
        assert revoked.match({'tenant_id': ['t-1', 't-2']}) == {
            'tenant_id': 't-2'}

    def test_match_empty(self):
        revoked = RevokedStamps()
        assert revoked.match({'tenant_id': 't-1'}) is None
        revoked.add('tenant_id', 't-1')
        assert revoked.match({}) is None
        assert revoked.match(None) is None

    def test_regex_match(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', 't-.*', strategy='regex')
        assert revoked.match({'tenant_id': 't-42'}) == {'tenant_id': 't-42'}
        assert revoked.match({'tenant_id': 't-1'}) is not None
        # re.match semantics: anchored at the start of the value
        assert revoked.match({'tenant_id': 'x-t-1'}) is None

    def test_regex_and_exact_coexist(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', 't-1')
        revoked.add('tenant_id', 'tmp-.*', strategy='regex')
        assert revoked.match({'tenant_id': 't-1'})
        assert revoked.match({'tenant_id': 'tmp-9'})
        assert revoked.match({'tenant_id': 't-2'}) is None

    def test_invalid_regex_never_matches(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', '[', strategy='regex')
        assert revoked.match({'tenant_id': '['}) is None

    def test_unknown_strategy_rejected(self):
        revoked = RevokedStamps()
        with pytest.raises(ValueError):
            revoked.add('tenant_id', 't-1', strategy='fuzzy')

    def test_matching_stamp_values_helper(self):
        assert matching_stamp_values(['a', 'b'], 'a') == ['a']
        assert matching_stamp_values('a', ['a', 'b']) == ['a']
        assert matching_stamp_values('a', 'b') == []
        assert matching_stamp_values(
            't-.*', ['x', 't-1'], strategy='regex') == ['t-1']


class test_RevokedStamps_expiry:

    def test_expires_after_ttl(self):
        revoked = RevokedStamps()
        now = time()
        revoked.add('tenant_id', 't-1', expires=10, now=now)
        assert revoked.match({'tenant_id': 't-1'}, now=now + 5)
        assert revoked.match({'tenant_id': 't-1'}, now=now + 11) is None

    def test_default_ttl_from_mapping(self):
        revoked = RevokedStamps(expires=10)
        now = time()
        revoked.add('tenant_id', 't-1', now=now)
        assert revoked.match({'tenant_id': 't-1'}, now=now + 11) is None

    def test_explicit_expires_overrides_default(self):
        revoked = RevokedStamps(expires=10)
        now = time()
        revoked.add('tenant_id', 't-1', expires=0, now=now)
        assert revoked.match({'tenant_id': 't-1'}, now=now + 3600)

    def test_purge_removes_expired_entries_and_empty_headers(self):
        revoked = RevokedStamps()
        now = time()
        revoked.add('tenant_id', 't-1', expires=10, now=now)
        revoked.add('tenant_id', 't-2', expires=100, now=now)
        revoked.add('deployment_id', 'd-1', expires=10, now=now)
        revoked.purge(now=now + 50)
        assert revoked['tenant_id'] == ['t-2']
        assert 'deployment_id' not in revoked

    def test_add_purges_previously_expired_entries(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', 't-1', expires=10, now=time() - 100)
        revoked.add('tenant_id', 't-2', expires=0)
        assert revoked['tenant_id'] == ['t-2']


class test_RevokedStamps_merge:

    def test_merge_is_union(self):
        this = RevokedStamps()
        other = RevokedStamps()
        this.add('tenant_id', 't-1')
        other.add('tenant_id', 't-2')
        other.add('deployment_id', 'd-1')
        this.merge(other)
        assert sorted(this['tenant_id']) == ['t-1', 't-2']
        assert this['deployment_id'] == ['d-1']

    def test_merge_keeps_longest_expiry(self):
        now = time()
        this = RevokedStamps()
        other = RevokedStamps()
        this.add('tenant_id', 't-1', expires=10, now=now)
        other.add('tenant_id', 't-1', expires=100, now=now)
        this.merge(other)
        assert this.match({'tenant_id': 't-1'}, now=now + 50)

    def test_merge_never_expiring_wins(self):
        now = time()
        this = RevokedStamps()
        other = RevokedStamps()
        this.add('tenant_id', 't-1', expires=10, now=now)
        other.add('tenant_id', 't-1', expires=0)
        this.merge(other)
        assert this.match({'tenant_id': 't-1'}, now=now + 3600)

    def test_update_with_revoked_stamps_merges(self):
        this = RevokedStamps()
        other = RevokedStamps()
        other.add('tenant_id', 't-1')
        this.update(other)
        assert this.match({'tenant_id': 't-1'})


class test_RevokedStamps_serialization:

    def test_roundtrip(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', ['t-1', 't-2'])
        revoked.add('deployment_id', 'web-.*', strategy='regex')
        data = revoked.as_dict()
        # must be JSON-safe for the result backend and gossip
        restored = RevokedStamps.from_dict(json.loads(json.dumps(data)))
        assert restored.match({'tenant_id': 't-1'})
        assert restored.match({'deployment_id': 'web-7'})
        assert restored.match({'deployment_id': 'api-1'}) is None

    def test_roundtrip_preserves_expiry(self):
        revoked = RevokedStamps()
        now = time()
        revoked.add('tenant_id', 't-1', expires=10, now=now)
        restored = RevokedStamps.from_dict(revoked.as_dict())
        assert restored.match({'tenant_id': 't-1'}, now=now + 5)
        assert restored.match({'tenant_id': 't-1'}, now=now + 11) is None

    def test_update_from_dict_merges(self):
        revoked = RevokedStamps()
        revoked.add('tenant_id', 't-1')
        revoked.update_from_dict(RevokedStamps.from_dict({
            'version': 1,
            'stamps': {
                'tenant_id': [
                    {'value': 't-2', 'expires': None, 'strategy': 'exact'},
                ],
            },
        }).as_dict())
        assert sorted(revoked['tenant_id']) == ['t-1', 't-2']

    def test_update_from_dict_empty(self):
        revoked = RevokedStamps()
        revoked.update_from_dict(None)
        revoked.update_from_dict({})
        assert not revoked

    def test_covers(self):
        this = RevokedStamps()
        other = RevokedStamps()
        this.add('tenant_id', ['t-1', 't-2'])
        other.add('tenant_id', 't-1')
        assert this.covers(other)
        assert not other.covers(this)
        other.add('deployment_id', 'd-1')
        assert not this.covers(other)


class test_BackendRevokedStampsStore:

    def test_load_empty(self, store):
        assert not store.load()

    def test_save_and_load_roundtrip(self, store):
        revoked = RevokedStamps()
        revoked.add('tenant_id', 't-1')
        revoked.add('deployment_id', 'web-.*', strategy='regex')
        store.save(revoked)
        loaded = store.load()
        assert loaded.match({'tenant_id': 't-1'})
        assert loaded.match({'deployment_id': 'web-1'})

    def test_save_merges_with_stored_state(self, store, kv_backend):
        first = RevokedStamps()
        first.add('tenant_id', 't-1')
        store.save(first)
        second = RevokedStamps()
        second.add('tenant_id', 't-2')
        merged = store.save(second)
        assert merged.match({'tenant_id': 't-1'})
        assert merged.match({'tenant_id': 't-2'})
        assert store.load().match({'tenant_id': 't-1'})

    def test_save_purges_expired_entries(self, store, kv_backend):
        expired = RevokedStamps()
        expired.add('tenant_id', 't-1', expires=10, now=time() - 100)
        kv_backend.set(store.key, json.dumps(expired.as_dict()))
        fresh = RevokedStamps()
        fresh.add('tenant_id', 't-2', expires=0)
        store.save(fresh)
        stored = json.loads(kv_backend.get(store.key))
        values = [e['value'] for e in stored['stamps']['tenant_id']]
        assert values == ['t-2']

    def test_save_deletes_key_when_empty(self, store, kv_backend):
        expired = RevokedStamps()
        expired.add('tenant_id', 't-1', expires=10, now=time() - 100)
        kv_backend.set(store.key, json.dumps(expired.as_dict()))
        store.save(RevokedStamps())
        assert store.key not in kv_backend._data

    def test_concurrent_saves_do_not_lose_revocations(
            self, store, kv_backend):
        other_store = BackendRevokedStampsStore(Mock(backend=kv_backend))
        first = RevokedStamps()
        first.add('tenant_id', 't-1')
        second = RevokedStamps()
        second.add('tenant_id', 't-2')
        store.save(first)
        other_store.save(second)
        loaded = store.load()
        assert loaded.match({'tenant_id': 't-1'})
        assert loaded.match({'tenant_id': 't-2'})

    def test_save_retries_when_concurrently_clobbered(
            self, store, kv_backend):
        # Simulate a concurrent writer overwriting our first write before
        # the verification read: the save must retry and converge to the
        # union instead of losing either revocation.
        clobber = RevokedStamps()
        clobber.add('tenant_id', 'other')
        clobbered = json.dumps(clobber.as_dict())
        original_set = kv_backend.set
        writes = []

        def interleaved_set(key, value):
            original_set(key, value)
            writes.append(value)
            if len(writes) == 1:
                original_set(key, clobbered)

        kv_backend.set = interleaved_set
        ours = RevokedStamps()
        ours.add('tenant_id', 't-1')
        merged = store.save(ours)
        assert merged.match({'tenant_id': 't-1'})
        assert merged.match({'tenant_id': 'other'})
        assert len(writes) >= 2

    def test_non_key_value_backend_rejected(self, app):
        from celery.backends.base import BaseBackend
        store = BackendRevokedStampsStore(Mock(backend=BaseBackend(app)))
        with pytest.raises(ImproperlyConfigured):
            store.load()


class test_create_revocation_store:

    def test_memory_by_default(self, app):
        app.conf.worker_revoked_stamps_store = 'memory'
        assert create_revocation_store(app) is None

    def test_backend(self, app):
        app.conf.worker_revoked_stamps_store = 'backend'
        store = create_revocation_store(app)
        assert isinstance(store, BackendRevokedStampsStore)

    def test_custom_path(self, app):
        app.conf.worker_revoked_stamps_store = (
            'celery.worker.revocation.BackendRevokedStampsStore')
        store = create_revocation_store(app)
        assert isinstance(store, BackendRevokedStampsStore)

    def test_store_instance_passthrough(self, app):
        instance = BackendRevokedStampsStore(app)
        app.conf.worker_revoked_stamps_store = instance
        assert create_revocation_store(app) is instance

    def test_invalid_setting(self, app):
        app.conf.worker_revoked_stamps_store = 42
        with pytest.raises(ImproperlyConfigured):
            create_revocation_store(app)
