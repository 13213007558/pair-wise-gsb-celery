"""Persistent revocation rules for stamped task headers."""
from __future__ import annotations

import hashlib
import json
import re
import time
from collections import defaultdict

from kombu.utils.encoding import bytes_to_str

from celery.exceptions import ImproperlyConfigured

__all__ = ('RevocationRule', 'RevocationRegistry', 'get_persistent_revocation_backend',
           'normalize_headers')

EXACT = 'exact'
REGEX = 'regex'


def _stringify(value):
    return bytes_to_str(value) if isinstance(value, bytes) else str(value)


def _as_list(value):
    return list(value) if isinstance(value, (list, tuple, set, frozenset)) else [value]


def _json_default(value):
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    if isinstance(value, bytes):
        return bytes_to_str(value)
    raise TypeError(f'Object of type {type(value)!r} is not JSON serializable')


def normalize_headers(headers):
    if isinstance(headers, (list, tuple)):
        parsed = {}
        for item in headers:
            key, separator, value = str(item).partition('=')
            if not separator:
                raise ValueError(f'Invalid stamped header revocation: {item!r}')
            parsed[key] = value
        headers = parsed
    if not isinstance(headers, dict) or not headers:
        raise ValueError('Stamped header revocations must contain at least one header')
    return {
        str(key): sorted({_stringify(value) for value in _as_list(values)})
        for key, values in sorted(headers.items())
    }


class RevocationRule:
    def __init__(self, headers, match=EXACT, expires_at=None, id=None, reason=None,
                 created_at=None):
        if match not in (EXACT, REGEX):
            raise ValueError(f'Unsupported revocation match strategy: {match!r}')
        self.headers = normalize_headers(headers)
        self.match = match
        self.expires_at = None if expires_at is None else float(expires_at)
        self.created_at = float(created_at if created_at is not None else time.time())
        self.id = id or self._make_id()
        self.reason = reason or self.default_reason
        self._patterns = None

    def _make_id(self):
        payload = json.dumps({'headers': self.headers, 'match': self.match},
                             sort_keys=True, separators=(',', ':'),
                             default=_json_default)
        return hashlib.sha256(payload.encode()).hexdigest()

    @property
    def default_reason(self):
        parts = ', '.join(f'{key}={values[0] if len(values) == 1 else values}'
                          for key, values in sorted(self.headers.items()))
        return f'revoked by stamped headers ({self.match}: {parts})'

    @property
    def patterns(self):
        if self._patterns is None:
            self._patterns = {
                key: [re.compile(pattern) for pattern in patterns]
                for key, patterns in self.headers.items()
            } if self.match == REGEX else {}
        return self._patterns

    def expired(self, now=None):
        return self.expires_at is not None and self.expires_at <= (now or time.time())

    def matches(self, stamps):
        if not isinstance(stamps, dict):
            return None
        matched = {}
        for key, expected in self.headers.items():
            if key not in stamps:
                return None
            actual = {_stringify(value)
                      for value in _as_list(stamps[key])}
            if self.match == EXACT:
                values = actual & set(expected)
                if not values:
                    return None
                matched[key] = sorted(values)
            else:
                for value in actual:
                    if not any(pattern.search(value) for pattern in self.patterns[key]):
                        return None
                matched[key] = sorted(actual)
        return matched

    def as_dict(self):
        return {'id': self.id, 'headers': self.headers, 'match': self.match,
                'expires_at': self.expires_at, 'created_at': self.created_at,
                'reason': self.reason}

    @classmethod
    def from_dict(cls, data):
        return cls(data['headers'], match=data.get('match', EXACT),
                   expires_at=data.get('expires_at'), id=data.get('id'),
                   reason=data.get('reason'), created_at=data.get('created_at'))


class RevocationRegistry:
    def __init__(self):
        self.rules = {}
        self.legacy = defaultdict(list)
        self.persistent = None

    def add(self, rule, now=None):
        rule = rule if isinstance(rule, RevocationRule) else RevocationRule.from_dict(rule)
        now = now or time.time()
        if rule.expired(now):
            return None
        old = self.rules.get(rule.id)
        if old is None or _later(rule, old):
            self.rules[rule.id] = rule
        if rule.match == EXACT:
            for key, values in rule.headers.items():
                self.legacy[key] = sorted(set(self.legacy[key]) | set(values))
        return self.rules[rule.id]

    def match(self, stamped_headers, stamps=None, now=None):
        now = now or time.time()
        self.purge(now)
        keys = set(stamps or {})
        if isinstance(stamped_headers, (list, tuple, set, frozenset, dict)):
            keys.update(stamped_headers)
        for rule in self.rules.values():
            if not keys.isdisjoint(rule.headers):
                matched = rule.matches(stamps or {})
                if matched is not None:
                    return rule, matched
        return None, None

    def snapshot(self):
        now = time.time()
        self.purge(now)
        return [rule.as_dict() for rule in sorted(self.rules.values(), key=lambda r: r.id)]

    def merge(self, rules, now=None):
        now = now or time.time()
        return [rule for rule in (self.add(data, now) for data in rules or []) if rule]

    def replace(self, rules, now=None):
        now = now or time.time()
        self.rules.clear()
        self.legacy.clear()
        for data in rules or []:
            self.add(data, now=now)

    def load_persistent(self, now=None):
        if self.persistent is not None:
            now = now or time.time()
            self.replace(self.persistent.load(now=now), now=now)

    def purge(self, now=None):
        now = now if now is not None else time.time()
        expired = [id for id, rule in self.rules.items() if rule.expired(now)]
        for id in expired:
            self.rules.pop(id, None)
        if expired:
            values = defaultdict(set)
            for rule in self.rules.values():
                if rule.match == EXACT:
                    for key, vals in rule.headers.items():
                        values[key].update(vals)
            self.legacy = defaultdict(list, {key: sorted(vals) for key, vals in values.items()})
        return expired

    def clear(self):
        self.rules.clear()
        self.legacy.clear()

    def update(self, headers):
        for key, values in headers.items():
            self.add(RevocationRule({key: values}))

    def get(self, key, default=None):
        values = self.legacy.get(key)
        return values if values else default

    def __getitem__(self, key):
        return self.legacy[key]

    def keys(self):
        return self.legacy.keys()

    def __contains__(self, key):
        return key in self.legacy


def _later(candidate, existing):
    if candidate.expires_at is None:
        return True
    if existing.expires_at is None:
        return False
    return candidate.expires_at > existing.expires_at


class PersistentRevocationBackend:
    keyprefix = 'celery-task-revoked-headers-'

    def __init__(self, backend, max_rules=50000, now=time.time):
        self.backend = backend
        self.max_rules = max_rules
        self.now = now

    @staticmethod
    def supports(backend):
        return all(hasattr(backend, name) for name in ('get', 'set', 'delete'))

    def rule_key(self, rule_id):
        return f'{self.keyprefix}{rule_id}'

    def index_key(self):
        return f'{self.keyprefix}index'

    def encode(self, value):
        return json.dumps(value, sort_keys=True, default=_json_default)

    def decode(self, value):
        if value is None:
            return None
        return json.loads(bytes_to_str(value) if isinstance(value, bytes) else value)

    def ttl(self, rule, configured):
        if rule.expires_at is not None:
            return max(0, int(rule.expires_at - self.now()))
        return int(configured) if configured else None


class GenericPersistentRevocationBackend(PersistentRevocationBackend):
    def save(self, rule, ttl=None):
        self.backend.set(self.rule_key(rule.id), self.encode(rule.as_dict()))
        ttl = self.ttl(rule, ttl)
        if ttl:
            self.backend.expire(self.rule_key(rule.id), ttl)
        index = [item for item in self._index() if item['id'] != rule.id]
        index.append({'id': rule.id, 'expires_at': rule.expires_at,
                      'created_at': rule.created_at})
        index = self._limit(index)
        self.backend.set(self.index_key(), self.encode(index))

    def load(self, now=None):
        now = now or self.now()
        active, retained, index = [], [], self._index()
        for item in index:
            data = self.decode(self.backend.get(self.rule_key(item['id'])))
            if data is None:
                continue
            rule = RevocationRule.from_dict(data)
            if rule.expired(now):
                self.backend.delete(self.rule_key(rule.id))
            else:
                active.append(data)
                retained.append(item)
        if len(retained) != len(index):
            self.backend.set(self.index_key(), self.encode(retained))
        return active

    def cleanup(self, now=None):
        self.load(now)

    def _index(self):
        try:
            index = self.decode(self.backend.get(self.index_key())) or []
        except (TypeError, ValueError):
            return []
        return sorted(index, key=lambda item: item.get('expires_at') or 0,
                      reverse=True)[:self.max_rules]

    def _limit(self, index):
        index.sort(key=lambda item: (item.get('expires_at') is None,
                                     item.get('expires_at') or 0,
                                     item.get('created_at') or 0), reverse=True)
        for item in index[self.max_rules:]:
            self.backend.delete(self.rule_key(item['id']))
        return index[:self.max_rules]


class RedisPersistentRevocationBackend(GenericPersistentRevocationBackend):
    def __init__(self, backend, max_rules=50000, now=time.time):
        super().__init__(backend, max_rules=max_rules, now=now)
        self.client = backend.client

    @staticmethod
    def supports(backend):
        return type(backend).__module__ == 'celery.backends.redis' and hasattr(backend, 'client')

    def save(self, rule, ttl=None):
        ttl = self.ttl(rule, ttl)
        if ttl is not None and ttl <= 0:
            return
        pipe = self.client.pipeline()
        if ttl:
            pipe.setex(self.rule_key(rule.id), ttl, self.encode(rule.as_dict()))
        else:
            pipe.set(self.rule_key(rule.id), self.encode(rule.as_dict()))
        pipe.zadd(self.index_key(), {rule.id: rule.created_at})
        pipe.execute()
        overflow = self.client.zcard(self.index_key()) - self.max_rules
        if overflow > 0:
            self.client.zremrangebyrank(self.index_key(), 0, overflow - 1)

    def load(self, now=None):
        now = now or self.now()
        self.cleanup(now)
        ids = self.client.zrange(self.index_key(), 0, -1)
        values = self.client.mget([self.rule_key(bytes_to_str(id)) for id in ids])
        rules = [self.decode(value) for value in values if value]
        expired = [data['id'] for data in rules
                   if RevocationRule.from_dict(data).expired(now)]
        if expired:
            self.client.zrem(*([self.index_key()] + expired))
        return [data for data in rules
                if not RevocationRule.from_dict(data).expired(now)]

    def cleanup(self, now=None):
        now = now or self.now()
        expired = []
        for rule_id in self.client.zrange(self.index_key(), 0, -1):
            if self.client.get(self.rule_key(bytes_to_str(rule_id))) is None:
                expired.append(rule_id)
        if expired:
            self.client.zrem(*([self.index_key()] + expired))


def get_persistent_revocation_backend(backend, max_rules=50000):
    if RedisPersistentRevocationBackend.supports(backend):
        return RedisPersistentRevocationBackend(backend, max_rules=max_rules)
    raise ImproperlyConfigured(
        'Persistent stamped-header revocations require a Redis result backend.'
    )
