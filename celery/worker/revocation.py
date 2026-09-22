"""Revocation of tasks by stamped headers.

This module implements the state, matching and (optional) persistent
storage used by the ``revoke_by_stamped_headers`` remote control command.

Revoking by stamped headers flags (header, value) pairs - e.g.
``tenant_id=t-1`` - so that any task message carrying a matching stamp is
discarded before it is executed.  Compared to revoking by task id this
state has to be:

- *matchable* against multiple stamps, using either exact value matching
  or regular expressions,
- *expirable*, so the state does not grow without bounds,
- *persistent* (optionally), so a worker restart - or another worker in
  the cluster - still rejects matching tasks that have not executed yet.
"""
import json
import re
from collections.abc import MutableMapping
from time import time

from celery.exceptions import ImproperlyConfigured
from celery.utils.functional import maybe_list
from celery.utils.imports import symbol_by_name
from celery.utils.log import get_logger

__all__ = (
    'EXACT', 'REGEX', 'STRATEGIES',
    'RevokedStamps',
    'matching_stamp_values',
    'BaseRevokedStampsStore', 'BackendRevokedStampsStore',
    'create_revocation_store',
)

logger = get_logger(__name__)

#: Match revoked stamps by exact value equality.
EXACT = 'exact'

#: Match revoked stamps by regular expression.
REGEX = 'regex'

#: Supported stamp matching strategies.
STRATEGIES = frozenset({EXACT, REGEX})

#: Serialization format version used by :meth:``RevokedStamps.as_dict``.
SERIALIZATION_VERSION = 1


def _expires_later(this, other):
    """Return True if expiry timestamp ``this`` outlives ``other``.

    ``None`` means "never expires" and outlives any concrete timestamp.
    """
    if other is None:
        return False
    return this is None or this > other


def _regex_match(pattern, value):
    try:
        return re.match(str(pattern), str(value)) is not None
    except re.error:
        return False


def matching_stamp_values(revoked_values, stamped_values, strategy=EXACT):
    """Return the stamped values matching any of the revoked values.

    Arguments:
        revoked_values: Revoked stamp value, or list of values; regular
            expressions when ``strategy`` is ``regex``.
        stamped_values: The task's stamp value, or list of values.
        strategy (str): ``exact`` (default) or ``regex``.

    Returns:
        list: The task's stamp values matching a revoked value
        (possibly empty).
    """
    matched = []
    for value in maybe_list(stamped_values):
        for revoked_value in maybe_list(revoked_values):
            if strategy == REGEX:
                hit = _regex_match(revoked_value, value)
            else:
                hit = value == revoked_value
            if hit:
                matched.append(value)
                break
    return matched


class RevokedStamps(MutableMapping):
    """Mapping of stamped headers flagged for revocation.

    Behaves like a dict mapping a header name to the list of revoked
    stamp values, but every ``(header, value)`` entry additionally carries
    a matching *strategy* (``exact`` or ``regex``) and an optional
    absolute *expiry* timestamp.

    The optional ``expires`` argument is the default time-to-live (in
    seconds) applied to entries added without an explicit expiry, used to
    keep the mapping from growing without bounds.
    """

    def __init__(self, expires=None):
        #: Default TTL in seconds for new entries (None: never expire).
        self.expires = expires
        # header -> {value: {'expires': float | None, 'strategy': str}}
        self._data = {}

    # -- mapping interface ---------------------------------------------
    def __getitem__(self, header):
        return list(self._data[header])

    def __setitem__(self, header, values):
        self._data[header] = {}
        self.add(header, values)

    def __delitem__(self, header):
        del self._data[header]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return f'<{type(self).__name__}: {self._data!r}>'

    def update(self, other=(), **kwargs):
        if isinstance(other, RevokedStamps):
            self.merge(other)
            if kwargs:
                self.update(kwargs)
            return
        for header, values in dict(other, **kwargs).items():
            self.add(header, values)

    # -- mutation -------------------------------------------------------
    def add(self, header, values, strategy=EXACT, expires=None, now=None):
        """Flag ``values`` of ``header`` as revoked.

        Arguments:
            header (str): Name of the stamped header.
            values: Stamp value, or list of stamp values, to revoke.
            strategy (str): ``exact`` (default) for value equality, or
                ``regex`` to treat the revoked values as regular
                expressions matched with :func:``re.match``.
            expires (float): Time-to-live of the revocation in seconds.
                If not given, the mapping default TTL is used; a false
                value means the revocation never expires.
        """
        if strategy not in STRATEGIES:
            raise ValueError(
                f'Unknown revoked stamp strategy: {strategy!r}; '
                f'must be one of {sorted(STRATEGIES)}')
        ttl = self.expires if expires is None else expires
        expires_at = None
        if ttl:
            expires_at = (time() if now is None else now) + ttl
        entries = self._data.setdefault(header, {})
        for value in maybe_list(values):
            entries[value] = {'expires': expires_at, 'strategy': strategy}
        self.purge(now)

    def merge(self, other):
        """Union-merge another :class:``RevokedStamps`` into this one.

        Entries present on both sides keep the expiry that outlives the
        other, so merging is commutative and safe to use when syncing
        state between workers.
        """
        for header, entries in other._data.items():
            mine = self._data.setdefault(header, {})
            for value, meta in entries.items():
                existing = mine.get(value)
                if existing is None or _expires_later(
                        meta['expires'], existing['expires']):
                    mine[value] = dict(meta)

    def purge(self, now=None):
        """Remove all expired entries."""
        now = time() if now is None else now
        for header in list(self._data):
            entries = self._data[header]
            for value, meta in list(entries.items()):
                expires = meta['expires']
                if expires is not None and expires <= now:
                    del entries[value]
            if not entries:
                del self._data[header]

    # -- matching -------------------------------------------------------
    def match(self, stamps, now=None):
        """Match a task's stamps against the revoked entries.

        A task is revoked when *any* of its stamped headers matches a
        revoked entry (logical OR over the stamps).

        Arguments:
            stamps (dict): Mapping of header name to the task's stamp
                value (or list of values), e.g. ``Request.stamps``.

        Returns:
            dict: ``{header: value}`` of the first matching task stamp,
            or None if nothing matches.
        """
        if not stamps or not self._data:
            return None
        now = time() if now is None else now
        for header, stamped_values in stamps.items():
            entries = self._data.get(header)
            if not entries:
                continue
            for value in maybe_list(stamped_values):
                if self._value_matches(entries, value, now):
                    return {header: value}
        return None

    def _value_matches(self, entries, value, now):
        for revoked_value, meta in entries.items():
            expires = meta['expires']
            if expires is not None and expires <= now:
                continue
            if meta['strategy'] == REGEX:
                if _regex_match(revoked_value, value):
                    return True
            elif value == revoked_value:
                return True
        return False

    def covers(self, other):
        """Return True if every entry of ``other`` is also revoked here."""
        for header, entries in other._data.items():
            mine = self._data.get(header)
            if not mine:
                return False
            for value in entries:
                if value not in mine:
                    return False
        return True

    # -- serialization --------------------------------------------------
    def as_dict(self):
        """Return a JSON/pickle-safe representation of this mapping."""
        return {
            'version': SERIALIZATION_VERSION,
            'stamps': {
                header: [
                    {'value': value,
                     'expires': meta['expires'],
                     'strategy': meta['strategy']}
                    for value, meta in entries.items()
                ]
                for header, entries in self._data.items()
            },
        }

    @classmethod
    def from_dict(cls, data, expires=None):
        """Create a :class:``RevokedStamps`` from :meth:``as_dict`` output."""
        obj = cls(expires=expires)
        obj.update_from_dict(data)
        return obj

    def update_from_dict(self, data):
        """Merge serialized state (see :meth:``as_dict``) into this mapping."""
        if not data:
            return
        if isinstance(data.get('stamps'), dict):
            data = data['stamps']
        for header, entries in data.items():
            mine = self._data.setdefault(header, {})
            for entry in entries:
                meta = {
                    'expires': entry.get('expires'),
                    'strategy': entry.get('strategy', EXACT),
                }
                value = entry['value']
                existing = mine.get(value)
                if existing is None or _expires_later(
                        meta['expires'], existing['expires']):
                    mine[value] = meta


class BaseRevokedStampsStore:
    """Interface for persistent revoked-stamps stores."""

    def load(self):
        """Load the persisted revoked stamps.

        Returns:
            RevokedStamps: the persisted state (may be empty).
        """
        raise NotImplementedError()

    def save(self, revoked_stamps):
        """Persist ``revoked_stamps``, merging with concurrently stored state.

        Returns:
            RevokedStamps: the merged state that is now stored.
        """
        raise NotImplementedError()


class BackendRevokedStampsStore(BaseRevokedStampsStore):
    """Stores revoked stamps in the result backend.

    The state is shared by every worker using the same result backend, so
    a revocation survives worker restarts and is visible cluster-wide
    without relying on a local state file.

    Concurrent revokes are merged with set-union semantics (a revoke is
    never lost because two workers wrote at the same time), and expired
    entries are purged on every read and write to keep the stored value
    bounded.
    """

    #: Backend key the serialized state is stored under.
    key = 'celery-revoked-stamps'

    def __init__(self, app, key=None):
        self.app = app
        self.key = key or self.key

    def __repr__(self):
        return f'<{type(self).__name__}: {self.key}>'

    @property
    def _backend(self):
        # imported here to avoid a circular import at module level.
        from celery.backends.base import BaseKeyValueStoreBackend
        backend = self.app.backend
        if not isinstance(backend, BaseKeyValueStoreBackend):
            raise ImproperlyConfigured(
                'worker_revoked_stamps_store="backend" requires a '
                'key-value store result backend (e.g. Redis), '
                f'got {backend!r}')
        return backend

    def load(self):
        data = self._backend.get(self.key)
        if not data:
            return RevokedStamps()
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        revoked = RevokedStamps.from_dict(json.loads(data))
        revoked.purge()
        return revoked

    def save(self, revoked_stamps, retries=3):
        current = None
        for _ in range(max(1, retries)):
            merged = self.load()
            merged.merge(revoked_stamps)
            merged.purge()
            if merged:
                self._backend.set(self.key, json.dumps(merged.as_dict()))
            else:
                self._backend.delete(self.key)
            # Verify a concurrent writer did not clobber the entries we
            # just merged; retry with the union of both states if so.
            current = self.load()
            if current.covers(revoked_stamps):
                break
        return current


def create_revocation_store(app):
    """Create the persistent revoked-stamps store configured for ``app``.

    Uses the :setting:``worker_revoked_stamps_store`` setting:

    - ``'memory'`` (default): no persistence, returns None.
    - ``'backend'``: persist in the key-value result backend.
    - a dotted path to a :class:``BaseRevokedStampsStore`` subclass, or an
      already instantiated store, for custom persistence.
    """
    setting = app.conf.get('worker_revoked_stamps_store', 'memory')
    if not setting or setting == 'memory':
        return None
    if setting == 'backend':
        return BackendRevokedStampsStore(app)
    if isinstance(setting, str):
        return symbol_by_name(setting)(app)
    if isinstance(setting, BaseRevokedStampsStore):
        return setting
    raise ImproperlyConfigured(
        'worker_revoked_stamps_store must be "memory", "backend", '
        'a BaseRevokedStampsStore subclass path or instance, '
        f'got {setting!r}')
