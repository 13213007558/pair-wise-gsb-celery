#!/usr/bin/env python3
"""Runtime verification of the three review constraints, layer by layer.

Every check uses real Celery/kombu objects and in-memory transports
(memory:// broker, cache+memory:// result store) plus the test certificates.
Nothing here binds a network port or contacts a production service.

Layers, and what each is responsible for:
  1. setup_security ........ registers the auth serializer; fail-fast config
  2. task consumption ...... verifies signer against FSCertStore on every task
  3. result backend init ... builds the JSON accept gate used when reading
  4. event dispatch ........ publishes unsigned observability messages only

Exit status is non-zero if any baseline expectation fails. The output ends
with an explicit reminder that these checks do not certify a deployment.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from review_common import (C3, CERT_DIR, load_config_module, read_settings,
                           resolve_path, root_cause)

from celery import Celery
from celery.backends.cache import _DUMMY_CLIENT_CACHE
from celery.events import EventDispatcher
from celery.exceptions import ImproperlyConfigured
from kombu import Connection, Consumer, Exchange, Producer, Queue
from kombu.exceptions import ContentDisallowed

failures = []


def check(label: str, ok: bool, detail: str):
    status = 'PASS' if ok else 'FAIL'
    print(f'  [{status}] {label}: {detail}')
    if not ok:
        failures.append(label)


def section(title: str):
    print(f'\n--- {title} ---')


def app_for(config_name: str, name: str, env: dict | None = None,
            security: bool = True):
    settings = read_settings(load_config_module(config_name, env))
    app = Celery(name)
    app.conf.update(settings)
    if security:
        app.setup_security()
    return app


# --------------------------------------------------------------------------
# Layer 1: setup_security
# --------------------------------------------------------------------------
def layer_setup_security():
    section('Layer 1 - setup_security: serializer registration and fail-fast')

    app = app_for('baseline', 'l1-baseline')
    from kombu.serialization import registry
    check('L1 auth serializer registered', 'auth' in registry._encoders,
          "registry._encoders contains 'auth' after setup_security()")
    check('L1 default serializer is auth',
          registry._default_content_type == 'application/data',
          f"registry default content type = "
          f"{registry._default_content_type!r} (the auth envelope type)")

    # Fail-fast: the exact task_serializer/accept_content combination is
    # enforced inside celery/security/__init__.py.
    weak = Celery('l1-weak')
    weak.conf.update(task_serializer='json', accept_content=['json'])
    try:
        weak.setup_security()
        check('L1 rejects unsigned-task posture', False, 'no error raised')
    except ImproperlyConfigured as exc:
        check('L1 rejects unsigned-task posture', True,
              f'ImproperlyConfigured: {str(exc).splitlines()[0]}')

    # Fail-fast: missing key/cert/store paths.
    incomplete = Celery('l1-missing')
    incomplete.conf.update(task_serializer='auth', accept_content=['auth'])
    try:
        incomplete.setup_security()
        check('L1 rejects missing key material', False, 'no error raised')
    except ImproperlyConfigured as exc:
        check('L1 rejects missing key material', True,
              f'ImproperlyConfigured: {str(exc).splitlines()[0]}')


# --------------------------------------------------------------------------
# Layer 2: task consumption (real signed messages over memory://)
# --------------------------------------------------------------------------
def _publish_consume(app, exchange_name: str):
    exchange = Exchange(exchange_name, type='direct')
    queue = Queue(exchange_name + '-q', exchange=exchange, routing_key='rk')
    received = []
    with Connection('memory://') as conn:
        queue(conn.default_channel).declare()
        with Producer(conn) as producer:
            producer.publish({'task': 'review.echo', 'args': [1, 2]},
                             exchange=exchange, routing_key='rk',
                             serializer='auth')
        with Consumer(conn, queues=[queue], accept=['auth'],
                      callbacks=[lambda body, msg: received.append((body, msg))]):
            conn.drain_events(timeout=2)
    return received[0]


def layer_task_consumption():
    section('Layer 2 - task consumption: FSCertStore verification per message')

    baseline = app_for('baseline', 'l2-baseline')
    body, message = _publish_consume(baseline, 'l2-good')
    check('L2 signed task round-trips', body['task'] == 'review.echo',
          f'decoded body = {body}')
    check('L2 wire content type is the auth envelope',
          message.content_type == 'application/data',
          f"message.content_type = {message.content_type!r} (inner JSON is "
          'signed, not parsed, before verification)')

    # Tamper/rogue signer: a second certificate the trust store never saw.
    # The consumer-side SecureSerializer is built with the same FSCertStore
    # the worker uses, so this is the exact verification path a task takes.
    from celery.security.certificate import Certificate, FSCertStore
    from celery.security.key import PrivateKey
    from celery.security.serialization import SecureSerializer
    store = FSCertStore(resolve_path('certs/trusted/*.pem'))
    verifier = SecureSerializer(None, None, store)
    rogue_signer = SecureSerializer(
        PrivateKey(open(os.path.join(CERT_DIR, 'issuer-rogue.key')).read()),
        Certificate(open(os.path.join(CERT_DIR, 'issuer-rogue.crt')).read()),
        store)
    envelope = rogue_signer.serialize({'task': 'review.echo'})
    try:
        verifier.deserialize(envelope)
        check('L2 rogue signer rejected', False, 'rogue envelope accepted')
    except Exception as exc:  # kombu/celery wrap the underlying SecurityError
        check('L2 rogue signer rejected',
              'Unknown certificate' in root_cause(exc),
              f'root cause -> {root_cause(exc)}')

    # Incomplete trust stores must fail closed at consumption.
    for variant in ('empty', 'wrong'):
        app = app_for('incomplete_trust_counterexample', f'l2-{variant}',
                      env={'STORE_VARIANT': variant})
        try:
            _publish_consume(app, f'l2-{variant}')
            check(f'L2 {variant} store fails closed', False, 'task accepted')
        except Exception as exc:
            check(f'L2 {variant} store fails closed',
                  'Unknown certificate' in root_cause(exc),
                  f'consumption error root cause -> {root_cause(exc)}')


# --------------------------------------------------------------------------
# Layer 3: result backend initialization and the JSON read gate
# --------------------------------------------------------------------------
class _Canary:
    """Flips a flag if pickle deserialization ever executes."""
    triggered = False

    def __reduce__(self):
        return (_trip_canary, ())


def _trip_canary():
    _Canary.triggered = True
    return 'untrusted object constructed'


def layer_result_backend():
    section('Layer 3 - result backend: accept gate built at initialization')

    # setup_security() globally disables non-allowlisted kombu serializers.
    # The attack models a writer that legitimately has pickle enabled, so we
    # snapshot and restore the global registry around this layer.
    from kombu.serialization import registry
    saved_disabled = set(registry._disabled_content_types)
    try:
        registry._disabled_content_types.clear()
        _DUMMY_CLIENT_CACHE.clear()
        baseline = app_for('baseline', 'l3-baseline')
        backend = baseline.backend
        check('L3 backend accept list is JSON-only',
              backend.accept == {'application/json'},
              f'Backend.accept = {sorted(backend.accept)} (from '
              'result_accept_content, enforced in kombu loads())')
        check('L3 backend writes JSON',
              backend.content_type == 'application/json',
              f'Backend.content_type = {backend.content_type!r}')

        task_id = 'l3-task-1'
        backend.store_result(task_id, {'ok': True}, 'SUCCESS')
        meta = backend.get_task_meta(task_id)
        check('L3 legitimate JSON result decodes',
              meta['result'] == {'ok': True} and meta['status'] == 'SUCCESS',
              f"meta = {meta['status']} {meta['result']!r}")

        # Attacker model: a separate, pickle-configured writer shares the store.
        # It targets a fresh key, so its dedup read does not meet the JSON
        # record written by the baseline backend above.
        pickle_task_id = 'l3-task-pickle'
        writer_app = Celery('l3-writer', backend='cache+memory://')
        writer_app.conf.result_serializer = 'pickle'
        writer_app.conf.result_accept_content = ['pickle']
        writer = writer_app.backend
        writer.store_result(pickle_task_id, _Canary(), 'SUCCESS')
        stored = writer.client.get(writer.get_key_for_task(pickle_task_id))
        check('L3 injected record is a pickle blob', stored[:2] == b'\x80\x04',
              f'first bytes = {stored[:2]!r}')

        # Baseline reader: serializer switched to pickle (migration), JSON gate on.
        reader = Celery('l3-reader', backend='cache+memory://')
        reader.conf.result_serializer = 'pickle'
        reader.conf.result_accept_content = ['json']
        reader_backend = reader.backend
        _Canary.triggered = False
        try:
            reader_backend.get_task_meta(pickle_task_id)
            check('L3 JSON gate blocks pickle record', False,
                  'pickle deserialized')
        except ContentDisallowed as exc:
            check('L3 JSON gate blocks pickle record',
                  not _Canary.triggered, f'ContentDisallowed: {exc}')

        # Counterexample A: load its actual config first to show the missing
        # result_accept_content, then model the open read path that this gap
        # permits (shared store / serializer migration with pickle enabled).
        counter_settings = read_settings(
            load_config_module('task_only_counterexample'))
        check('L3 counterexample has no result accept gate',
              counter_settings.get('result_accept_content') is None,
              f"result_accept_content = "
              f"{counter_settings.get('result_accept_content')!r}")
        open_app = Celery('l3-open', backend='cache+memory://')
        open_app.conf.result_serializer = 'pickle'
        open_app.conf.result_accept_content = ['pickle']
        open_backend = open_app.backend
        _Canary.triggered = False
        try:
            decoded = open_backend.get_task_meta(pickle_task_id)
            executed = (_Canary.triggered and
                        decoded['result'] == 'untrusted object constructed')
            check('L3 open backend executes injected pickle (documents the C2 hole)',
                  executed, f"decoded result = {decoded['result']!r}, "
                  f'canary triggered = {_Canary.triggered}')
        except ContentDisallowed as exc:
            check('L3 open backend executes injected pickle (documents the C2 hole)',
                  False, f'unexpectedly blocked: {exc}')
    finally:
        registry._disabled_content_types.clear()
        registry._disabled_content_types.update(saved_disabled)
        registry._set_default_serializer('auth')


# --------------------------------------------------------------------------
# Layer 4: event dispatch (unsigned, forgeable)
# --------------------------------------------------------------------------
def layer_event_dispatch():
    section('Layer 4 - event dispatch: observability messages, unsigned')

    app = Celery('l4-events', broker='memory://')
    app.conf.event_serializer = 'json'
    exchange = Exchange('l4-celeryev', type='direct')
    routing_key = 'task.succeeded'
    queue = Queue('l4-q', exchange=exchange, routing_key=routing_key)

    with Connection('memory://') as conn:
        queue(conn.default_channel).declare()
        dispatcher = EventDispatcher(conn, app=app, enabled=True)
        dispatcher.exchange = exchange
        dispatcher.send('task-succeeded', uuid='real-task', result=42,
                        routing_key=routing_key)

        # Anyone able to publish to the event exchange can forge an event;
        # EventDispatcher._publish itself adds no signature header.
        with Producer(conn) as producer:
            producer.publish(
                {'type': 'task-succeeded', 'hostname': 'worker@forged',
                 'uuid': 'task-that-never-ran', 'result': 999, 'clock': 1},
                exchange=exchange.name, routing_key=routing_key,
                serializer='json', headers={})

        events = []
        with Consumer(conn, queues=[queue], accept=['json'],
                      callbacks=[lambda body, msg: events.append((body, msg.headers))]):
            conn.drain_events(timeout=1)
            conn.drain_events(timeout=1)

    forged = [b for b, _ in events if b['uuid'] == 'task-that-never-ran']
    check('L4 forged completion event is accepted', bool(forged),
          f'received {len(events)} events, including = {forged}')
    unsigned = all('signature' not in (headers or {}) for _, headers in events)
    check('L4 events carry no signature header', unsigned,
          f'headers seen = {[h for _, h in events]}')
    print(f'  [{C3}] conclusion: task-succeeded events cannot prove completion; '
          'use AsyncResult/the result backend for completion decisions')


def main() -> int:
    print('Runtime verification with real Celery objects; memory transport and '
          'test certificates only.')
    layer_setup_security()
    layer_task_consumption()
    layer_result_backend()
    layer_event_dispatch()

    print('\n=== runtime summary ===')
    if failures:
        for label in failures:
            print(f'  FAIL: {label}')
        print(f'{len(failures)} check(s) failed.')
        return 1
    print('  all baseline expectations held; both counterexamples exhibited '
          'their documented failure.')
    print('  Scope: this proves behavior of the reviewed configuration in an '
          'isolated in-memory environment.')
    print('  It is NOT evidence that the production deployment is secure: '
          'broker ACLs, result-store access,')
    print('  certificate lifecycle, and application event-handling code still '
          'need separate operational review.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
