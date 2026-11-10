"""Regression tests: signed-task messages from producer to worker dispatch.

These tests run the real producer path (``AMQP.send_task_message`` with
``task_serializer='signed-task'``) over an in-memory broker and feed the
resulting wire messages into the real consumer dispatch
(``Consumer.create_task_handler``), using the test keys/certificates from
``t/unit/security``.
"""
import json
from unittest.mock import MagicMock, Mock, patch
from uuid import uuid4

import pytest
from kombu import Connection, Exchange, Queue
from kombu.message import Message
from kombu.serialization import prepare_accept_content, registry

from celery.security.certificate import Certificate, CertStore
from celery.security.key import PrivateKey
from celery.security.signed_task import (SIGNED_TASK_CONTENT_TYPE,
                                         SignedTaskSerializer)
from celery.worker.consumer.consumer import Consumer
from t.unit.security import CERT1, KEY1
from t.unit.security.case import SecurityCase

RECORD_TASK = 't.signed_task.record'
OTHER_TASK = 't.signed_task.other'
QUEUE_NAME = 'signed-task-test'


class test_signed_task_dispatch(SecurityCase):

    @pytest.fixture(autouse=True)
    def _patch_expired(self):
        with patch.object(Certificate, 'has_expired', return_value=False):
            yield

    def setup_method(self):
        self.calls = []
        self.other_calls = []

        @self.app.task(name=RECORD_TASK, shared=False)
        def record(*args, **kwargs):
            self.calls.append((args, kwargs))
            return True

        @self.app.task(name=OTHER_TASK, shared=False)
        def other(*args, **kwargs):
            self.other_calls.append((args, kwargs))
            return True

        self.app.conf.update(
            task_serializer='signed-task',
            accept_content=['signed-task'],
            task_protocol=2,
        )
        store = CertStore()
        store.add_cert(Certificate(CERT1))
        serializer = SignedTaskSerializer(
            PrivateKey(KEY1), Certificate(CERT1), store)
        registry.register(
            'signed-task', serializer.serialize, serializer.deserialize,
            content_type=SIGNED_TASK_CONTENT_TYPE,
            content_encoding='utf-8')
        self.accept = prepare_accept_content(['signed-task'])
        self._conn = Connection('memory://')
        self._queue = Queue(
            QUEUE_NAME, Exchange(QUEUE_NAME), routing_key=QUEUE_NAME)
        self.consumer = self._make_consumer()
        self.on_task_received = self.consumer.create_task_handler()

    def teardown_method(self):
        try:
            registry.unregister('signed-task')
        except Exception:
            pass
        self._conn.close()

    def _make_consumer(self):
        consumer = Consumer(
            on_task_request=lambda req: req.execute(),
            init_callback=Mock(),
            pool=Mock(),
            app=self.app,
            timer=Mock(),
            controller=MagicMock(),
            hub=None,
        )
        consumer.controller.state.revoked = set()
        consumer.event_dispatcher = None
        consumer.update_strategies()
        return consumer

    def _publish(self, task=RECORD_TASK, args=(1, 2), kwargs=None, **options):
        """Run the real producer path and return the wire message."""
        task_id = str(uuid4())
        message = self.app.amqp.create_task_message(
            task_id, task, args=list(args), kwargs=kwargs or {}, **options)
        producer = self.app.amqp.Producer(self._conn)
        self.app.amqp.send_task_message(
            producer, task, message, queue=self._queue)
        wire = self._queue(self._conn).get(accept=['signed-task'])
        assert wire is not None
        return wire

    def _tampered(self, wire, mutate, **message_kwargs):
        """Wire message with the signed envelope mutated, signature kept."""
        envelope = json.loads(wire.body)
        mutate(envelope)
        return Message(
            body=json.dumps(envelope),
            content_type=wire.content_type,
            content_encoding=wire.content_encoding,
            headers=dict(wire.headers or {}),
            properties=dict(wire.properties or {}),
            delivery_info=dict(wire.delivery_info or {}),
            channel=self._conn.channel(),
            delivery_tag='tampered-1',
            accept=self.accept,
            **message_kwargs
        )

    def _assert_no_task_executed(self):
        assert self.calls == []
        assert self.other_calls == []

    def test_signed_envelope_has_no_outer_task_fields(self):
        wire = self._publish()
        assert wire.content_type == SIGNED_TASK_CONTENT_TYPE
        assert 'task' not in (wire.headers or {})
        assert 'id' not in (wire.headers or {})
        envelope = json.loads(wire.body)
        assert envelope['headers']['task'] == RECORD_TASK
        assert envelope['format'] == 'celery-signed-task'
        assert envelope['version'] == 1

    def test_valid_message_executes(self):
        wire = self._publish(args=(40, 2))
        self.on_task_received(wire)
        assert self.calls == [((40, 2), {})]
        assert wire.acknowledged

    def test_tampered_task_name_not_executed(self):
        wire = self._publish()
        message = self._tampered(
            wire, lambda e: e['headers'].update(task=OTHER_TASK))
        self.on_task_received(message)
        self._assert_no_task_executed()
        assert message.acknowledged

    def test_tampered_task_id_not_executed(self):
        wire = self._publish()
        message = self._tampered(
            wire, lambda e: e['headers'].update(id=str(uuid4())))
        self.on_task_received(message)
        self._assert_no_task_executed()
        assert message.acknowledged

    def test_tampered_content_type_not_executed(self):
        wire = self._publish()
        message = self._tampered(
            wire, lambda e: e.update(
                content_type='application/x-python-serialize'))
        self.on_task_received(message)
        self._assert_no_task_executed()
        assert message.acknowledged

    def test_tampered_content_encoding_not_executed(self):
        wire = self._publish()
        message = self._tampered(
            wire, lambda e: e.update(content_encoding='binary'))
        self.on_task_received(message)
        self._assert_no_task_executed()
        assert message.acknowledged

    def test_tampered_chain_not_executed(self):
        import base64
        wire = self._publish()

        def inject_chain(envelope):
            body = json.loads(base64.b64decode(envelope['body']))
            body[2]['chain'] = [
                {'task': OTHER_TASK, 'args': [], 'kwargs': {},
                 'options': {}},
            ]
            envelope['body'] = base64.b64encode(
                json.dumps(body).encode()).decode()

        message = self._tampered(wire, inject_chain)
        self.on_task_received(message)
        self._assert_no_task_executed()
        assert message.acknowledged

    @pytest.mark.parametrize('version', [0, 2, '1', None])
    def test_tampered_version_not_executed(self, version):
        wire = self._publish()
        message = self._tampered(wire, lambda e: e.update(version=version))
        self.on_task_received(message)
        self._assert_no_task_executed()
        assert message.acknowledged

    def test_tampered_outer_content_type_no_downgrade(self):
        # a signed envelope re-labeled as plain json must not be
        # dispatched through the unsigned protocol 2 path either.
        wire = self._publish()
        message = Message(
            body=wire.body,
            content_type='application/json',
            content_encoding=wire.content_encoding,
            headers={},
            properties={},
            delivery_info=dict(wire.delivery_info or {}),
            channel=self._conn.channel(),
            delivery_tag='tampered-2',
            accept=prepare_accept_content(['signed-task', 'json']),
        )
        self.on_task_received(message)
        self._assert_no_task_executed()
        assert message.acknowledged

    def test_unsigned_message_not_dispatched_as_signed(self):
        # an unsigned protocol 2 message cannot reach the task when only
        # signed-task content is accepted.
        body, content_type, content_encoding = registry.dumps(
            ([1, 2], {}, {'callbacks': None, 'errbacks': None,
                          'chain': None, 'chord': None}),
            serializer='json')
        message = Message(
            body=body,
            content_type=content_type,
            content_encoding=content_encoding,
            headers={'task': RECORD_TASK, 'id': str(uuid4())},
            properties={},
            delivery_info={},
            channel=self._conn.channel(),
            delivery_tag='unsigned-1',
            accept=self.accept,
        )
        self.on_task_received(message)
        self._assert_no_task_executed()
