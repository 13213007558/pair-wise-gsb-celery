import json
from unittest.mock import patch

import pytest
from kombu.exceptions import SerializerNotInstalled
from kombu.serialization import registry

from celery.exceptions import ImproperlyConfigured, SecurityError
from celery.security.certificate import Certificate, CertStore
from celery.security.key import PrivateKey
from celery.security.signed_task import (SIGNED_TASK_CONTENT_TYPE, SIGNED_TASK_FORMAT, SIGNED_TASK_VERSION,
                                         SignedTaskSerializer, canonical_dumps, register_signed_task)

from . import CERT1, CERT2, KEY1, KEY2
from .case import SecurityCase


def task_message(task='tasks.add', task_id='id-1'):
    return {
        'headers': {
            'task': task,
            'id': task_id,
            'eta': None,
            'timelimit': [None, None],
        },
        'properties': {'correlation_id': task_id, 'reply_to': ''},
        'body': (
            [1, 2], {'x': 1},
            {'callbacks': None, 'errbacks': None, 'chain': None, 'chord': None},
        ),
    }


def tamper(wire, mutate):
    envelope = json.loads(wire)
    mutate(envelope)
    return json.dumps(envelope)


class test_signed_task_serializer(SecurityCase):

    @pytest.fixture(autouse=True)
    def _patch_expired(self):
        with patch.object(Certificate, 'has_expired', return_value=False):
            yield

    def _get_s(self, key=KEY1, cert=CERT1, certs=(CERT1,)):
        store = CertStore()
        for c in certs:
            store.add_cert(Certificate(c))
        return SignedTaskSerializer(PrivateKey(key), Certificate(cert), store)

    def test_roundtrip(self):
        s = self._get_s()
        data = task_message()
        # the inner json serializer normalizes tuples to lists
        assert s.deserialize(s.serialize(data)) == json.loads(
            json.dumps(data))

    def test_canonical_encoding_is_unambiguous(self):
        assert canonical_dumps({'b': 1, 'a': {}}) == b'{"a":{},"b":1}'
        assert canonical_dumps({'b': 1, 'a': {}}) == \
            canonical_dumps({'a': {}, 'b': 1})

    def test_envelope_format_and_version(self):
        s = self._get_s()
        envelope = json.loads(s.serialize(task_message()))
        assert envelope['format'] == SIGNED_TASK_FORMAT
        assert envelope['version'] == SIGNED_TASK_VERSION
        assert envelope['signature']

    @pytest.mark.parametrize('mutate', [
        pytest.param(lambda e: e['headers'].update(task='tasks.evil'),
                     id='task-name'),
        pytest.param(lambda e: e['headers'].update(id='other-id'),
                     id='task-id'),
        pytest.param(lambda e: e['properties'].update(reply_to='elsewhere'),
                     id='properties'),
        pytest.param(lambda e: e.update(
            content_type='application/x-python-serialize'),
            id='content-type'),
        pytest.param(lambda e: e.update(content_encoding='binary'),
                     id='content-encoding'),
        pytest.param(lambda e: e.update(body='AAAA'), id='body'),
        pytest.param(lambda e: e.update(version=0), id='version-0'),
        pytest.param(lambda e: e.update(version=2), id='version-2'),
        pytest.param(lambda e: e.update(format='celery-signed-task2'),
                     id='format'),
        pytest.param(lambda e: e.update(signer='someone else'),
                     id='signer'),
    ])
    def test_tampered_envelope_rejected(self, mutate):
        s = self._get_s()
        wire = s.serialize(task_message())
        with pytest.raises(SecurityError):
            s.deserialize(tamper(wire, mutate))

    def test_tampered_body_embed_rejected(self):
        s = self._get_s()
        wire = s.serialize(task_message())

        def inject_chain(envelope):
            import base64
            body = json.loads(base64.b64decode(envelope['body']))
            body[2]['chain'] = [{'task': 'tasks.evil', 'args': []}]
            envelope['body'] = base64.b64encode(
                json.dumps(body).encode()).decode()

        with pytest.raises(SecurityError):
            s.deserialize(tamper(wire, inject_chain))

    def test_missing_signature_rejected(self):
        s = self._get_s()
        wire = s.serialize(task_message())
        with pytest.raises(SecurityError):
            s.deserialize(tamper(wire, lambda e: e.pop('signature')))

    def test_missing_field_rejected(self):
        s = self._get_s()
        wire = s.serialize(task_message())
        with pytest.raises(SecurityError):
            s.deserialize(tamper(wire, lambda e: e.pop('headers')))

    def test_not_json_rejected(self):
        s = self._get_s()
        with pytest.raises(SecurityError):
            s.deserialize('not json')
        with pytest.raises(SecurityError):
            s.deserialize('[1, 2, 3]')

    def test_unknown_signer_rejected(self):
        s1 = self._get_s(certs=(CERT2,))
        s2 = self._get_s()
        with pytest.raises(SecurityError):
            s1.deserialize(s2.serialize(task_message()))

    def test_unmatched_key_cert_rejected(self):
        signer = self._get_s(key=KEY2, cert=CERT2, certs=(CERT1, CERT2))
        verifier = self._get_s()
        with pytest.raises(SecurityError):
            verifier.deserialize(signer.serialize(task_message()))

    @pytest.mark.parametrize('data', [
        None,
        'foo',
        {},
        {'headers': {}, 'properties': {}},
        {'headers': {}, 'properties': {}, 'body': ()},
        {'headers': {'task': 't'}, 'properties': {}, 'body': ()},
        {'headers': {'id': 'i'}, 'properties': {}, 'body': ()},
        {'headers': [], 'properties': {}, 'body': ()},
    ])
    def test_serialize_requires_task_message(self, data):
        s = self._get_s()
        with pytest.raises(SecurityError):
            s.serialize(data)


class test_register_signed_task(SecurityCase):

    @pytest.fixture(autouse=True)
    def _patch_expired(self):
        with patch.object(Certificate, 'has_expired', return_value=False):
            yield

    def teardown_method(self):
        try:
            registry.unregister('signed-task')
        except SerializerNotInstalled:
            pass

    def test_register(self, tmp_path):
        store = tmp_path / 'certs'
        store.mkdir()
        (store / 'cert1.pem').write_text(CERT1)
        register_signed_task(KEY1, None, CERT1, str(store / '*'))
        data = task_message()
        content_type, content_encoding, wire = registry.dumps(
            data, serializer='signed-task')
        assert content_type == SIGNED_TASK_CONTENT_TYPE
        assert registry.loads(wire, content_type, content_encoding) == \
            json.loads(json.dumps(data))


class test_setup_security_signed_task(SecurityCase):

    @pytest.fixture(autouse=True)
    def _patch_expired(self):
        with patch.object(Certificate, 'has_expired', return_value=False):
            yield

    def teardown_method(self):
        registry._disabled_content_types.clear()
        registry._set_default_serializer('json')
        for name in ('auth', 'signed-task'):
            try:
                registry.unregister(name)
            except SerializerNotInstalled:
                pass

    def _setup_conf(self, tmp_path, **conf):
        key = tmp_path / 'key.pem'
        store = tmp_path / 'store'
        store.mkdir()
        cert = store / 'cert.pem'
        key.write_text(KEY1)
        cert.write_text(CERT1)
        self.app.conf.update(
            security_key=str(key),
            security_certificate=str(cert),
            security_cert_store=str(store),
            **conf
        )

    def test_signed_task_mode(self, tmp_path):
        self._setup_conf(
            tmp_path,
            task_serializer='signed-task',
            accept_content=['signed-task'],
        )
        self.app.setup_security()
        data = task_message()
        content_type, _, wire = registry.dumps(data, 'signed-task')
        assert content_type == SIGNED_TASK_CONTENT_TYPE
        assert registry.loads(wire, content_type, 'utf-8')

    def test_signed_task_mode_rejects_plain_accept(self, tmp_path):
        self._setup_conf(
            tmp_path,
            task_serializer='signed-task',
            accept_content=['json'],
        )
        with pytest.raises(ImproperlyConfigured):
            self.app.setup_security()

    def test_signed_task_mode_requires_protocol_2(self, tmp_path):
        self._setup_conf(
            tmp_path,
            task_serializer='signed-task',
            accept_content=['signed-task'],
            task_protocol=1,
        )
        with pytest.raises(ImproperlyConfigured):
            self.app.setup_security()

    def test_legacy_auth_needs_explicit_switch(self, tmp_path):
        self._setup_conf(
            tmp_path,
            task_serializer='signed-task',
            accept_content=['signed-task', 'auth'],
        )
        with pytest.raises(ImproperlyConfigured):
            self.app.setup_security()

    def test_legacy_auth_compat_switch(self, tmp_path):
        self._setup_conf(
            tmp_path,
            task_serializer='signed-task',
            accept_content=['signed-task', 'auth'],
            security_allow_legacy_auth=True,
        )
        self.app.setup_security()
        # both serializers are registered in compat mode
        _, _, signed_wire = registry.dumps(task_message(), 'signed-task')
        assert signed_wire
        _, _, auth_wire = registry.dumps('foo', 'auth')
        assert auth_wire
