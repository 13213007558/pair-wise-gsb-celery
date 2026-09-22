import pytest

from celery.app.task_protocol import validate_decoded_body, validate_headers_before_decode
from celery.exceptions import TaskProtocolError


class TestHeaderValidation:

    def test_missing_required_v2_field(self):
        with pytest.raises(TaskProtocolError, match='missing_field') as exc:
            validate_headers_before_decode(
                {'task': 'tasks.add'}, accepted_protocols={2},
            )
        assert exc.value.reason == 'missing_field'

    def test_unknown_version(self):
        with pytest.raises(TaskProtocolError, match='unsupported_version'):
            validate_headers_before_decode(
                {'task': 'tasks.add', 'id': 'id', 'task_protocol': 99},
                accepted_protocols={99},
            )

    def test_worker_version_range_mismatch(self):
        with pytest.raises(TaskProtocolError, match='version_mismatch'):
            validate_headers_before_decode(
                {'task': 'tasks.add', 'id': 'id', 'task_protocol': 3},
                accepted_protocols={1, 2},
            )

    def test_v3_extra_header_is_rejected(self):
        with pytest.raises(TaskProtocolError, match='extra_field'):
            validate_headers_before_decode({
                'task': 'tasks.add', 'id': 'id', 'root_id': 'root',
                'argsrepr': '()', 'kwargsrepr': '{}',
                'task_protocol': 3, 'unexpected': True,
            }, accepted_protocols={3})

    def test_stamped_header_requires_matching_stamp(self):
        with pytest.raises(TaskProtocolError, match='missing_field'):
            validate_headers_before_decode({
                'task': 'tasks.add', 'id': 'id',
                'task_protocol': 2,
                'stamped_headers': ['tenant'], 'stamps': {},
            }, accepted_protocols={2})


class TestBodyValidation:

    def test_v3_requires_four_item_envelope(self):
        with pytest.raises(TaskProtocolError, match='invalid_body'):
            validate_decoded_body(
                ((), {}, {}), protocol=3,
            )

    def test_v3_rejects_unknown_embed_fields(self):
        with pytest.raises(TaskProtocolError, match='extra_field'):
            validate_decoded_body((
                'celery_protocol3', (), {}, {'unexpected': True},
            ), protocol=3)

    def test_v2_rejects_non_sequence_args_before_execution(self):
        with pytest.raises(TaskProtocolError, match='invalid_field'):
            validate_decoded_body(
                ({}, {}, None), protocol=2,
            )
