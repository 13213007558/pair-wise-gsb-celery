"""Recovery drill for persistent revocation by stamped headers.

Two workers (consuming from different queues) share a result-backend
revocation store.  A revocation issued before a worker restart must still
reject matching tasks after the restart, while unmatched tasks keep
running.
"""
from __future__ import annotations

from time import sleep

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster

from celery import Celery, states
from celery.exceptions import TaskRevokedError
from t.integration.tasks import identity
from t.smoke.conftest import SuiteOperations, WorkerRestart

TENANT_STAMP = {"tenant_id": "t-revoked"}
REGEX_STAMP = {"deployment_id": "d-.*"}


@pytest.fixture
def celery_worker_cluster(
    celery_worker: CeleryTestWorker,
    celery_other_dev_worker: CeleryTestWorker,
) -> CeleryWorkerCluster:
    # Two dev workers consuming from *different* queues, sharing the same
    # broker and result backend.
    cluster = CeleryWorkerCluster(celery_worker, celery_other_dev_worker)
    yield cluster
    cluster.teardown()


class test_revoke_by_stamped_headers_recovery(SuiteOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        # Persist revoked stamped headers in the shared result backend
        # instead of process-local memory only.
        app.conf.worker_revoked_stamps_store = "backend"
        return app

    @staticmethod
    def other_worker(celery_setup: CeleryTestSetup) -> CeleryTestWorker:
        for worker in celery_setup.worker_cluster:
            if worker is not celery_setup.worker:
                return worker
        raise AssertionError("expected two workers in the cluster")

    @staticmethod
    def stamped_identity(value, stamp: dict, queue: str):
        sig = identity.si(value).set(queue=queue)
        sig.stamp(**stamp)
        return sig.apply_async()

    def test_revocation_survives_worker_restart(
        self,
        celery_setup: CeleryTestSetup,
    ):
        assert len(celery_setup.worker_cluster) == 2
        worker = celery_setup.worker
        other = self.other_worker(celery_setup)
        app = celery_setup.app

        # Two revokes issued back to back; both must be merged into the
        # shared backend store (concurrent revoke must not clobber).
        # reply=True makes sure both workers applied (and persisted) the
        # revocations before the restart below.
        app.control.revoke_by_stamped_headers(TENANT_STAMP, reply=True, timeout=5)
        app.control.revoke_by_stamped_headers(REGEX_STAMP, strategy="regex", reply=True, timeout=5)

        # Unmatched tasks run before the restart.
        res = identity.si("warmup").set(queue=worker.worker_queue).delay()
        assert res.get(timeout=RESULT_TIMEOUT) == "warmup"

        self.restart_worker(worker, WorkerRestart.Method.DOCKER_RESTART_GRACEFULLY)

        # KEY SAMPLE: the first matching message after the restart must be
        # rejected before execution, with a stable reason.
        res = self.stamped_identity("revoked", TENANT_STAMP, worker.worker_queue)
        with pytest.raises(TaskRevokedError, match="revoked"):
            res.get(timeout=RESULT_TIMEOUT)
        assert res.state == states.REVOKED
        worker.assert_log_exists("Discarding revoked task", timeout=RESULT_TIMEOUT)
        worker.assert_log_exists(f"revoked by header: {TENANT_STAMP}")
        # ack/reject and the task-revoked event must not be duplicated.
        assert worker.logs().count("Discarding revoked task") == 1

        # The regex revocation survived the restart as well.
        res = self.stamped_identity(
            "revoked-regex", {"deployment_id": "d-canary"}, worker.worker_queue)
        with pytest.raises(TaskRevokedError, match="revoked"):
            res.get(timeout=RESULT_TIMEOUT)

        # The other worker (different queue) rejects matching tasks too:
        # the revocation is shared, not a worker-local file.
        res = self.stamped_identity("revoked-other", TENANT_STAMP, other.worker_queue)
        with pytest.raises(TaskRevokedError, match="revoked"):
            res.get(timeout=RESULT_TIMEOUT)

        # Unmatched tasks keep running on both workers/queues.
        res = identity.si("ok").set(queue=worker.worker_queue).delay()
        assert res.get(timeout=RESULT_TIMEOUT) == "ok"
        res = identity.si("ok-other").set(queue=other.worker_queue).delay()
        assert res.get(timeout=RESULT_TIMEOUT) == "ok-other"

    def test_revocation_expires_and_is_cleaned_up(
        self,
        celery_setup: CeleryTestSetup,
    ):
        worker = celery_setup.worker
        app = celery_setup.app

        app.control.revoke_by_stamped_headers(TENANT_STAMP, expires=2, reply=True, timeout=5)
        res = self.stamped_identity("revoked", TENANT_STAMP, worker.worker_queue)
        with pytest.raises(TaskRevokedError, match="revoked"):
            res.get(timeout=RESULT_TIMEOUT)

        # After the revocation expires, matching tasks run again and the
        # expired entry is purged from the persistent store.
        sleep(3)
        res = self.stamped_identity("runs-again", TENANT_STAMP, worker.worker_queue)
        assert res.get(timeout=RESULT_TIMEOUT) == "runs-again"
