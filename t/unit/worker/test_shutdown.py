"""Tests for the unified worker shutdown state machine."""
import threading

import pytest

from celery.worker.shutdown import ShutdownPhase, ShutdownState


class test_ShutdownPhase:

    def test_phases_are_ordered(self):
        assert ShutdownPhase.RUNNING < ShutdownPhase.DRAIN
        assert ShutdownPhase.DRAIN < ShutdownPhase.CANCEL
        assert ShutdownPhase.CANCEL < ShutdownPhase.WAIT_POOL
        assert ShutdownPhase.WAIT_POOL < ShutdownPhase.TERMINATE
        assert ShutdownPhase.TERMINATE < ShutdownPhase.CLOSE_RESOURCES
        assert ShutdownPhase.CLOSE_RESOURCES < ShutdownPhase.CLOSED


class test_ShutdownState:

    def test_starts_running(self):
        state = ShutdownState()
        assert state.phase == ShutdownPhase.RUNNING
        assert state.history == ()
        assert not state.is_at_least(ShutdownPhase.DRAIN)
        assert state.describe() == 'shutdown: still running'

    def test_enter_returns_true_exactly_once_per_phase(self):
        state = ShutdownState()
        assert state.enter(ShutdownPhase.DRAIN) is True
        # repeated shutdown signal: no transition, no repeated side effects
        assert state.enter(ShutdownPhase.DRAIN) is False
        assert state.phase == ShutdownPhase.DRAIN
        assert len(state.history) == 1

    def test_phases_are_monotonic(self):
        state = ShutdownState()
        state.enter(ShutdownPhase.CANCEL)
        # cannot go back to an earlier phase
        assert state.enter(ShutdownPhase.DRAIN) is False
        assert state.phase == ShutdownPhase.CANCEL

    def test_phases_may_be_skipped(self):
        # cold shutdown goes DRAIN -> TERMINATE without WAIT_POOL
        state = ShutdownState()
        assert state.enter(ShutdownPhase.DRAIN) is True
        assert state.enter(ShutdownPhase.TERMINATE) is True
        assert state.enter(ShutdownPhase.CLOSED) is True
        assert [p for p, _ in state.history] == [
            ShutdownPhase.DRAIN, ShutdownPhase.TERMINATE, ShutdownPhase.CLOSED,
        ]

    def test_is_at_least(self):
        state = ShutdownState()
        state.enter(ShutdownPhase.WAIT_POOL)
        assert state.is_at_least(ShutdownPhase.DRAIN)
        assert state.is_at_least(ShutdownPhase.WAIT_POOL)
        assert not state.is_at_least(ShutdownPhase.TERMINATE)

    def test_describe(self):
        state = ShutdownState()
        state.enter(ShutdownPhase.DRAIN)
        state.enter(ShutdownPhase.CLOSED)
        assert state.describe() == 'shutdown: DRAIN -> CLOSED'

    def test_history_records_monotonic_timestamps(self):
        state = ShutdownState()
        state.enter(ShutdownPhase.DRAIN)
        state.enter(ShutdownPhase.CLOSED)
        (p1, t1), (p2, t2) = state.history
        assert p1 == ShutdownPhase.DRAIN
        assert p2 == ShutdownPhase.CLOSED
        assert t1 <= t2

    def test_concurrent_enters_collapse_to_single_transition(self):
        state = ShutdownState()
        results = []

        def enter():
            results.append(state.enter(ShutdownPhase.DRAIN))

        threads = [threading.Thread(target=enter) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert results.count(True) == 1
        assert len(state.history) == 1

    def test_enter_validates_phase(self):
        state = ShutdownState()
        with pytest.raises(ValueError):
            state.enter('not-a-phase')

    def test_repr(self):
        state = ShutdownState()
        assert 'RUNNING' in repr(state)
        state.enter(ShutdownPhase.CLOSED)
        assert 'CLOSED' in repr(state)
