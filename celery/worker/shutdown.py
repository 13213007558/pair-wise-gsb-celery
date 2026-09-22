"""Unified worker shutdown state machine.

This module defines the ordered shutdown phases shared by the worker
blueprint, the consumer and the execution pool, so that a worker stop
(warm), terminate (cold), repeated signals, connection loss and embedded
shutdowns all drive the *same* monotonic sequence:

1. DRAIN -- stop receiving new tasks.  The task consumer is cancelled
   and QoS is left to drain; no new messages are accepted.
2. CANCEL -- cancel/revoke already-received requests according to pool
   capability.  Reserved (queued) requests are dropped so the broker can
   redeliver them; active (executing) requests that cannot finish are
   cancelled.  Every task_revoked/task-cancelled signal emitted here maps
   to exactly one request and one Request.shutdown_phase.
3. WAIT_POOL -- drain executing tasks (warm shutdown).  Late acks, soft
   time limits, task_postrun and task signals all complete here, while
   the broker connection and result backend are still open.
4. TERMINATE -- cold shutdown: terminate remaining pool jobs after the
   (soft shutdown) timeout instead of waiting for them.
5. CLOSE_RESOURCES -- close the result backend and the broker
   connection.  This phase must never be entered before WAIT_POOL (warm)
   or TERMINATE (cold) completed, or in-flight acks and results would be
   lost.
6. CLOSED -- terminal state.  Entering it is idempotent, so repeated
   shutdown signals, worker crashes, abnormal child exits and autoscale
   events cannot wedge or re-run the teardown.

Every phase transition is recorded in ShutdownState.history as
(phase, monotonic timestamp) pairs so a shutdown can be audited stage
by stage.
"""
import threading
from enum import IntEnum
from time import monotonic

from celery.utils.log import get_logger

__all__ = ('ShutdownPhase', 'ShutdownState')

logger = get_logger(__name__)


class ShutdownPhase(IntEnum):
    """Ordered worker shutdown phases.

    The numeric order is significant: a ShutdownState only ever moves
    forward through these phases.
    """

    #: Normal operation; tasks are received and executed.
    RUNNING = 0
    #: Stopped receiving new tasks (task consumer cancelled).
    DRAIN = 1
    #: Cancelling/revoking received-but-unfinished requests.
    CANCEL = 2
    #: Draining executing tasks (warm shutdown).
    WAIT_POOL = 3
    #: Terminating remaining pool jobs (cold shutdown).
    TERMINATE = 4
    #: Closing result backend and broker connection.
    CLOSE_RESOURCES = 5
    #: Shutdown complete; terminal and idempotent.
    CLOSED = 6


class ShutdownState:
    """Thread-safe, monotonic shutdown phase tracker.

    enter() returns True exactly once per phase -- for the call that
    actually performs the transition -- so repeated shutdown signals,
    concurrent signal-handler invocations and double blueprint stops
    collapse onto a single transition and never repeat side effects
    (e.g. sending task_revoked twice for the same request, or closing
    the result backend before the pool has drained).
    """

    def __init__(self):
        self._phase = ShutdownPhase.RUNNING
        self._lock = threading.Lock()
        self._history = []

    @property
    def phase(self):
        """The current shutdown phase."""
        return self._phase

    @property
    def history(self):
        """Tuple of (phase, monotonic_timestamp) transitions so far."""
        return tuple(self._history)

    def enter(self, phase):
        """Advance to phase.

        Returns True if this call performed the transition, False if the
        state was already at or past phase.  Phases can be skipped (e.g.
        cold shutdown goes straight from DRAIN to TERMINATE) but never
        re-entered or reversed.
        """
        phase = ShutdownPhase(phase)
        with self._lock:
            if phase <= self._phase:
                return False
            self._phase = phase
            self._history.append((phase, monotonic()))
        logger.info('worker shutdown: entered phase %s', phase.name)
        return True

    def is_at_least(self, phase):
        """Return True if the state reached or passed phase."""
        return self._phase >= ShutdownPhase(phase)

    def describe(self):
        """Human-readable description of the recorded transitions."""
        if not self._history:
            return 'shutdown: still running'
        return 'shutdown: ' + ' -> '.join(
            phase.name for phase, _ in self._history)

    def __repr__(self):
        return f'<ShutdownState: {self._phase.name}>'
