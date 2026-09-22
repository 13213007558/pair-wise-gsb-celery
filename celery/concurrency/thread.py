"""Thread execution pool."""
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, wait
from concurrent.futures.thread import _threads_queues, _worker
import threading
import weakref
from typing import TYPE_CHECKING, Any, Callable

from .base import BasePool, apply_target

__all__ = ('TaskPool',)

if TYPE_CHECKING:
    from typing import TypedDict

    PoolInfo = TypedDict('PoolInfo', {'max-concurrency': int, 'threads': int})

    # `TargetFunction` should be a Protocol that represents fast_trace_task and
    # trace_task_ret.
    TargetFunction = Callable[..., Any]


class ApplyResult:
    def __init__(self, future: Future) -> None:
        self.f = future
        self.get = self.f.result

    def wait(self, timeout: float | None = None) -> None:
        wait([self.f], timeout)


class _DaemonThreadPoolExecutor(ThreadPoolExecutor):
    def _adjust_thread_count(self):
        if self._idle_semaphore.acquire(timeout=0):
            return
        def weakref_cb(_, queue=self._work_queue):
            queue.put(None)

        num_threads = len(self._threads)
        if num_threads >= self._max_workers:
            return
        worker_name = (
            f'{self._thread_name_prefix or self.__class__.__name__}_{num_threads}'
        )
        worker = threading.Thread(
            name=worker_name,
            target=_worker,
            args=(weakref.ref(self, weakref_cb), self._work_queue,
                  self._initializer, self._initargs),
        )
        worker.daemon = True
        self._threads.add(worker)
        _threads_queues[worker] = self._work_queue
        worker.start()


class TaskPool(BasePool):
    """Thread Task Pool."""
    limit: int

    body_can_be_buffer = True
    signal_safe = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._executor_lock = threading.Lock()
        self.executor = _DaemonThreadPoolExecutor(max_workers=self.limit)

    def on_stop(self) -> None:
        with self._executor_lock:
            executor, self.executor = self.executor, None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)
        super().on_stop()

    def on_terminate(self) -> None:
        with self._executor_lock:
            executor, self.executor = self.executor, None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        super().on_terminate()

    def on_apply(
        self,
        target: TargetFunction,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        callback: Callable[..., Any] | None = None,
        accept_callback: Callable[..., Any] | None = None,
        **_: Any
    ) -> ApplyResult:
        with self._executor_lock:
            if self.executor is None:
                raise RuntimeError('cannot apply to a stopped thread pool')
            f = self.executor.submit(apply_target, target, args, kwargs,
                                     callback, accept_callback)
        return ApplyResult(f)

    def terminate_job(self, pid, signal=None):
        # Running Python threads cannot be safely killed. Queued futures are
        # cancelled in on_terminate(); active workers are daemonized so a
        # repeated cold shutdown can still let the main process exit.
        pass

    def _get_info(self) -> PoolInfo:
        info = super()._get_info()
        info.update({
            'max-concurrency': self.limit,
            'threads': len(self.executor._threads) if self.executor else 0
        })
        return info
