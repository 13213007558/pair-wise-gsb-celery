import itertools
import time

import pytest
from billiard.einfo import ExceptionInfo

from celery.concurrency.base import BasePool

pytest.importorskip('multiprocessing')


def do_something(i):
    return i * i


def long_something():
    time.sleep(1)


def raise_something(i):
    try:
        raise KeyError('FOO EXCEPTION')
    except KeyError:
        return ExceptionInfo()


class test_TaskPool:

    def setup_method(self):
        from celery.concurrency.prefork import TaskPool
        self.TaskPool = TaskPool

    def test_attrs(self):
        p = self.TaskPool(2)
        assert p.limit == 2
        assert p._pool is None

    def x_apply(self):
        p = self.TaskPool(2)
        p.start()
        scratchpad = {}
        proc_counter = itertools.count()

        def mycallback(ret_value):
            process = next(proc_counter)
            scratchpad[process] = {}
            scratchpad[process]['ret_value'] = ret_value

        myerrback = mycallback

        res = p.apply_async(do_something, args=[10], callback=mycallback)
        res2 = p.apply_async(raise_something, args=[10], errback=myerrback)
        res3 = p.apply_async(do_something, args=[20], callback=mycallback)

        assert res.get() == 100
        time.sleep(0.5)
        assert scratchpad.get(0)['ret_value'] == 100

        assert isinstance(res2.get(), ExceptionInfo)
        assert scratchpad.get(1)
        time.sleep(1)
        assert isinstance(scratchpad[1]['ret_value'], ExceptionInfo)
        assert scratchpad[1]['ret_value'].exception.args == ('FOO EXCEPTION',)

        assert res3.get() == 400
        time.sleep(0.5)
        assert scratchpad.get(2)['ret_value'] == 400

        res3 = p.apply_async(do_something, args=[30], callback=mycallback)

        assert res3.get() == 900
        time.sleep(0.5)
        assert scratchpad.get(3)['ret_value'] == 900
        p.stop()


class StubPool(BasePool):
    """BasePool subclass recording lifecycle callbacks."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []

    def on_start(self):
        self.calls.append('start')

    def on_close(self):
        self.calls.append('close')

    def on_stop(self):
        self.calls.append('stop')

    def on_terminate(self):
        self.calls.append('terminate')


class test_BasePool_shutdown:

    def test_close_is_idempotent(self):
        pool = StubPool(2)
        pool.start()
        pool.close()
        pool.close()
        assert pool.calls == ['start', 'close']
        assert pool._state == pool.CLOSE

    def test_stop_is_idempotent(self):
        pool = StubPool(2)
        pool.start()
        pool.stop()
        pool.stop()
        assert pool.calls == ['start', 'stop']
        assert pool._state == pool.TERMINATE

    def test_terminate_is_idempotent(self):
        pool = StubPool(2)
        pool.start()
        pool.terminate()
        pool.terminate()
        assert pool.calls == ['start', 'terminate']

    def test_terminate_after_stop_runs_once(self):
        # cold shutdown escalating after a warm stop must still
        # terminate the pool, exactly once.
        pool = StubPool(2)
        pool.start()
        pool.stop()
        pool.terminate()
        pool.terminate()
        assert pool.calls == ['start', 'stop', 'terminate']

    def test_close_does_not_regress_terminated_state(self):
        pool = StubPool(2)
        pool.start()
        pool.terminate()
        pool.close()
        assert pool._state == pool.TERMINATE
        assert pool.calls == ['start', 'terminate']

    def test_restart_resets_terminate_guard(self):
        pool = StubPool(2)
        pool.start()
        pool.terminate()
        pool.start()
        pool.terminate()
        assert pool.calls == ['start', 'terminate', 'start', 'terminate']

    def test_state_name(self):
        pool = StubPool(2)
        assert pool.state_name == 'initializing'
        pool.start()
        assert pool.state_name == 'running'
        pool.close()
        assert pool.state_name == 'closing'
        pool.stop()
        assert pool.state_name == 'terminating'
