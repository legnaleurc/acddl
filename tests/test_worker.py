import functools
import unittest
from unittest import mock as um

from tornado import ioloop as ti, gen as tg

from acddl import worker


class TestWorker(unittest.TestCase):

    def setUp(self):
        self._worker = worker.Worker()
        self._worker.start()
        self.assertTrue(self._worker.is_alive())

    def tearDown(self):
        async_call(self._worker.stop)
        self.assertFalse(self._worker.is_alive())

    def testDo(self):
        x = [1]
        def fn():
            x[0] = 2
        async def fn_():
            await self._worker.do(fn)
        async_call(fn_)
        self.assertEqual(x[0], 2)


class TestAsyncWorker(unittest.TestCase):

    def setUp(self):
        self._worker = worker.AsyncWorker()
        self._worker.start()
        self.assertTrue(self._worker.is_alive)

    def tearDown(self):
        async_call(self._worker.stop)
        self.assertFalse(self._worker.is_alive)

    def _createSyncMock(self):
        return um.Mock(return_value=42)

    def _createAsyncMock(self):
        return AsyncMock(return_value=42)

    def testDoWithSync(self):
        fn = self._createSyncMock()
        rv = async_call(self._worker.do, fn)
        fn.assert_called_once_with()
        self.assertEqual(rv, 42)

    def testDoWithAsync(self):
        fn = self._createAsyncMock()
        rv = async_call(self._worker.do, fn)
        fn.assert_called_once_with()
        fn.assert_awaited()
        self.assertEqual(rv, 42)

    def testDoLaterWithSync(self):
        x = [1]
        def fn():
            x[0] = 2
        self._worker.do_later(fn)
        async_call(functools.partial(tg.sleep, 0.001))
        self.assertEqual(x[0], 2)

    def testDoLaterWithAsync(self):
        x = [1]
        async def fn():
            await tg.moment
            x[0] = 2
        self._worker.do_later(fn)
        async_call(functools.partial(tg.sleep, 0.001))
        self.assertEqual(x[0], 2)

    def testDoWithSyncPartial(self):
        x = [1]
        def fn(rv):
            x[0] = 2
            return rv
        async def fn_():
            return await self._worker.do(functools.partial(fn, 3))
        rv = async_call(fn_)
        self.assertEqual(x[0], 2)
        self.assertEqual(rv, 3)

    def testDoWithAsyncPartial(self):
        x = [1]
        async def fn(rv):
            await tg.moment
            x[0] = 2
            return rv
        async def fn_():
            return await self._worker.do(worker.AsyncTask(fn, 3))
        rv = async_call(fn_)
        self.assertEqual(x[0], 2)
        self.assertEqual(rv, 3)

    def testDoLaterWithSyncPartial(self):
        x = [1]
        def fn(v):
            x[0] = v
        self._worker.do_later(functools.partial(fn, 2))
        async_call(functools.partial(tg.sleep, 0.001))
        self.assertEqual(x[0], 2)

    def testDoLaterWithAsyncPartial(self):
        x = [1]
        async def fn(v):
            await tg.moment
            x[0] = v
        self._worker.do_later(worker.AsyncTask(fn, 2))
        async_call(functools.partial(tg.sleep, 0.001))
        self.assertEqual(x[0], 2)


class AsyncMock(um.Mock):

    def __init__(self, return_value=None):
        super(AsyncMock, self).__init__(return_value=self)

        self._return_value = return_value
        self._awaited = False

    def __await__(self):
        yield tg.moment
        self._awaited = True
        return self._return_value

    def assert_awaited(self):
        assert self._awaited


def async_call(fn, *args, **kwargs):
    return ti.IOLoop.instance().run_sync(functools.partial(fn, *args, **kwargs))
