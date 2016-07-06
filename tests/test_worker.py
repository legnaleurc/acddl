import functools
import unittest
from unittest import mock as um

from tornado import ioloop as ti, gen as tg

from acddl import worker
from . import util as u


class TestWorker(unittest.TestCase):

    def setUp(self):
        self._worker = worker.Worker()
        self._worker.start()
        self.assertTrue(self._worker.is_alive())

    def tearDown(self):
        u.async_call(self._worker.stop)
        self.assertFalse(self._worker.is_alive())

    def testDo(self):
        x = [1]
        def fn():
            x[0] = 2
        async def fn_():
            await self._worker.do(fn)
        u.async_call(fn_)
        self.assertEqual(x[0], 2)


class TestAsyncWorker(unittest.TestCase):

    def setUp(self):
        self._worker = worker.AsyncWorker()
        self._worker.start()
        self.assertTrue(self._worker.is_alive)

    def tearDown(self):
        u.async_call(self._worker.stop)
        self.assertFalse(self._worker.is_alive)

    @um.patch('tornado.ioloop.IOLoop', autospec=True)
    @um.patch('threading.Condition', autospec=True)
    @um.patch('threading.Thread', autospec=True)
    def testStartTwice(self, FakeThread, FakeCondition, FakeIOLoop):
        w = self._createTemporaryWorker()
        w.start()

        the_loop = w._loop
        w.start()
        w._thread.start.assert_called_once_with()
        self.assertEqual(the_loop, w._loop)

    @um.patch('tornado.ioloop.IOLoop', autospec=True)
    @um.patch('threading.Condition', autospec=True)
    @um.patch('threading.Thread', autospec=True)
    def testStopTwice(self, FakeThread, FakeCondition, FakeIOLoop):
        w = self._createTemporaryWorker()
        w.start()
        w.stop()

        w.stop()

    def testDoWithSync(self):
        fn = self._createSyncMock()
        rv = u.async_call(self._worker.do, fn)
        fn.assert_called_once_with()
        self.assertEqual(rv, 42)

    def testDoWithAsync(self):
        fn = self._createAsyncMock()
        rv = u.async_call(self._worker.do, fn)
        fn.assert_called_once_with()
        fn.assert_awaited()
        self.assertEqual(rv, 42)

    def testDoLaterWithSync(self):
        fn = self._createSyncMock()
        self._worker.do_later(fn)
        u.async_call(functools.partial(tg.sleep, 0.001))
        fn.assert_called_once_with()

    def testDoLaterWithAsync(self):
        fn = self._createAsyncMock()
        self._worker.do_later(fn)
        u.async_call(functools.partial(tg.sleep, 0.001))
        fn.assert_called_once_with()

    def testDoWithSyncPartial(self):
        fn = self._createSyncMock()
        rv = u.async_call(self._worker.do, functools.partial(fn, 1, k=7))
        fn.assert_called_once_with(1, k=7)
        self.assertEqual(rv, 42)

    def testDoWithAsyncPartial(self):
        fn = self._createAsyncMock()
        rv = u.async_call(self._worker.do, functools.partial(fn, 1, k=7))
        fn.assert_called_once_with(1, k=7)
        fn.assert_awaited()
        self.assertEqual(rv, 42)

    def testDoLaterWithSyncPartial(self):
        fn = self._createSyncMock()
        self._worker.do_later(functools.partial(fn, 1, k=7))
        u.async_call(functools.partial(tg.sleep, 0.001))
        fn.assert_called_once_with(1, k=7)

    def testDoLaterWithAsyncPartial(self):
        fn = self._createAsyncMock()
        self._worker.do_later(functools.partial(fn, 1, k=7))
        u.async_call(functools.partial(tg.sleep, 0.001))
        fn.assert_called_once_with(1, k=7)
        fn.assert_awaited()

    def _createTemporaryWorker(self):
        w = worker.AsyncWorker()
        w._thread.is_alive.return_value = False
        def thread_start_side_effect():
            w._run()
            w._thread.is_alive.return_value = True
        w._thread.start.side_effect = thread_start_side_effect
        return w

    def _createSyncMock(self):
        return um.Mock(return_value=42)

    def _createAsyncMock(self):
        return u.AsyncMock(return_value=42)
