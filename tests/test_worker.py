import unittest
import functools

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

    def testDoWithSync(self):
        x = [1]
        def fn():
            x[0] = 2
            return 3
        async def fn_():
            return await self._worker.do(fn)
        rv = async_call(fn_)
        self.assertEqual(x[0], 2)
        self.assertEqual(rv, 3)

    def testDoWithAsync(self):
        x = [1]
        async def fn():
            await tg.moment
            x[0] = 2
            return 3
        async def fn_():
            return await self._worker.do(fn)
        rv = async_call(fn_)
        self.assertEqual(x[0], 2)
        self.assertEqual(rv, 3)

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


def async_call(fn):
    return ti.IOLoop.instance().run_sync(fn)
