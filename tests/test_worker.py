import unittest

from tornado import ioloop as ti

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

    def testDo1(self):
        x = [1]
        async def fn():
            x[0] = 2
            return 3
        async def fn_():
            return await self._worker.do(fn)
        rv = async_call(fn_)
        self.assertEqual(x[0], 2)
        self.assertEqual(rv, 3)

    def testDo2(self):
        x = [1]
        def fn():
            x[0] = 2
            return 3
        async def fn_():
            return await self._worker.do(fn)
        rv = async_call(fn_)
        self.assertEqual(x[0], 2)
        self.assertEqual(rv, 3)


def async_call(fn):
    return ti.IOLoop.instance().run_sync(fn)
