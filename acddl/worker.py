import contextlib
import functools
import inspect
import queue
import threading

from tornado import gen as tg, ioloop as ti, queues as tq

from .log import EXCEPTION


class Worker(threading.Thread):

    def __init__(self):
        super(Worker, self).__init__()

        self._queue = TaskQueue()

    # Override
    def run(self):
        while True:
            with self._queue.pop() as (task, done):
                try:
                    rv = task()
                except StopWorker as e:
                    rv = None
                    break
                except Exception as e:
                    rv = None
                    EXCEPTION('acddl') << str(e)
                finally:
                    done(rv)

    async def do(self, callable_):
        return await tg.Task(lambda callback: self._queue.push(callable_, callback))

    async def stop(self):
        def terminate():
            raise StopWorker()
        await tg.Task(lambda callback: self._queue.push(terminate, callback))
        self.join()


class TaskQueue(object):

    def __init__(self):
        self._queue = queue.Queue()

    @contextlib.contextmanager
    def pop(self):
        try:
            yield self._queue.get()
        finally:
            self._queue.task_done()

    def push(self, callable_, done):
        self._queue.put((callable_, done))


class StopWorker(Exception):

    def __init__(self):
        super(StopWorker, self).__init__()


class AsyncWorker(object):

    def __init__(self):
        super(AsyncWorker, self).__init__()

        self._thread = threading.Thread(target=self._run)
        self._loop = None
        self._queue = tq.PriorityQueue()
        self._done = {}

    @property
    def is_alive(self):
        return self._thread.is_alive()

    def start(self):
        if not self.is_alive and self._loop is None:
            self._thread.start()

    def stop(self):
        self._loop.add_callback(self._loop.stop)
        self._thread.join()

    async def do(self, task):
        def _(callback):
            self._done[id(task)] = callback

        await self._queue.put(task)
        rv = await tg.Task(_)
        return rv

    def _run(self):
        assert self._loop is None
        self._loop = ti.IOLoop()
        self._loop.add_callback(self._process)
        self._loop.start()
        self._loop.close()

    async def _process(self):
        while True:
            task = await self._queue.get()
            try:
                if inspect.iscoroutinefunction(task):
                    rv = await task()
                else:
                    rv = task()
            except Exception as e:
                rv = None
                EXCEPTION('acddl') << str(e)
            finally:
                self._queue.task_done()
                done = self._done.get(id(task), None)
                if done:
                    del self._done[id(task)]
                    done(rv)


@functools.total_ordering
class Task(object):

    def __init__(self):
        super(Task, self).__init__()

    def __eq__(self, that):
        return id(self) == id(that)

    def __lt__(self, that):
        return id(self) < id(that)
