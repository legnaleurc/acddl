import contextlib
import functools
import inspect
import queue
import threading

from tornado import gen as tg, ioloop as ti, queues as tq, locks as tl

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

        self._thread = None
        self._ready_lock = threading.Condition()
        self._loop = None
        self._queue = tq.PriorityQueue()
        self._task_lock = tl.Lock()
        self._done = {}

    @property
    def is_alive(self):
        return self._thread and self._thread.is_alive()

    def start(self):
        if not self.is_alive:
            self._thread = threading.Thread(target=self._run)
            self._thread.start()
        with self._ready_lock:
            if self._loop is None:
                if not self._ready_lock.wait_for(lambda: self._loop is not None, 1):
                    raise Exception('timeout')

    def stop(self):
        if self._loop is not None:
            self._loop.add_callback(self._loop.stop)
        if self.is_alive:
            self._thread.join()
            self._thread = None

    async def do(self, task):
        def _(callback):
            self._done[id(task)] = callback

        task = self._ensure_task(task)
        await self._queue.put(task)
        rv = await tg.Task(_)
        return rv

    def do_later(self, task):
        self._loop.add_callback(self.do, task)

    def _ensure_task(self, maybe_task):
        if not isinstance(maybe_task, Task):
            maybe_task = Task(maybe_task)
        return maybe_task

    def _run(self):
        with self._ready_lock:
            self._loop = ti.IOLoop()
            self._loop.add_callback(self._process)
            self._ready_lock.notify()
        self._loop.start()
        self._loop.close()
        self._loop = None

    async def _process(self):
        while True:
            await self._task_lock.acquire()
            task = await self._queue.get()
            try:
                rv = task()
                if inspect.isawaitable(rv):
                    rv = await rv
            except FlushTasks as e:
                rv = None
                queue = filter(e, self._queue._queue)
                self._queue._queue = list(queue)
            except Exception as e:
                rv = None
                EXCEPTION('acddl') << str(e)
            finally:
                self._queue.task_done()
                self._task_lock.release()
                done = self._done.get(id(task), None)
                if done:
                    del self._done[id(task)]
                    done(rv)


@functools.total_ordering
class Task(object):

    def __init__(self, callable_=None):
        super(Task, self).__init__()

        self._callable = callable_

    def __eq__(self, that):
        return self.priority == that.priority and id(self) == id(that)

    def __gt__(self, that):
        if self.priority < that.priority:
            return True
        if self.priority > that.priority:
            return False
        return id(self) < id(that)

    def __call__(self):
        if not self._callable:
            raise NotImplementedError()
        return self._callable()

    # highest first
    @property
    def priority(self):
        return 0


class FlushTasks(Exception):

    def __init__(self, filter_):
        super(FlushTasks, self).__init__()

        self._filter = filter_

    def __call__(self, task):
        return self._filter(task)
