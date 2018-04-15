import asyncio
import collections
import datetime as dt
import json
import logging
import math
import threading
import time


class InvalidPatternError(Exception):

    def __init__(self, message):
        self._message = message

    def __str__(self):
        return self._message


class SearchFailedError(Exception):

    def __init__(self, message):
        self._message = message

    def __str__(self):
        return self._message


class LogQueue(logging.Handler):

    def __init__(self, level=logging.NOTSET):
        super(LogQueue, self).__init__(level)

        self._queue = collections.deque(maxlen=10)
        self._sockets = {}
        # log may happens in other threads, while the sockets may be removed in
        # the main thread
        self._socket_lock = threading.Lock()
        self._loop = asyncio.get_event_loop()

    def emit(self, record):
        log = {
            'level': record.levelno,
            'timestamp': math.floor(record.created * 1000),
            'thread': record.threadName,
            'message': record.message,
        }
        self._loop.create_task(self._push(log))

    def get_recent(self):
        return list(self._queue)

    def add(self, id_, ws):
        with self._socket_lock:
            if id_ in self._sockets:
                return False
            self._sockets[id_] = ws
            return True

    def remove(self, id_):
        with self._socket_lock:
            if id_ not in self._sockets:
                return False
            del self._sockets[id_]
            return True

    async def _push(self, log):
        self._queue.append(log)
        log = json.dumps(log)
        with self._socket_lock:
            for id_, ws in self._sockets.items():
                # TODO catch exceptions
                await ws.send_str(log)


def get_local_timezone():
    offset = time.timezone if time.daylight == 0 else time.altzone
    offset = dt.timedelta(seconds=offset)
    return dt.timezone(offset)
