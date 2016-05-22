import contextlib
import datetime as dt
import hashlib
import os
import os.path as op
import queue
import shutil
import subprocess as sp
import sys
import threading

from acdcli.api import client as ACD
from acdcli.api.common import RequestError
from acdcli.cache import db as DB
from acdcli.utils import hashing
from acdcli.utils.time import datetime_to_timestamp
from tornado import ioloop

from .log import ERROR, WARNING, INFO, EXCEPTION


class Controller(object):

    def __init__(self, cache_folder):
        self._common_context = CommonContext(cache_folder)
        self._download_context = DownloadContext(self._common_context)
        self._update_context = UpdateContext(self._common_context, self._download_context)

    def stop(self, signum, frame):
        self._update_context.end_queue()
        self._download_context.end_queue()
        main_loop = ioloop.IOLoop.instance()
        main_loop.stop()

    def download(self, node_id):
        node = self._common_context.get_node(node_id)
        if node:
            dtd = DownloadTaskDescriptor.create_no_mtime(node)
            self._download_context.push_queue(dtd)

    def update_cache_from(self, acd_paths):
        self._update_context.push_queue(acd_paths)


class DownloadThread(threading.Thread):

    def __init__(self, context):
        super(DownloadThread, self).__init__()

        self._context = context

    # Override
    def run(self):
        try:
            while True:
                with self._context.pop_queue() as dtd:
                    if dtd.stop:
                        # special value, need stop
                        break
                    if dtd.flush:
                        self._context.flush_queue()
                        continue
                    self._download(dtd.node, self._context.common.root_folder, dtd.need_mtime)
        except Exception as e:
            EXCEPTION('acddl') << str(e)

    def _download(self, node, local_path, need_mtime):
        local_path = local_path if local_path else ''
        full_path = op.join(local_path, node.name)

        if not node.is_available:
            return False

        if node.is_folder:
            ok = self._download_folder(node, full_path, need_mtime)
        else:
            ok = self._download_file(node, local_path, full_path)

        if ok and need_mtime:
            ok = preserve_mtime(node, full_path)

        return ok

    def _download_folder(self, node, full_path, need_mtime):
        try:
            os.makedirs(full_path, exist_ok=True)
        except OSError:
            WARNING('acddl') << 'mkdir failed:' << full_path
            return False

        children = self._context.common.get_children(node)
        for child in children:
            ok = self._download(child, full_path, need_mtime)
            if not ok:
                return False

        return True

    def _download_file(self, node, local_path, full_path):
        if op.isfile(full_path):
            INFO('acddl') << 'checking existed:' << full_path
            local = md5sum(full_path)
            remote = node.md5
            if local == remote:
                INFO('acddl') << 'skip same file:' << full_path
                return True
            INFO('acddl') << 'md5 mismatch:' << full_path
            os.remove(full_path)

        self._context.reserve_space(node)

        # retry until succeed
        while True:
            try:
                remote_path = self._context.common.get_path(node)
                INFO('acddl') << 'downloading:' << remote_path
                local_hash = self._context.download_node(node, local_path)
                INFO('acddl') << 'downloaded'
            except RequestError as e:
                ERROR('acddl') << 'download failed:' << str(e)
            except OSError as e:
                if e.errno == 36:
                    WARNING('acddl') << 'download failed: file name too long'
                    return True
                # fatal unknown error
                raise
            else:
                remote_hash = node.md5
                if local_hash != remote_hash:
                    INFO('acddl') << 'md5 mismatch:' << full_path
                    os.remove(full_path)
                else:
                    break

        return True


class UpdateThread(threading.Thread):

    def __init__(self, context):
        super(UpdateThread, self).__init__()

        self._context = context

    # Override
    def run(self):
        try:
            while True:
                with self._context.pop_queue() as acd_paths:
                    if acd_paths is None:
                        # special value, need stop
                        break
                    self._sync()
                    children = self._context.get_unified_children(acd_paths)
                    mtime = self._context.get_oldest_mtime()
                    children = filter(lambda _: _.modified > mtime, children)
                    # flush previous update queue
                    self._context.flush()
                    for child in children:
                        self._context.download_later(child)
        except Exception as e:
            EXCEPTION('acddl') << str(e)

    def _sync(self):
        INFO('acddl') << 'syncing'
        sp.run(['acdcli', 'sync'], stdout=sp.DEVNULL, stderr=sp.DEVNULL)
        INFO('acddl') << 'synced'


# used by all threads
class CommonContext(object):

    def __init__(self, cache_folder):
        self._cache_folder = cache_folder
        self._auth_folder = op.expanduser('~/.cache/acd_cli')
        self._acd_db = DB.NodeCache(self._auth_folder)

    # thread safe
    @property
    def root_folder(self):
        return self._cache_folder

    # thread safe
    @property
    def auth_folder(self):
        return self._auth_folder

    # main thread
    def get_node(self, node_id):
        return self._acd_db.get_node(node_id)

    # download thread
    def get_path(self, node):
        return self._acd_db.first_path(node.id) + node.name

    # update thread
    def resolve_path(self, acd_path):
        return self._acd_db.resolve(acd_path)

    # all threads
    def get_children(self, node):
        folders, files = self._acd_db.list_children(node.id)
        children = folders + files
        return children

    # all threads
    def get_cache_entries(self):
        entries = os.listdir(self._cache_folder)
        entries = (op.join(self._cache_folder, _) for _ in entries)
        entries = ((_, op.getmtime(_)) for _ in entries)
        entries = sorted(entries, key=lambda _: _[1])
        return entries


# used by download/main thread
class DownloadContext(object):

    def __init__(self, common_context):
        self._common_context = common_context
        auth_folder = self._common_context.auth_folder
        self._acd_client = ACD.ACDClient(auth_folder)
        self._queue = queue.PriorityQueue()
        self._thread = None
        self._queue_lock = threading.RLock()
        self._thread_lock = threading.RLock()

    # thread safe
    @property
    def common(self):
        return self._common_context

    # main thread
    def end_queue(self):
        td = DownloadTaskDescriptor.create_stop()
        with self._queue_lock:
            self._queue.put(td)

        with self._thread_lock:
            if self._thread:
                self._thread.join()

    # main/update thread
    def push_queue(self, dtd):
        with self._queue_lock:
            self._queue.put(dtd)

        with self._thread_lock:
            if not self._thread:
                self._thread = DownloadThread(self)
                self._thread.start()

    # download thread
    @contextlib.contextmanager
    def pop_queue(self):
        with self._queue_lock:
            try:
                yield self._queue.get()
            finally:
                self._queue.task_done()

    # download thread
    def flush_queue(self):
        with self._queue_lock:
            new_queue = queue.PriorityQueue()
            while not self._queue.empty():
                with self.pop_queue() as dtd:
                    if dtd.stop or dtd.flush or not dtd.need_mtime:
                        new_queue.put(dtd)
            self._queue = new_queue

    # download thread
    def download_node(self, node, local_path):
        hasher = hashing.IncrementalHasher()
        self._acd_client.download_file(node.id, node.name, local_path, write_callbacks=[
            hasher.update,
        ])
        return hasher.get_result()

    # download thread
    def reserve_space(self, node):
        entries = None
        while self._need_recycle(node):
            if not entries:
                entries = self.common.get_cache_entries()
            full_path, mtime = entries.pop(0)
            if op.isdir(full_path):
                shutil.rmtree(full_path)
            else:
                os.remove(full_path)
            INFO('acddl') << 'recycled:' << full_path

    # download thread
    def _need_recycle(self, node):
        free_space = self._get_free_space()
        required_space = self._get_node_size(node)
        gb_free_space = free_space / 1024 / 1024 / 1024
        gb_required_space = required_space / 1024 / 1024 / 1024
        INFO('acddl') << 'free space: {0} GB, required: {1} GB'.format(gb_free_space, gb_required_space)
        return free_space <= required_space

    # download thread
    # in bytes
    def _get_free_space(self):
        s = os.statvfs(self.common.root_folder)
        s = s.f_frsize * s.f_bavail
        return s

    # download thread
    # in bytes
    def _get_node_size(self, node):
        if not node.is_available:
            return 0

        if not node.is_folder:
            return node.size

        children = self.common.get_children(node)
        children = (self._get_node_size(_) for _ in children)
        return sum(children)


# used by update/main thread
class UpdateContext(object):

    def __init__(self, common_context, download_context):
        self._common_context = common_context
        self._download_context = download_context
        self._queue = queue.Queue()
        self._thread = None

    @property
    def common(self):
        return self._common_context

    # main thread
    def end_queue(self):
        self._queue.put(None)
        if self._thread:
            self._thread.join()

    # main thread
    def push_queue(self, acd_paths):
        self._queue.put(acd_paths)

        if not self._thread:
            self._thread = UpdateThread(self)
            self._thread.start()

    # update thread
    @contextlib.contextmanager
    def pop_queue(self):
        try:
            yield self._queue.get()
        finally:
            self._queue.task_done()

    # update thread
    def get_unified_children(self, acd_paths):
        children = []
        for acd_path in acd_paths:
            folder = self.common.resolve_path(acd_path)
            tmp = self.common.get_children(folder)
            children.extend(tmp)
        children = sorted(children, key=lambda _: _.modified, reverse=True)
        return children

    # update thread
    def get_oldest_mtime(self):
        entries = self.common.get_cache_entries()
        full_path, mtime = entries[0]
        # just convert from local TZ, no need to use UTC
        return dt.datetime.fromtimestamp(mtime)

    # update thread
    def flush(self):
        dtd = DownloadTaskDescriptor.create_flush()
        self._download_context.push_queue(dtd)

    # update thread
    def download_later(self, node):
        dtd = DownloadTaskDescriptor.create_mtime(node)
        self._download_context.push_queue(dtd)


class DownloadTaskDescriptor(object):

    @staticmethod
    def create_stop():
        return DownloadTaskDescriptor(sys.maxsize, None, None, True, False)

    @staticmethod
    def create_flush():
        return DownloadTaskDescriptor(sys.maxsize - 1, None, None, False, True)

    @staticmethod
    def create_need_mtime(node):
        return DownloadTaskDescriptor(0, node, True, False, False)

    @staticmethod
    def create_no_mtime(node):
        return DownloadTaskDescriptor(1, node, False, False, False)

    def __init__(self, priority, node, need_mtime, stop, flush):
        self._priority = priority
        self._node = node
        self._need_mtime = need_mtime
        self._stop = stop
        self._flush = flush

    def __lt__(self, that):
        if self._priority < that._priority:
            return False
        if self._priority == that._priority:
            if not self._node or not that._node:
                return False
            return self._node.modified > that._node.modified
        return self._priority > that._priority

    def __eq__(self, that):
        if self._priority != that._priority:
            return False
        if self._node and that._node:
            return self._node.modified == that._node.modified
        return self._node == that._node

    @property
    def stop(self):
        return self._stop

    @property
    def flush(self):
        return self._flush

    @property
    def node(self):
        return self._node

    @property
    def need_mtime(self):
        return self._need_mtime


def md5sum(full_path):
    hasher = hashlib.md5()
    with open(full_path, 'rb') as fin:
        while True:
            chunk = fin.read(65536)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def preserve_mtime(node, full_path):
    mtime = datetime_to_timestamp(node.modified)
    try:
        os.utime(full_path, (mtime, mtime))
    except OSError as e:
        # file name too long
        if e.errno != 36:
            raise
    return True
