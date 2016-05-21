import contextlib
import hashlib
import os
import os.path as op
import queue
import threading

from acdcli.api import client as ACD
from acdcli.cache import db as DB
from acdcli.utils import hashing
from acdcli.utils.time import datetime_to_timestamp
from tornado import ioloop

from .log import ERROR, WARNING, INFO


class Controller(object):

    def __init__(self, cache_folder):
        self._context = Context(cache_folder)
        self._download_thread = None

    def stop(self, signum, frame):
        self._context.end_queue()
        if self._download_thread:
            self._download_thread.join()
        main_loop = ioloop.IOLoop.instance()
        main_loop.stop()

    def download(self, node_id, priority, need_mtime):
        node = self._context.get_node(node_id)
        if not node:
            return

        td = TaskDescriptor(priority, node, need_mtime)
        self._context.push_queue(td)

        if not self._download_thread:
            self._download_thread = DownloadThread(self._context)
            self._download_thread.start()


class DownloadThread(threading.Thread):

    def __init__(self, context):
        super(DownloadThread, self).__init__()

        self._context = context

    # Override
    def run(self):
        while True:
            with self._context.pop_queue() as td:
                if td is None:
                    # special value, need stop
                    break
                self._download(td.node, self._context.root_folder, td.need_mtime)

    def _download(self, node, local_path, need_mtime):
        local_path = local_path if local_path else ''
        full_path = op.join(local_path, node.name)

        if not node.is_available:
            return False

        if node.is_folder:
            ok = self._download_folder(node, full_path, need_mtime)
        else:
            ok = self._download_file(node, local_path, full_path)

        if need_mtime:
            ok = preserve_mtime(node, full_path)

        return ok

    def _download_folder(self, node, full_path, need_mtime):
        try:
            os.makedirs(full_path, exist_ok=True)
        except OSError:
            WARNING('acddl') << 'mkdir failed:' << full_path
            return False

        children = self._context.get_children(node)
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
                INFO('acddl') << 'downloading:' << full_path
                local_hash = self._context.download_node(node, local_path)
                INFO('acddl') << 'downloaded:' << full_path
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


# used by both threads
class Context(object):

    def __init__(self, cache_folder):
        self._cache_folder = cache_folder
        auth_folder = op.expanduser('~/.cache/acd_cli')
        self._acd_client = ACD.ACDClient(auth_folder)
        self._acd_db = DB.NodeCache(auth_folder)
        self._download_queue = queue.PriorityQueue()

    # thread safe
    @property
    def root_folder(self):
        return self._cache_folder

    # main thread
    def end_queue(self):
        self._download_queue.put(None)

    # main thread
    def push_queue(self, td):
        self._download_queue.put(td)

    # worker thread
    @contextlib.contextmanager
    def pop_queue(self):
        try:
            yield self._download_queue.get()
        finally:
            self._download_queue.task_done()

    # main thread
    def get_node(self, node_id):
        return self._acd_db.get_node(node_id)

    # worker thread
    def get_children(self, node):
        folders, files = self._acd_db.list_children(node.id)
        children = folders + files
        return children

    # worker thread
    def download_node(self, node, local_path):
        hasher = hashing.IncrementalHasher()
        self._acd_client.download_file(node.id, node.name, local_path, write_callbacks=[
            hasher.update,
        ])
        return hasher.get_result()

    # worker thread
    def reserve_space(self, node):
        entries = self._get_cache_entries()

        while True:
            free_space = self._get_free_space()
            required_space = self._get_node_size(node)
            gb_free_space = free_space / 1024 / 1024 / 1024
            gb_required_space = required_space / 1024 / 1024 / 1024
            INFO('acddl') << 'free space: {0} GB, required: {1} GB'.format(gb_free_space, gb_required_space)
            if free_space > required_space:
                break

            full_path, mtime = entries.pop(0)
            if op.isdir(full_path):
                shutil.rmtree(full_path)
            else:
                os.remove(full_path)
            INFO('acddl') << 'recycled:' << full_path

    # worker thread
    def _get_cache_entries(self):
        entries = os.listdir(self._cache_folder)
        entries = (op.join(self._cache_folder, _) for _ in entries)
        entries = ((_, os.stat(_).st_mtime) for _ in entries)
        entries = sorted(entries, key=lambda _: _[1])
        return entries

    # worker thread
    # in bytes
    def _get_free_space(self):
        s = os.statvfs(self._cache_folder)
        s = s.f_frsize * s.f_bavail
        return s

    # worker thread
    # in bytes
    def _get_node_size(self, node):
        if not node.is_available:
            return 0

        if not node.is_folder:
            return node.size

        children = self.get_children(node)
        children = (self._get_node_size(_) for _ in children)
        return sum(children)


class TaskDescriptor(object):

    def __init__(self, priority, node, need_mtime):
        self._priority = priority
        self._node = node
        self._need_mtime = need_mtime

    def __lt__(self, that):
        return self._priority > that._priority

    def __eq__(self, that):
        return self._priority == that._priority

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
