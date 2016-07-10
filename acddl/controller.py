import contextlib
import datetime as dt
import functools
import hashlib
import os
import os.path as op
import pathlib
import queue
import re
import shutil
import subprocess as sp
import sys
import threading

from acdcli.api import client as ACD
from acdcli.api.common import RequestError
from acdcli.cache import db as DB
from acdcli.utils import hashing
from acdcli.utils.time import datetime_to_timestamp
from tornado import ioloop as ti, gen as tg

from .log import ERROR, WARNING, INFO, EXCEPTION
from . import worker


class Context(object):

    def __init__(self, root_path):
        self._root = pathlib.Path(root_path)
        self._auth_path = op.expanduser('~/.cache/acd_cli')
        self._dl = DownloadController(self)
        self._db = ACDDBController(self)
        self._client = ACDClientController(self)

    def close(self):
        for ctrl in (self._dl, self._db, self._client):
            ctrl.close()

    @property
    def root(self):
        return self._root

    @property
    def auth_path(self):
        return self._auth_path

    @property
    def dl(self):
        return self._dl

    @property
    def db(self):
        return self._db

    @property
    def client(self):
        return self._client


class RootController(object):

    def __init__(self, cache_folder):
        self._context = Context(cache_folder)

    def close(self, signum, frame):
        self._context.close()
        main_loop = ioloop.IOLoop.instance()
        main_loop.stop()

    async def search(self, pattern):
        real_pattern = re.sub(r'(\s|-)+', '.*', pattern)
        real_pattern = '.*{0}.*'.format(real_pattern)
        nodes = await self._context.db.find_by_regex(real_pattern)
        nodes = {_.id: self._context.db.get_path(_) for _ in nodes if _.is_available}
        nodes = await tg.multi(nodes)
        return nodes

    async def download(self, node_id):
        node = await self._context.db.get_node(node_id)
        self._context.client.download_later(node)

    def update_cache_from(self, remote_paths):
        self._context.client.multiple_download_later(*remote_paths)

    def compare(self, node_ids):
        nodes = [self._common_context.get_node(_) for _ in node_ids]
        unique = set(_.md5 for _ in nodes)
        if len(unique) == 1:
            return True
        else:
            return [_.size for _ in nodes]


class DownloadController(object):

    def __init__(self, context):
        self._context = context
        self._worker = worker.AsyncWorker()
        self._last_recycle = 0

    def close(self):
        self._worker.stop()

    def download_later(self, node):
        self._ensure_alive()
        task = self._make_download_task(node, need_mtime=True)
        self._worker.do_later(task)

    def multiple_download_later(self, *remote_paths):
        self._ensure_alive()
        task = functools.partial(self._download_from, *remote_paths)
        self._worker.do_later(task)

    def _ensure_alive(self):
        self._worker.start()

    async def _download_from(self, *remote_paths):
        await self._context.db.sync()
        children = await self._get_unified_children(remote_paths)
        mtime = self._get_oldest_mtime()
        children = list(filter(lambda _: _.modified > mtime, children))
        for child in children:
            task = self._make_download_task(child, need_mtime=False)
            self._worker.do_later(task)

    def _make_download_task(self, node, need_mtime):
        return DownloadTask(self._download, node, self._context.root_folder, need_mtime)

    async def _get_unified_children(self, remote_paths):
        children = []
        for remote_path in remote_paths:
            folder = await self._context.db.resolve_path(remote_path)
            tmp = await self._context.db.get_children(folder)
            children.extend(tmp)
        children = sorted(children, key=lambda _: _.modified, reverse=True)
        return children

    def _get_oldest_mtime(self):
        entries = self._get_cache_entries()
        if not entries:
            return dt.datetime.fromtimestamp(0)
        full_path, mtime = entries[0]
        # just convert from local TZ, no need to use UTC
        return dt.datetime.fromtimestamp(mtime)

    def _get_cache_entries(self):
        # get first level children
        entries = self._context.root.iterdir()
        # generate (path, mtime) pair
        entries = ((_, _.stat().st_mtime) for _ in entries)
        entries = sorted(entries, key=lambda _: _[1])
        return entries

    def _is_too_old(self, node):
        mtime = datetime_to_timestamp(node.modified)
        return mtime <= self._last_recycle

    async def _reserve_space(self, node):
        entries = None
        while await self._need_recycle(node):
            if not entries:
                entries = self.common.get_cache_entries()
            full_path, mtime = entries.pop(0)
            if op.isdir(full_path):
                shutil.rmtree(full_path)
            else:
                full_path.unlink()
            self._last_recycle = mtime
            INFO('acddl') << 'recycled:' << full_path

    async def _need_recycle(self, node):
        free_space = self._get_free_space()
        required_space = await self._get_node_size(node)
        gb_free_space = free_space / 1024 / 1024 / 1024
        gb_required_space = required_space / 1024 / 1024 / 1024
        INFO('acddl') << 'free space: {0} GB, required: {1} GB'.format(gb_free_space, gb_required_space)
        return free_space <= required_space

    # in bytes
    def _get_free_space(self):
        s = os.statvfs(str(self._context.root))
        s = s.f_frsize * s.f_bavail
        return s

    # in bytes
    async def _get_node_size(self, node):
        if not node.is_available:
            return 0

        if not node.is_folder:
            return node.size

        children = await self._context.db.get_children(node)
        children = (self._get_node_size(_) for _ in children)
        children = await tg.multi(children)
        return sum(children)

    async def _download(self, node, local_path, need_mtime):
        local_path = local_path if local_path else pathlib.Path()
        full_path = local_path / node.name

        if not node.is_available:
            return False

        if need_mtime and self._is_too_old(node):
            return False

        if node.is_folder:
            ok = await self._download_folder(node, full_path, need_mtime)
        else:
            ok = await self._download_file(node, local_path, full_path)

        if ok:
            if need_mtime:
                ok = preserve_mtime_by_node(full_path, node)
            else:
                ok = update_mtime(full_path, dt.datetime.now().timestamp())

        return ok

    async def _download_folder(self, node, full_path, need_mtime):
        try:
            full_path.mkdir(parents=True, exist_ok=True)
        except OSError:
            WARNING('acddl') << 'mkdir failed:' << full_path
            return False

        children = await self._context.db.get_children(node)
        for child in children:
            ok = await self._download(child, full_path, need_mtime)
            if not ok:
                return False

        return True

    async def _download_file(self, node, local_path, full_path):
        if full_path.is_file():
            INFO('acddl') << 'checking existed:' << full_path
            local = md5sum(full_path)
            remote = node.md5
            if local == remote:
                INFO('acddl') << 'skip same file'
                return True
            INFO('acddl') << 'md5 mismatch'
            full_path.unlink()

        await self._reserve_space(node)

        # retry until succeed
        while True:
            try:
                remote_path = await self._context.db.get_path(node)
                INFO('acddl') << 'downloading:' << remote_path
                local_hash = await self._context.client.download_node(node, local_path)
                INFO('acddl') << 'downloaded'
            except RequestError as e:
                ERROR('acddl') << 'download failed:' << str(e)
            except OSError as e:
                if e.errno == 36:
                    WARNING('acddl') << 'download failed: file name too long'
                    return False
                # fatal unknown error
                raise
            else:
                remote_hash = node.md5
                if local_hash != remote_hash:
                    INFO('acddl') << 'md5 mismatch:' << full_path
                    full_path.unlink()
                else:
                    break

        return True


class DownloadTask(worker.Task):

    def __init__(self, callable_, node, local_path, need_mtime):
        super(DownloadTask, self).__init__(functools.partial(callable_, node, local_path, need_mtime))

        self._node = node
        self._need_mtime = need_mtime
        # higher will do first
        self._priority = 0 if self._need_mtime else 1

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


class ACDClientController(object):

    def __init__(self, context):
        self._context = context
        self._worker = worker.AsyncWorker()
        self._acd_client = None

    def close(self):
        self._worker.stop()
        self._acd_client = None

    async def download_node(self, node, local_path):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._download, node, local_path))

    async def get_changes(self, checkpoint, include_purged):
        return await self._worker.do(functools.partial(self._acd_client.get_changes, checkpoint=checkpoint, include_purged=include_purged, silent=False, file=None))

    def iter_changes_lines(self, changes):
        return self._acd_client._iter_changes_lines(changes)

    async def _ensure_alive(self):
        if not self._acd_client:
            self._worker.start()
            await self._worker.do(self._create_client)

    def _create_client(self):
        self._acd_client = ACD.ACDClient(self._context.auth_path)

    def _download(self, node, local_path):
        hasher = hashing.IncrementalHasher()
        self._acd_client.download_file(node.id, node.name, str(local_path), write_callbacks=[
            hasher.update,
        ])
        return hasher.get_result()


class ACDDBController(object):

    _CHECKPOINT_KEY = 'checkpoint'
    _LAST_SYNC_KEY = 'last_sync'
    _MAX_AGE = 30

    def __init__(self, context):
        self._context = context
        self._worker = worker.AsyncWorker()
        self._acd_db = None

    def close(self):
        self._worker.stop()
        self._acd_db = None

    async def sync(self):
        await self._ensure_alive()

        # copied from acd_cli

        check_point = self._acd_db.KeyValueStorage.get(self._CHECKPOINT_KEY)

        f = await self._context.client.get_changes(checkpoint=check_point, include_purged=bool(check_point))

        try:
            full = False
            first = True

            for changeset in self._context.client.iter_changes_lines(f):
                if changeset.reset or (full and first):
                    await self._worker.do(self._acd_db.drop_all)
                    await self._worker.do(self._acd_db.init)
                    full = True
                else:
                    await self._worker.do(functools.partial(self._acd_db.remove_purged, changeset.purged_nodes))

                if changeset.nodes:
                    await self._worker.do(functools.partial(self._acd_db.insert_nodes, changeset.nodes, partial=not full))
                self._acd_db.KeyValueStorage.update({
                    self._LAST_SYNC_KEY: time.time(),
                })

                if changeset.nodes or changeset.purged_nodes:
                    self._acd_db.KeyValueStorage.update({
                        self._CHECKPOINT_KEY: changeset.checkpoint,
                    })

                first = False
        except RequestError as e:
            EXCEPTION('acddl') << str(e)
            return False

        return True

    async def resolve_path(self, remote_path):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.resolve, node))

    async def get_children(self, node):
        await self._ensure_alive()
        folders, files = await self._worker.do(functools.partial(self._acd_db.list_children, node.id))
        children = folders + files
        return children

    async def get_path(self, node):
        await self._ensure_alive()
        dirname = await self._worker.do(functools.partial(self._acd_db.first_path, node.id))
        return dirname + node.name

    async def get_node(self, node_id):
        return await self._worker.do(functools.partial(self._acd_db.get_node, node_id))

    async def find_by_regex(self, pattern):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.find_by_regex, pattern))

    async def _ensure_alive(self):
        if not self._acd_db:
            self._worker.start()
            await self._worker.do(self._create_db)

    def _create_db(self):
        self._acd_db = DB.NodeCache(self._context.auth_path)


'''


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

        if need_mtime and self._context.is_too_old(node):
            return False

        if node.is_folder:
            ok = self._download_folder(node, full_path, need_mtime)
        else:
            ok = self._download_file(node, local_path, full_path)

        if ok:
            if need_mtime:
                ok = preserve_mtime_by_node(full_path, node)
            else:
                ok = update_mtime(full_path, dt.datetime.now().timestamp())

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
                INFO('acddl') << 'skip same file'
                return True
            INFO('acddl') << 'md5 mismatch'
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
                    return False
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

    # download/main thread
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

    # main thread
    def find_by_regex(self, pattern):
        return self._acd_db.find_by_regex(pattern)


# used by download/main thread
class DownloadContext(object):

    def __init__(self, common_context):
        self._common_context = common_context
        auth_folder = self._common_context.auth_folder
        self._acd_client = ACD.ACDClient(auth_folder)
        self._queue = queue.PriorityQueue()
        self._thread = None
        self._thread_lock = threading.RLock()
        self._last_recycle = 0

    # thread safe
    @property
    def common(self):
        return self._common_context

    # main thread
    def end_queue(self):
        td = DownloadTaskDescriptor.create_stop()
        self._queue.put(td)

        with self._thread_lock:
            if self._thread:
                self._thread.join()

    # main/update thread
    def push_queue(self, dtd):
        self._queue.put(dtd)

        with self._thread_lock:
            if not self._thread:
                self._thread = DownloadThread(self)
                self._thread.start()

    # download thread
    @contextlib.contextmanager
    def pop_queue(self):
        try:
            yield self._queue.get()
        finally:
            self._queue.task_done()

    # download thread
    # FIXME breaks many things
    def flush_queue(self):
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
            self._last_recycle = mtime
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
        if not entries:
            return dt.datetime.fromtimestamp(0)
        full_path, mtime = entries[0]
        # just convert from local TZ, no need to use UTC
        return dt.datetime.fromtimestamp(mtime)

    # update thread
    def flush(self):
        dtd = DownloadTaskDescriptor.create_flush()
        self._download_context.push_queue(dtd)

    # update thread
    def download_later(self, node):
        dtd = DownloadTaskDescriptor.create_need_mtime(node)
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

'''


def md5sum(full_path):
    hasher = hashlib.md5()
    with full_path.open('rb') as fin:
        while True:
            chunk = fin.read(65536)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def preserve_mtime_by_node(full_path, node):
    mtime = datetime_to_timestamp(node.modified)
    return update_mtime(full_path, mtime)


def update_mtime(full_path, s_mtime):
    try:
        os.utime(str(full_path), (s_mtime, s_mtime))
    except OSError as e:
        # file name too long
        if e.errno != 36:
            raise
    return True
