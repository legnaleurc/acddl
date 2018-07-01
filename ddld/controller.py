import asyncio
import concurrent.futures as cf
import contextlib as cl
import datetime as dt
import functools as ft
import hashlib
import os
import os.path as op
import pathlib
import re
import shutil
import time

import wcpan.drive.google as wdg
import wcpan.worker as ww
from wcpan.logger import ERROR, WARNING, INFO, EXCEPTION, DEBUG

from . import util as u


class Context(object):

    def __init__(self, root_path):
        self._root = pathlib.Path(root_path)
        self._auth_path = op.expanduser('~/.cache/wcpan/drive/google')
        self._dl = DownloadController(self)
        self._drive = wdg.Drive(self._auth_path)
        self._search_engine = SearchEngine(self._drive)
        self._raii = None

    async def __aenter__(self):
        async with cl.AsyncExitStack() as stack:
            await stack.enter_async_context(self._drive)
            await stack.enter_async_context(self._dl)
            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, type_, value, traceback):
        await self._raii.aclose()
        self._raii = None

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
    def drive(self):
        return self._drive

    @property
    def search_engine(self):
        return self._search_engine


class RootController(object):

    def __init__(self, cache_folder):
        self._context = Context(cache_folder)
        self._loop = asyncio.get_event_loop()
        self._raii = None

    async def __aenter__(self):
        async with cl.AsyncExitStack() as stack:
            await stack.enter_async_context(self._context)
            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, type_, value, traceback):
        await self._raii.aclose()
        self._raii = None

    async def search(self, pattern):
        real_pattern = normalize_search_pattern(pattern)
        try:
            re.compile(real_pattern)
        except Exception as e:
            EXCEPTION('ddld', e) << real_pattern
            raise u.InvalidPatternError(real_pattern)

        se = self._context.search_engine
        nodes = await se.get_nodes_by_regex(real_pattern)
        return nodes

    def download_high(self, node_id):
        self._loop.create_task(self._download_glue(node_id))

    def download_low(self, remote_paths):
        self._context.dl.multiple_download(*remote_paths)

    async def compare(self, node_ids):
        nodes = (self._context.drive.get_node_by_id(_) for _ in node_ids)
        nodes = await asyncio.gather(*nodes)
        unique = set(_.md5 for _ in nodes)
        if len(unique) == 1:
            return None

        paths = (self._context.drive.get_path(_) for _ in nodes)
        paths = await asyncio.gather(*paths)
        return [(node.size, path) for node, path in zip(nodes, paths)]

    def trash(self, node_id):
        self._loop.create_task(self._trash_glue(node_id))

    def sync_db(self):
        self._loop.create_task(self._sync_glue())

    async def _download_glue(self, node_id):
        node = await self._context.drive.get_node_by_id(node_id)
        self._context.dl.download(node)

    async def _sync_glue(self):
        await self._context.search_engine.clear_cache()
        await self._context.drive.sync()

    async def _trash_glue(self, node_id):
        await self._context.drive.trash_node_by_id(node_id)
        path = await self._context.drive.get_path_by_id(node_id)
        self._context.search_engine.drop_value(path)


class DownloadController(object):

    def __init__(self, context):
        self._context = context
        self._queue = ww.AsyncQueue(1)
        self._loop = asyncio.get_event_loop()
        self._last_recycle = 0
        self._pending_size = 0
        self._pool = None

    async def __aenter__(self):
        self._pool = cf.ProcessPoolExecutor()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._pool.shutdown()
        await self._queue.stop()
        self._pool = None

    def download(self, node):
        self._ensure_alive()
        task = self._make_high_download_task(node)
        self._queue.post(task)

    def multiple_download(self, *remote_paths):
        self._ensure_alive()
        self.abort()
        task = ft.partial(self._download_from, *remote_paths)
        self._queue.post(task)

    def abort(self):
        self._queue.flush(lambda _: isinstance(_, LowDownloadTask))

    def _ensure_alive(self):
        self._queue.start()

    async def _download_from(self, *remote_paths):
        await self._context.search_engine.clear_cache()
        await self._context.drive.sync()
        children = await self._get_unified_children(remote_paths)
        for child in children:
            task = self._make_low_download_task(child)
            self._queue.post(task)

    def _make_high_download_task(self, node):
        return HighDownloadTask(self._download, node, self._context.root)

    def _make_low_download_task(self, node):
        return LowDownloadTask(self._download, node, self._context.root)

    async def _get_unified_children(self, remote_paths):
        children = (self._context.drive.get_node_by_path(_)
                    for _ in remote_paths)
        children = await asyncio.gather(*children)
        children = (self._context.drive.get_children(_) for _ in children)
        children = await asyncio.gather(*children)
        children = [_1 for _0 in children for _1 in _0]
        children = sorted(children, key=lambda _: _.modified, reverse=True)
        return children

    def _get_oldest_mtime(self):
        local_timezone = u.get_local_timezone()
        entries = self._get_recyclable_entries()
        if not entries:
            return dt.datetime.fromtimestamp(0, local_timezone)
        full_path, mtime = entries[0]
        # just convert from local TZ, no need to use UTC
        return dt.datetime.fromtimestamp(mtime, local_timezone)

    def _get_recyclable_entries(self):
        # get first level children
        entries = self._context.root.iterdir()
        # filter
        entries = (_ for _ in entries if is_unlinkable(_))
        # generate (path, mtime) pair
        entries = ((_, _.stat().st_mtime) for _ in entries)
        entries = sorted(entries, key=lambda _: _[1])
        return entries

    def _is_too_old(self, node):
        mtime = self._get_oldest_mtime()
        return node.modified <= mtime

    async def _reserve_space(self, node):
        entries = None
        required_space = await self._get_node_size(node)
        while await self._need_recycle(required_space):
            if not entries:
                entries = self._get_recyclable_entries()
            full_path, mtime = entries.pop(0)
            if full_path.is_dir():
                shutil.rmtree(str(full_path))
            else:
                full_path.unlink()
            self._last_recycle = mtime
            INFO('ddld') << 'recycled:' << full_path

    async def _need_recycle(self, required_space):
        free_space = self._get_free_space()
        hfs, fsu = human_readable(free_space)
        hrs, rsu = human_readable(required_space)
        INFO('ddld') << 'free space: {0:.2f} {1}, required: {2:.2f} {3}'.format(hfs, fsu, hrs, rsu)
        return free_space <= required_space

    # in bytes
    def _get_free_space(self):
        s = os.statvfs(str(self._context.root))
        s = s.f_frsize * s.f_bavail
        s = s - self._pending_size
        return s

    # in bytes
    async def _get_node_size(self, node):
        if node.trashed:
            return 0

        if not node.is_folder:
            return node.size

        children = await self._context.drive.get_children(node)
        children = (self._get_node_size(_) for _ in children)
        children = await asyncio.gather(*children)
        return sum(children)

    async def _check_existence(self, node, full_path):
        full_path /= node.name

        if node.is_folder:
            children = await self._context.drive.get_children(node)
            for child in children:
                ok = await self._check_existence(child, full_path)
                if not ok:
                    return False
            return True

        if not full_path.is_file():
            # is not file or does not even exists
            return False

        INFO('ddld') << 'checking existed:' << full_path
        local = await self._md5sum(full_path)
        remote = node.md5
        if local == remote:
            INFO('ddld') << 'skip same file'
            return True

        INFO('ddld') << 'expected:' << remote << 'got:' << local
        INFO('ddld') << 'remove' << full_path
        full_path.unlink()
        return False

    async def _download(self, node, local_path, need_mtime):
        if not node or not local_path:
            return False

        if node.trashed:
            return False

        try:
            if await self._check_existence(node, local_path):
                if not need_mtime:
                    full_path = local_path / node.name
                    ok = update_mtime(full_path, dt.datetime.now().timestamp())
                    return ok
                return True
        except OSError as e:
            if e.errno == 36:
                WARNING('ddld') << 'download failed: file name too long'
                return False
            # fatal unknown error
            raise

        DEBUG('ddld') << 'different'

        required_space = await self._get_node_size(node)
        if await self._need_recycle(required_space):
            if need_mtime and self._is_too_old(node):
                DEBUG('ddld') << 'too old'
                self.abort()
                return False
            await self._reserve_space(node)

        with self._reserve_pending_file(required_space):
            rv = await self._download_glue(node, local_path, need_mtime)

        return rv

    async def _download_glue(self, node, local_path, need_mtime):
        if node.trashed:
            return False

        full_path = local_path / node.name

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
            WARNING('ddld') << 'mkdir failed:' << full_path
            return False

        children = await self._context.drive.get_children(node)
        for child in children:
            ok = await self._download_glue(child, full_path, need_mtime)
            if not ok:
                return False

        return True

    async def _download_file(self, node, local_path, full_path):
        drive = self._context.drive
        # retry until succeed
        while True:
            try:
                remote_path = await drive.get_path(node)
                INFO('ddld') << 'downloading:' << remote_path
                ok = await drive.download_file(node, local_path)
                INFO('ddld') << 'checking:' << remote_path
                local_hash = await self._md5sum(full_path)
                INFO('ddld') << local_hash << remote_path
            except wdg.DownloadError as e:
                ERROR('ddld') << 'download failed:' << str(e)
            except OSError as e:
                if e.errno == 36:
                    WARNING('ddld') << 'download failed: file name too long'
                    return False
                # fatal unknown error
                raise
            else:
                remote_hash = node.md5
                if local_hash != remote_hash:
                    INFO('ddld') << 'md5 mismatch:' << full_path
                    full_path.unlink()
                else:
                    break

        return True

    async def _md5sum(self, full_path):
        return await self._loop.run_in_executor(self._pool, md5sum, full_path)

    @cl.contextmanager
    def _reserve_pending_file(self, size):
        self._pending_size = self._pending_size + size
        try:
            yield
        finally:
            self._pending_size = self._pending_size - size


class DownloadTask(ww.Task):

    def __init__(self, callable_, node, local_path, need_mtime):
        super(DownloadTask, self).__init__(ft.partial(callable_, node,
                                                      local_path, need_mtime))

        self._node = node

    def __repr__(self):
        return '{0}(native_id={1}, id={2})'.format(self.__class__.__name__, hex(id(self)), self.id_)

    def higher_then(self, that):
        # if that is not a DownloadTask, fallback to base class
        if not isinstance(that, DownloadTask):
            return super(DownloadTask, self).higher_then(that)
        # compare priority first
        if self.priority > that.priority:
            return True
        if self.priority < that.priority:
            return False
        # different policy
        rv = self.compare_node(that)
        if rv is not None:
            return rv
        # oldest task download first
        return self.id_ < that.id_

    def compare_node(self, that):
        return None


class HighDownloadTask(DownloadTask):

    def __init__(self, callable_, node, local_path):
        super(HighDownloadTask, self).__init__(callable_, node, local_path, False)

    @property
    def priority(self):
        return 2


class LowDownloadTask(DownloadTask):

    def __init__(self, callable_, node, local_path):
        super(LowDownloadTask, self).__init__(callable_, node, local_path, True)

    @property
    def priority(self):
        return 1

    def compare_node(self, that):
        # latest file download first
        if self._node.modified > that._node.modified:
            return True
        if self._node.modified < that._node.modified:
            return False
        return None


class SearchEngine(object):

    def __init__(self, drive):
        super(SearchEngine, self).__init__()
        # NOTE only takes a reference, do not do clean up
        self._drive = drive
        self._loop = asyncio.get_event_loop()
        self._cache = {}
        self._searching = {}

    async def get_nodes_by_regex(self, pattern):
        nodes = self._cache.get(pattern, None)
        if nodes is not None:
            return nodes

        if pattern in self._searching:
            lock = self._searching[pattern]
            return await self._wait_for_result(lock, pattern)

        lock = asyncio.Condition()
        self._searching[pattern] = lock
        self._loop.create_task(self._search(pattern))
        return await self._wait_for_result(lock, pattern)

    async def clear_cache(self):
        while len(self._searching) > 0:
            pattern, lock = next(iter(self._searching.items()))
            async with lock:
                await lock.wait()
        self._cache = {}

    def drop_value(self, value):
        keys = list(self._cache.keys())
        for k in keys:
            if re.search(k, value, re.I):
                del self._cache[k]

    async def _search(self, pattern):
        lock = self._searching[pattern]
        try:
            nodes = await self._drive.find_nodes_by_regex(pattern)
            nodes = {_.id_: self._drive.get_path(_)
                     for _ in nodes if not _.trashed}
            nodes = await async_dict(nodes)
            self._cache[pattern] = nodes
        except Exception as e:
            EXCEPTION('ddld', e) << 'search failed, abort'
            raise u.SearchFailedError(str(e))
        finally:
            del self._searching[pattern]
            async with lock:
                lock.notify_all()

    async def _wait_for_result(self, lock, pattern):
        async with lock:
            await lock.wait()
        try:
            return self._cache[pattern]
        except KeyError:
            raise u.SearchFailedError('{0} canceled search'.format(pattern))


def preserve_mtime_by_node(full_path, node):
    mtime = node.modified.to('local').timestamp
    return update_mtime(full_path, mtime)


def update_mtime(full_path, s_mtime):
    try:
        os.utime(str(full_path), (s_mtime, s_mtime))
    except OSError as e:
        # file name too long
        if e.errno != 36:
            raise
    return True


def human_readable(bytes_):
    units = ['B', 'KB', 'MB', 'GB']
    for unit in units:
        if bytes_ < 1024:
            return bytes_, unit
        bytes_ /= 1024
    else:
        return bytes_ * 1024, units[-1]


def is_unlinkable(full_path):
    flag = (os.W_OK | os.X_OK) if full_path.is_dir() else os.W_OK
    return os.access(full_path, flag, effective_ids=True)


async def async_dict(dict_):
    dict_ = (wait_for_value(k, v) for k, v in dict_.items())
    dict_ = await asyncio.gather(*dict_)
    return dict(dict_)


async def wait_for_value(k, v):
    return k, await v


def normalize_search_pattern(raw):
    rv = re.match(r'(.+?)\s*\((.+)\)', raw)
    if rv:
        rv = rv.groups()
    else:
        rv = (raw,)
    rv = map(inner_normalize_search_pattern, rv)
    rv = '|'.join(rv)
    rv = '.*({0}).*'.format(rv)
    return rv


def inner_normalize_search_pattern(raw):
    rv = re.split(r'(?:\s|-)+', raw)
    rv = map(re.escape, rv)
    rv = '.*'.join(rv)
    return rv


def md5sum(full_path):
    hasher = hashlib.md5()
    with full_path.open('rb') as fin:
        while True:
            chunk = fin.read(65536)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()
