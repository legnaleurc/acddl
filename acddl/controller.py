import datetime as dt
import functools
import hashlib
import os
import os.path as op
import pathlib
import re
import shutil
import time

from tornado import ioloop as ti, gen as tg, locks as tl
import wcpan.acd as wa
import wcpan.worker as ww
from wcpan.logger import ERROR, WARNING, INFO, EXCEPTION, DEBUG


class Context(object):

    def __init__(self, root_path):
        self._root = pathlib.Path(root_path)
        self._auth_path = op.expanduser('~/.cache/acd_cli')
        self._dl = DownloadController(self)
        self._acd = wa.ACDController(self._auth_path)
        self._search_engine = SearchEngine(self._acd)

    def close(self):
        for ctrl in (self._dl, self._acd):
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
    def acd(self):
        return self._acd

    @property
    def search_engine(self):
        return self._search_engine


class RootController(object):

    def __init__(self, cache_folder):
        self._context = Context(cache_folder)
        self._worker = ww.AsyncWorker()

    def close(self, signum, frame):
        self._worker.stop()
        self._context.close()
        main_loop = ti.IOLoop.instance()
        main_loop.stop()

    async def search(self, pattern):
        real_pattern = re.sub(r'(\s|-)+', '.*', pattern)
        real_pattern = '.*{0}.*'.format(real_pattern)
        try:
            re.compile(real_pattern)
        except Exception as e:
            EXCEPTION('ddl', e) << real_pattern
            return []

        nodes = await self._context.search_engine.get_nodes_by_regex(real_pattern)
        return nodes

    def download_high(self, node_id):
        self._ensure_alive()
        self._worker.do_later(functools.partial(self._download_glue, node_id))

    def abort_pending(self):
        self._ensure_alive()
        self._worker.do_later(self._context.dl.abort)

    def download_low(self, remote_paths):
        self._ensure_alive()
        self._worker.do_later(functools.partial(self._context.dl.multiple_download_later, *remote_paths))

    async def compare(self, node_ids):
        nodes = (self._context.acd.get_node(_) for _ in node_ids)
        nodes = await tg.multi(nodes)
        unique = set(_.md5 for _ in nodes)
        if len(unique) == 1:
            return True
        else:
            return [_.size for _ in nodes]

    def trash(self, node_id):
        self._ensure_alive()
        self._worker.do_later(functools.partial(self._context.acd.trash, node_id))

    async def sync_db(self):
        await self._context.search_engine.clear_cache()
        await self._context.acd.sync()

    def _ensure_alive(self):
        self._worker.start()

    async def _download_glue(self, node_id):
        node = await self._context.acd.get_node(node_id)
        self._context.dl.download_later(node)


class DownloadController(object):

    def __init__(self, context):
        self._context = context
        self._worker = ww.AsyncWorker()
        self._last_recycle = 0

    def close(self):
        self._worker.stop()

    def download_later(self, node):
        self._ensure_alive()
        task = self._make_high_download_task(node)
        self._worker.do_later(task)

    async def multiple_download_later(self, *remote_paths):
        self._ensure_alive()
        await self.abort()
        task = functools.partial(self._download_from, *remote_paths)
        self._worker.do_later(task)

    async def abort(self):
        await self._worker.flush(lambda _: isinstance(_, LowDownloadTask))

    def _abort_later(self):
        self._worker.flush_later(lambda _: isinstance(_, LowDownloadTask))

    def _ensure_alive(self):
        self._worker.start()

    async def _download_from(self, *remote_paths):
        await self._context.search_engine.clear_cache()
        await self._context.acd.sync()
        children = await self._get_unified_children(remote_paths)
        for child in children:
            task = self._make_low_download_task(child)
            self._worker.do_later(task)

    def _make_high_download_task(self, node):
        return HighDownloadTask(self._download, node, self._context.root)

    def _make_low_download_task(self, node):
        return LowDownloadTask(self._download, node, self._context.root)

    async def _get_unified_children(self, remote_paths):
        children = (self._context.acd.resolve_path(_) for _ in remote_paths)
        children = await tg.multi(children)
        children = (self._context.acd.get_children(_) for _ in children)
        children = await tg.multi(children)
        children = [_1 for _0 in children for _1 in _0]
        children = sorted(children, key=lambda _: _.modified, reverse=True)
        return children

    def _get_oldest_mtime(self):
        entries = self._get_recyclable_entries()
        if not entries:
            return dt.datetime.fromtimestamp(0)
        full_path, mtime = entries[0]
        # just convert from local TZ, no need to use UTC
        return dt.datetime.fromtimestamp(mtime)

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
        while await self._need_recycle(node):
            if not entries:
                entries = self._get_recyclable_entries()
            full_path, mtime = entries.pop(0)
            if full_path.is_dir():
                shutil.rmtree(str(full_path))
            else:
                full_path.unlink()
            self._last_recycle = mtime
            INFO('ddl') << 'recycled:' << full_path

    async def _need_recycle(self, node):
        free_space = self._get_free_space()
        required_space = await self._get_node_size(node)
        hfs, fsu = human_readable(free_space)
        hrs, rsu = human_readable(required_space)
        INFO('ddl') << 'free space: {0:.2f} {1}, required: {2:.2f} {3}'.format(hfs, fsu, hrs, rsu)
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

        children = await self._context.acd.get_children(node)
        children = (self._get_node_size(_) for _ in children)
        children = await tg.multi(children)
        return sum(children)

    async def _check_existence(self, node, full_path):
        full_path /= node.name

        if node.is_folder:
            children = await self._context.acd.get_children(node)
            for child in children:
                ok = await self._check_existence(child, full_path)
                if not ok:
                    return False
            return True

        if not full_path.is_file():
            # is not file or does not even exists
            return False

        INFO('ddl') << 'checking existed:' << full_path
        local = md5sum(full_path)
        remote = node.md5
        if local == remote:
            INFO('ddl') << 'skip same file'
            return True

        INFO('ddl') << 'expected:' << remote << 'got:' << local
        INFO('ddl') << 'remove' << full_path
        full_path.unlink()
        return False

    async def _download(self, node, local_path, need_mtime):
        if not node or not local_path:
            return False

        if not node.is_available:
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
                WARNING('ddl') << 'download failed: file name too long'
                return False
            # fatal unknown error
            raise

        DEBUG('ddl') << 'different'

        if await self._need_recycle(node):
            if need_mtime and self._is_too_old(node):
                DEBUG('ddl') << 'too old'
                self._abort_later()
                return False
            await self._reserve_space(node)

        return await self._download_glue(node, local_path, need_mtime)

    async def _download_glue(self, node, local_path, need_mtime):
        if not node.is_available:
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
            WARNING('ddl') << 'mkdir failed:' << full_path
            return False

        children = await self._context.acd.get_children(node)
        for child in children:
            ok = await self._download_glue(child, full_path, need_mtime)
            if not ok:
                return False

        return True

    async def _download_file(self, node, local_path, full_path):
        # retry until succeed
        while True:
            try:
                remote_path = await self._context.acd.get_path(node)
                INFO('ddl') << 'downloading:' << remote_path
                local_hash = await self._context.acd.download_node(node, local_path)
                INFO('ddl') << 'downloaded'
            except wa.RequestError as e:
                ERROR('ddl') << 'download failed:' << str(e)
            except OSError as e:
                if e.errno == 36:
                    WARNING('ddl') << 'download failed: file name too long'
                    return False
                # fatal unknown error
                raise
            else:
                remote_hash = node.md5
                if local_hash != remote_hash:
                    INFO('ddl') << 'md5 mismatch:' << full_path
                    full_path.unlink()
                else:
                    break

        return True


class DownloadTask(ww.Task):

    def __init__(self, callable_, node, local_path, need_mtime):
        super(DownloadTask, self).__init__(functools.partial(callable_, node, local_path, need_mtime))

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

    def __init__(self, acd):
        super(SearchEngine, self).__init__()
        # NOTE only takes a reference, do not do clean up
        self._acd = acd
        self._cache = {}
        self._searching = {}

    async def get_nodes_by_regex(self, pattern):
        nodes = self._cache.get(pattern, None)
        if nodes is not None:
            return nodes

        if pattern in self._searching:
            lock = self._searching[pattern]
            await lock.wait()
            return self._cache[pattern]

        lock = tl.Condition()
        self._searching[pattern] = lock
        nodes = await self._acd.find_by_regex(pattern)
        nodes = {_.id: self._acd.get_path(_) for _ in nodes if _.is_available}
        nodes = await tg.multi(nodes)
        self._cache[pattern] = nodes
        del self._searching[pattern]
        lock.notify_all()
        return nodes

    async def clear_cache(self):
        while len(self._searching) > 0:
            pattern, lock = next(iter(self._searching.items()))
            await lock.wait()
        self._cache = {}


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
    mtime = wa.datetime_to_timestamp(node.modified)
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
