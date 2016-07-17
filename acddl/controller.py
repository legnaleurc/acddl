import datetime as dt
import functools
import hashlib
import os
import os.path as op
import pathlib
import re
import shutil
import time

from acdcli.api import client as ACD
from acdcli.api.common import RequestError
from acdcli.cache import db as DB
from acdcli.utils import hashing
from acdcli.utils.time import datetime_to_timestamp
from tornado import ioloop as ti, gen as tg

from .log import ERROR, WARNING, INFO, EXCEPTION, DEBUG
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
        main_loop = ti.IOLoop.instance()
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
        self._context.dl.download_later(node)

    def abort_pending(self):
        self._context.dl.abort()

    def update_cache_from(self, remote_paths):
        self._context.dl.multiple_download_later(*remote_paths)

    async def compare(self, node_ids):
        nodes = (self._context.db.get_node(_) for _ in node_ids)
        nodes = await tg.multi(nodes)
        unique = set(_.md5 for _ in nodes)
        if len(unique) == 1:
            return True
        else:
            return [_.size for _ in nodes]

    async def trash(self, node_id):
        return await self._context.db.trash(node_id)

    async def sync_db(self):
        await self._context.db.sync()


class DownloadController(object):

    def __init__(self, context):
        self._context = context
        self._worker = worker.AsyncWorker()
        self._last_recycle = 0

    def close(self):
        self._worker.stop()

    def download_later(self, node):
        self._ensure_alive()
        task = self._make_high_download_task(node)
        self._worker.do_later(task)

    def multiple_download_later(self, *remote_paths):
        self._ensure_alive()
        task = functools.partial(self._download_from, *remote_paths)
        self._worker.do_later(task)

    def abort(self):
        task = self._make_flush_task()
        self._worker.do_later(task)

    def _ensure_alive(self):
        self._worker.start()

    async def _download_from(self, *remote_paths):
        await self._context.db.sync()
        children = await self._get_unified_children(remote_paths)
        # mtime = self._get_oldest_mtime()
        # children = list(filter(lambda _: _.modified > mtime, children))
        for child in children:
            task = self._make_low_download_task(child)
            self._worker.do_later(task)

    def _make_high_download_task(self, node):
        return HighDownloadTask(self._download, node, self._context.root)

    def _make_low_download_task(self, node):
        return LowDownloadTask(self._download, node, self._context.root)

    def _make_flush_task(self):
        return FlushTask()

    async def _get_unified_children(self, remote_paths):
        children = (self._context.db.resolve_path(_) for _ in remote_paths)
        children = await tg.multi(children)
        children = (self._context.db.get_children(_) for _ in children)
        children = await tg.multi(children)
        children = [_1 for _0 in children for _1 in _0]
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

    async def _is_too_old(self, node):
        if not await self._need_recycle(node):
            return False
        # mtime = datetime_to_timestamp(node.modified)
        mtime = self._get_oldest_mtime()
        # DEBUG('acddl') << 'latest recyled' << self._last_recycle << 'node' << mtime
        # return mtime <= self._last_recycle
        return node.modified <= mtime

    async def _reserve_space(self, node):
        entries = None
        while await self._need_recycle(node):
            if not entries:
                entries = self._get_cache_entries()
            full_path, mtime = entries.pop(0)
            if full_path.is_dir():
                shutil.rmtree(str(full_path))
            else:
                full_path.unlink()
            self._last_recycle = mtime
            INFO('acddl') << 'recycled:' << full_path

    async def _need_recycle(self, node):
        free_space = self._get_free_space()
        required_space = await self._get_node_size(node)
        hfs, fsu = human_readable(free_space)
        hrs, rsu = human_readable(required_space)
        INFO('acddl') << 'free space: {0} {1}, required: {2} {3}'.format(hfs, fsu, hrs, rsu)
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

        if need_mtime and await self._is_too_old(node):
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
        if check_existed(node, full_path):
            return True

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


class HighDownloadTask(DownloadTask):

    def __init__(self, callable_, node, local_path):
        super(HighDownloadTask, self).__init__(callable_, node, local_path, False)

    def __gt__(self, that):
        if self.priority < that.priority:
            return True
        if self.priority > that.priority:
            return False
        return id(self) < id(that)

    def __eq__(self, that):
        if self.priority != that.priority:
            return False
        return id(self) == id(that)

    @property
    def priority(self):
        return 2


class LowDownloadTask(DownloadTask):

    def __init__(self, callable_, node, local_path):
        super(LowDownloadTask, self).__init__(callable_, node, local_path, True)

    def __gt__(self, that):
        if self.priority < that.priority:
            return True
        if self.priority > that.priority:
            return False
        if not self._node or not that._node:
            return False
        return self._node.modified < that._node.modified

    def __eq__(self, that):
        if self.priority != that.priority:
            return False
        if self._node and that._node:
            return self._node.modified == that._node.modified
        return self._node == that._node

    @property
    def priority(self):
        return 1


class FlushTask(worker.Task):

    def __init__(self):
        super(FlushTask, self).__init__()

    def __call__(self):
        raise worker.FlushTasks(self._filter)

    @property
    def priority(self):
        return 65535

    def _filter(self, task):
        return isinstance(task, LowDownloadTask)


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
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_client.get_changes, checkpoint=checkpoint, include_purged=include_purged, silent=True, file=None))

    async def iter_changes_lines(self, changes):
        await self._ensure_alive()
        return self._acd_client._iter_changes_lines(changes)

    async def move_to_trash(self, node_id):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_client.move_to_trash, node_id))

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
        INFO('acddl') << 'syncing'

        await self._ensure_alive()

        # copied from acd_cli

        check_point = await self._worker.do(functools.partial(self._acd_db.KeyValueStorage.get, self._CHECKPOINT_KEY))

        f = await self._context.client.get_changes(checkpoint=check_point, include_purged=bool(check_point))

        try:
            full = False
            first = True

            for changeset in await self._context.client.iter_changes_lines(f):
                if changeset.reset or (full and first):
                    await self._worker.do(self._acd_db.drop_all)
                    await self._worker.do(self._acd_db.init)
                    full = True
                else:
                    await self._worker.do(functools.partial(self._acd_db.remove_purged, changeset.purged_nodes))

                if changeset.nodes:
                    await self._worker.do(functools.partial(self._acd_db.insert_nodes, changeset.nodes, partial=not full))
                await self._worker.do(functools.partial(self._acd_db.KeyValueStorage.update, {
                    self._LAST_SYNC_KEY: time.time(),
                }))

                if changeset.nodes or changeset.purged_nodes:
                    await self._worker.do(functools.partial(self._acd_db.KeyValueStorage.update, {
                        self._CHECKPOINT_KEY: changeset.checkpoint,
                    }))

                first = False
        except RequestError as e:
            EXCEPTION('acddl') << str(e)
            return False

        INFO('acddl') << 'synced'

        return True

    async def resolve_path(self, remote_path):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.resolve, remote_path))

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
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.get_node, node_id))

    async def find_by_regex(self, pattern):
        await self._ensure_alive()
        return await self._worker.do(functools.partial(self._acd_db.find_by_regex, pattern))

    async def trash(self, node_id):
        await self._ensure_alive()
        try:
            r = await self._context.client.move_to_trash(node_id)
            DEBUG('acddl') << r
            await self._worker.do(functools.partial(self._acd_db.insert_node, r))
        except RequestError as e:
            EXCEPTION('acddl') << str(e)
            return False
        return True

    async def _ensure_alive(self):
        if not self._acd_db:
            self._worker.start()
            await self._worker.do(self._create_db)

    def _create_db(self):
        self._acd_db = DB.NodeCache(self._context.auth_path)


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


def human_readable(bytes):
    units = ['B', 'KB', 'MB', 'GB']
    for unit in units:
        if bytes < 1024:
            return bytes, unit
        bytes /= 1024
    else:
        return bytes * 1024, units[-1]


def check_existed(node, full_path):
    if not full_path.is_file():
        return False

    INFO('acddl') << 'checking existed:' << full_path
    local = md5sum(full_path)
    remote = node.md5
    if local == remote:
        INFO('acddl') << 'skip same file'
        return True

    INFO('acddl') << 'md5 mismatch'
    full_path.unlink()
    return False
