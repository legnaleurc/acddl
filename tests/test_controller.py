import functools
import unittest as ut
from unittest import mock as utm
import datetime as dt
import pathlib
import hashlib

from tornado import ioloop as ti, gen as tg
from pyfakefs import fake_filesystem as ffs

from acddl import controller as ctrl
from . import util as u


class PathMock(utm.Mock):

    def __init__(self, fs=None, *pathsegments, **kwargs):
        super(PathMock, self).__init__()

        self._fs = fs
        self._path = self._fs.JoinPaths(*pathsegments)

    def iterdir(self):
        fake_os = ffs.FakeOsModule(self._fs)
        for child in fake_os.listdir(self._path):
            yield PathMock(self._fs, self._path, child)

    def stat(self):
        fake_os = ffs.FakeOsModule(self._fs)
        return fake_os.stat(self._path)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        fake_os = ffs.FakeOsModule(self._fs)
        try:
            fake_os.makedirs(self._path)
        except OSError as e:
            # iDontCare
            pass
        return True

    def is_file(self):
        fake_os = ffs.FakeOsModule(self._fs)
        return fake_os.path.isfile(self._path)

    def unlink(self):
        fake_os = ffs.FakeOsModule(self._fs)
        return fake_os.unlink(self._path)

    def __truediv__(self, name):
        return PathMock(self._fs, self._path, name)

    def __str__(self):
        return self._path


class NodeMock(utm.Mock):

    def __init__(self, fs, path, *args, **kwargs):
        super(NodeMock, self).__init__()

        self._fs = fs
        self._path = path

    @property
    def name(self):
        dirname, basename = self._fs.SplitPath(self._path)
        return basename

    @property
    def modified(self):
        f = self._fs.GetObject(self._path)
        return dt.datetime.fromtimestamp(f.st_mtime)

    @property
    def is_available(self):
        return True

    @property
    def is_folder(self):
        fake_os = ffs.FakeOsModule(self._fs)
        return fake_os.path.isdir(self._path)

    @property
    def size(self):
        fake_os = ffs.FakeOsModule(self._fs)
        return fake_os.path.getsize(self._path)

    @property
    def md5(self):
        fake_open = ffs.FakeFileOpen(self._fs)
        hasher = hashlib.md5()
        with fake_open(self._path, 'rb') as fin:
            while True:
                chunk = fin.read(65536)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()


def create_fake_local_file_system():
    fs = ffs.FakeFilesystem()
    file_1 = fs.CreateFile('/local/file_1.txt', contents='file 1')
    file_1.st_mtime = 1467800000
    file_2 = fs.CreateFile('/local/folder_1/file_2.txt', contents='file 2')
    file_2.st_mtime = 1467801000
    folder_1 = fs.GetObject('/local/folder_1')
    folder_1.st_mtime = 1467802000
    return fs


def create_fake_remote_file_system():
    fs = ffs.FakeFilesystem()
    file_3 = fs.CreateFile('/remote/file_3.txt', contents='file 3')
    file_3.st_mtime = 1467803000
    file_4 = fs.CreateFile('/remote/folder_2/file_4.txt', contents='file 4')
    file_4.st_mtime = 1467804000
    folder_2 = fs.GetObject('/remote/folder_2')
    folder_2.st_mtime = 1467805000
    return fs


def metapathmock(fs, *args, **kwargs):
    class ConcretePathMock(PathMock):

        def __init__(self, *args, **kwargs):
            super(ConcretePathMock, self).__init__(fs, *args, **kwargs)

    return ConcretePathMock

create_nodemock = functools.partial(lambda fs, *args, **kwargs: NodeMock(fs, *args, **kwargs), fs=create_fake_remote_file_system())


class TestDownloadController(ut.TestCase):

    @utm.patch('acddl.worker.AsyncWorker', autospec=True)
    def testDownloadLater(self, FakeAsyncWorker):
        context = utm.Mock()
        dc = ctrl.DownloadController(context)
        node = utm.Mock()
        dc.download_later(node)
        dc._worker.start.assert_called_once_with()
        dc._worker.do_later.assert_called_once_with(utm.ANY)

    @utm.patch('acddl.worker.AsyncWorker', autospec=True)
    def testMultipleDownloadLater(self, FakeAsyncWorker):
        context = utm.Mock()
        dc = ctrl.DownloadController(context)
        dc.multiple_download_later('123', '456')
        dc._worker.start.assert_called_once_with()
        dc._worker.do_later.assert_called_once_with(utm.ANY)

    @utm.patch('acddl.worker.AsyncWorker', autospec=True)
    def testDownloadFrom(self, FakeAsyncWorker):
        lfs = create_fake_local_file_system()
        rfs = create_fake_remote_file_system()
        with utm.patch('pathlib.Path', new_callable=functools.partial(metapathmock, lfs)) as FakePath:
            context = utm.Mock()
            # mock acd_db
            context.db.sync = u.AsyncMock()
            context.db.resolve_path = functools.partial(fake_resolve_path, rfs)
            context.db.get_children = functools.partial(fake_get_children, rfs)
            # mock root
            context.root = pathlib.Path('/local')

            dc = ctrl.DownloadController(context)
            u.async_call(dc._download_from, '/remote')
            assert dc._worker.do_later.call_count == 2

    @utm.patch('os.utime')
    @utm.patch('os.statvfs')
    @utm.patch('acddl.worker.AsyncWorker', autospec=True)
    def testDownload(self, FakeAsyncWorker, fake_statvfs, fake_utime):
        lfs = create_fake_local_file_system()
        rfs = create_fake_remote_file_system()
        with utm.patch('pathlib.Path', new_callable=functools.partial(metapathmock, lfs)) as FakePath:
            context = utm.Mock()
            # mock client
            context.client.download_node = fake_download_node
            # mock db
            context.db.get_children = functools.partial(fake_get_children, rfs)
            context.db.get_path = functools.partial(fake_get_path, rfs)
            # mock root
            context.root = pathlib.Path('/local')
            # mock os
            vfs = utm.Mock()
            fake_statvfs.return_value = vfs
            vfs.f_frsize = 1
            vfs.f_bavail = 10 * 1024 ** 3

            dc = ctrl.DownloadController(context)
            u.async_call(dc._download, NodeMock(rfs, '/remote/folder_2'), context.root, True)

            l_fake_os = ffs.FakeOsModule(lfs)
            assert l_fake_os.path.isdir('/local/folder_2')
            assert l_fake_os.path.isfile('/local/folder_2/file_4.txt')


async def fake_resolve_path(fs, remote_path):
    return NodeMock(fs, remote_path)

async def fake_get_children(fs, node):
    fake_os = ffs.FakeOsModule(fs)
    children = fake_os.listdir(node._path)
    children = [NodeMock(fs, fs.JoinPaths(node._path, _)) for _ in children]
    return children

async def fake_get_path(fs, node):
    dirname, basename = fs.SplitPath(node._path)
    return dirname

async def fake_download_node(node, local_path):
    r_fake_open = ffs.FakeFileOpen(node._fs)
    l_fake_open = ffs.FakeFileOpen(local_path._fs)

    assert not node.is_folder

    local_file = str(local_path / node.name)
    with r_fake_open(node._path, 'rb') as fin, l_fake_open(local_file, 'wb') as fout:
        while True:
            chunk = fin.read(65535)
            if not chunk:
                break
            fout.write(chunk)

    hasher = hashlib.md5()
    with l_fake_open(local_file, 'rb') as fin:
        while True:
            chunk = fin.read(65536)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()
