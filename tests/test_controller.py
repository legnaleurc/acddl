import functools
import unittest as ut
from unittest import mock as utm
import datetime as dt
import pathlib

from tornado import ioloop as ti, gen as tg
from pyfakefs import fake_filesystem as ffs

from acddl import controller as ctrl
from . import util as u


class PathMock(utm.Mock):

    def __init__(self, fs, *pathsegments, **kwargs):
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

    def __init__(self, tree=None, *args, **kwargs):
        super(NodeMock, self).__init__()

        self._tree = tree

    @property
    def name(self):
        return self._tree['name']

    @property
    def modified(self):
        return dt.datetime.fromtimestamp(self._tree['mtime'])

    @property
    def is_available(self):
        return self._tree['is_available']

    @property
    def is_folder(self):
        return 'children' in self._tree

    @property
    def size(self):
        return self._tree['size']

    @property
    def md5(self):
        return self._tree['md5']


def create_fake_file_system():
    fs = ffs.FakeFilesystem()
    file_1 = fs.CreateFile('/local/file_1.txt', st_size=100)
    file_1.st_mtime = 1467808000
    file_2 = fs.CreateFile('/local/folder_1/file_2.txt', st_size=200)
    file_2.st_mtime = 1467807000
    folder_1 = fs.GetObject('/local/folder_1')
    folder_1.st_mtime = 1467809000
    return fs


def create_pathmock(fs, *args, **kwargs):
    return lambda *args, **kwargs: PathMock(fs, *args, **kwargs)


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

    @utm.patch('pathlib.Path', new_callable=functools.partial(create_pathmock, fs=create_fake_file_system()))
    @utm.patch('acddl.worker.AsyncWorker', autospec=True)
    def testDownloadFrom(self, FakeAsyncWorker, FakePath):
        context = utm.Mock()
        # mock acd_db
        context.acd_db.sync = u.AsyncMock()
        context.acd_db.resolve_path = u.AsyncMock()
        context.acd_db.get_children = u.AsyncMock(return_value=[
            NodeMock(REMOTE_TREE_1),
            NodeMock(REMOTE_TREE_1),
        ])
        # mock root
        context.root = pathlib.Path('/local')

        dc = ctrl.DownloadController(context)
        u.async_call(dc._download_from, '/remote')
        assert dc._worker.do_later.call_count == 2

    @utm.patch('os.utime')
    @utm.patch('os.statvfs')
    @utm.patch('pathlib.Path', new_callable=functools.partial(create_pathmock, fs=create_fake_file_system()))
    @utm.patch('acddl.worker.AsyncWorker', autospec=True)
    def testDownload(self, FakeAsyncWorker, FakePath, fake_statvfs, fake_utime):
        context = utm.Mock()
        # mock acd_client
        context.acd_client.download_node = u.AsyncMock(return_value='remote_md5')
        # mock acd_db
        context.acd_db.get_children = u.AsyncMock(return_value=[
            NodeMock(REMOTE_TREE_1['children'][0]),
            NodeMock(REMOTE_TREE_1['children'][1]),
        ])
        context.acd_db.get_path = u.AsyncMock(return_value='/remote/test')
        # mock root
        context.root = pathlib.Path('/local')
        # mock os
        vfs = utm.Mock()
        fake_statvfs.return_value = vfs
        vfs.f_frsize = 1
        vfs.f_bavail = 10 * 1024 ** 3

        dc = ctrl.DownloadController(context)
        u.async_call(dc._download, NodeMock(REMOTE_TREE_1), context.root, True)


LOCAL_TREE_1 = {
    'name': '/',
    'mtime': 1467809000,
    'children': [
        {
            'name': 'a.txt',
            'mtime': 1467808000,
            'size': 100,
        },
        {
            'name': 'b.txt',
            'mtime': 1467807000,
            'size': 200,
        },
    ],
}


REMOTE_TREE_1 = {
    'name': '/',
    'mtime': 1467809000,
    'is_available': True,
    'children': [
        {
            'name': 'a.txt',
            'mtime': 1467808000,
            'size': 100,
            'is_available': True,
            'md5': 'remote_md5',
        },
        {
            'name': 'b.txt',
            'mtime': 1467807000,
            'size': 200,
            'is_available': False,
            'md5': 'remote_md5',
        },
    ],
}
