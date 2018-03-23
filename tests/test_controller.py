import functools
import unittest as ut
from unittest import mock as utm

from pyfakefs import fake_filesystem as ffs
from tornado import testing as tt

from ddld import controller as ctrl
from . import util as u


class TestDownloadController(tt.AsyncTestCase):

    @utm.patch('wcpan.worker.AsyncQueue', autospec=True)
    @tt.gen_test
    def testDownloadFrom(self, FakeAsyncWorker):
        lfs = u.create_fake_local_file_system()
        rfs = u.create_fake_remote_file_system()
        with utm.patch('pathlib.Path', new_callable=functools.partial(u.metapathmock, lfs)) as FakePath:
            context = utm.Mock()
            # mock search_engine
            context.search_engine.clear_cache = u.AsyncMock()
            # mock drive
            context.drive.sync = u.AsyncMock()
            context.drive.get_node_by_path = functools.partial(fake_resolve_path, rfs)
            context.drive.get_children = functools.partial(fake_get_children, rfs)
            # mock root
            context.root = FakePath('/local')

            dc = ctrl.DownloadController(context)
            yield dc._download_from('/remote')
            self.assertEqual(dc._queue.post.call_count, 2)

    @utm.patch('os.utime')
    @utm.patch('os.statvfs')
    @utm.patch('wcpan.worker.AsyncWorker', autospec=True)
    @tt.gen_test
    def testDownload(self, FakeAsyncWorker, fake_statvfs, fake_utime):
        lfs = u.create_fake_local_file_system()
        rfs = u.create_fake_remote_file_system()
        with utm.patch('pathlib.Path', new_callable=functools.partial(u.metapathmock, lfs)) as FakePath:
            context = utm.Mock()
            # mock drive
            context.drive.download_file = fake_download_node
            context.drive.get_children = functools.partial(fake_get_children, rfs)
            context.drive.get_path = functools.partial(fake_get_path, rfs)
            # mock root
            context.root = FakePath('/local')
            # mock os
            vfs = utm.Mock()
            fake_statvfs.return_value = vfs
            vfs.f_frsize = 1
            vfs.f_bavail = 10 * 1024 ** 3

            dc = ctrl.DownloadController(context)
            yield dc._download(u.NodeMock(rfs, '/remote/folder_2'), context.root, True)

            l_fake_os = ffs.FakeOsModule(lfs)
            self.assertTrue(l_fake_os.path.isdir('/local/folder_2'))
            self.assertTrue(l_fake_os.path.isfile('/local/folder_2/file_4.txt'))


class TestDownloadTask(ut.TestCase):

    def testSort(self):
        a = self._createLowDownloadTask(100)
        b = self._createLowDownloadTask(200)
        c = self._createHighDownloadTask()
        d = self._createHighDownloadTask()
        e = sorted([a, b, c, d])
        self.assertEqual(e, [c, d, b, a])

    def _createHighDownloadTask(self):
        a = utm.Mock()
        b = utm.Mock()
        b.modified = 0
        return ctrl.HighDownloadTask(a, b, a)

    def _createLowDownloadTask(self, mtime):
        a = utm.Mock()
        b = utm.Mock()
        b.modified = mtime
        return ctrl.LowDownloadTask(a, b, a)


async def fake_resolve_path(fs, remote_path):
    return u.NodeMock(fs, remote_path)

async def fake_get_children(fs, node):
    fake_os = ffs.FakeOsModule(fs)
    children = fake_os.listdir(node._path)
    children = [u.NodeMock(fs, fs.JoinPaths(node._path, _)) for _ in children]
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

    return u.get_md5(l_fake_open, local_file)
