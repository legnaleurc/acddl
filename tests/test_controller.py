import functools
import unittest
from unittest import mock as um

from tornado import ioloop as ti, gen as tg

from acddl import controller as ctrl
from . import util as u


class TestDownloadController(unittest.TestCase):

    @um.patch('acddl.worker.AsyncWorker', autospec=True)
    def testDownloadLater(self, FakeAsyncWorker):
        context = um.Mock()
        dc = ctrl.DownloadController(context)
        node = um.Mock()
        dc.download_later(node)
        dc._worker.start.assert_called_once_with()
        dc._worker.do_later.assert_called_once_with(um.ANY)

    @um.patch('acddl.worker.AsyncWorker', autospec=True)
    def testMultipleDownloadLater(self, FakeAsyncWorker):
        context = um.Mock()
        dc = ctrl.DownloadController(context)
        dc.multiple_download_later('123', '456')
        dc._worker.start.assert_called_once_with()
        dc._worker.do_later.assert_called_once_with(um.ANY)

    @um.patch('acddl.worker.AsyncWorker', autospec=True)
    #@unittest.skip('need refactor')
    def testDownloadFrom(self, FakeAsyncWorker):
        context = um.Mock()
        # mock acd_db
        context.acd_db.sync = u.AsyncMock()
        context.acd_db.resolve_path = u.AsyncMock()
        context.acd_db.get_children = u.AsyncMock(return_value=[])
        # mock root
        context.root = PathMock(FS_1)

        dc = ctrl.DownloadController(context)
        u.async_call(dc._download_from, '/tmp')


class PathMock(um.Mock):

    def __init__(self, tree):
        super(PathMock, self).__init__()

        self._tree = tree

    def iterdir(self):
        for child in self._tree['children']:
            yield PathMock(child)

    def stat(self):
        m = um.Mock()
        m.st_mtime = self._tree['mtime']
        return m


FS_1 = {
    'name': '/',
    'mtime': 123456,
    'children': [
        {
            'name': 'a.txt',
            'mtime': 123456,
            'size': 100,
        },
        {
            'name': 'b.txt',
            'mtime': 123456,
            'size': 200,
        },
    ],
}
