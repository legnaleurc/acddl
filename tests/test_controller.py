import functools
import unittest
from unittest import mock as um
import datetime as dt

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
    def testDownloadFrom(self, FakeAsyncWorker):
        context = um.Mock()
        # mock acd_db
        context.acd_db.sync = u.AsyncMock()
        context.acd_db.resolve_path = u.AsyncMock()
        context.acd_db.get_children = u.AsyncMock(return_value=[
            NodeMock(REMOTE_TREE_1),
            NodeMock(REMOTE_TREE_1),
        ])
        # mock root
        context.root = PathMock(LOCAL_TREE_1)

        dc = ctrl.DownloadController(context)
        u.async_call(dc._download_from, '/tmp')
        assert dc._worker.do_later.call_count == 2

    @um.patch('acddl.worker.AsyncWorker', autospec=True)
    @unittest.skip('need refactor')
    def testDownload(self, FakeAsyncWorker):
        context = um.Mock()
        # mock acd_db
        # context.acd_db.sync = u.AsyncMock()
        # context.acd_db.resolve_path = u.AsyncMock()
        context.acd_db.get_children = u.AsyncMock(return_value=[
            NodeMock(REMOTE_TREE_1['children'][0]),
            NodeMock(REMOTE_TREE_1['children'][1]),
        ])
        # mock root
        # context.root = PathMock(FS_1)

        dc = ctrl.DownloadController(context)
        u.async_call(dc._download, NodeMock(REMOTE_TREE_1), '/tmp', True)
        assert dc._worker.do_later.call_count == 2


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


class NodeMock(um.Mock):

    def __init__(self, tree):
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
        },
        {
            'name': 'b.txt',
            'mtime': 1467807000,
            'size': 200,
            'is_available': False,
        },
    ],
}
