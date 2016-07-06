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
    @unittest.skip('need refactor')
    def testDownloadFrom(self, FakeAsyncWorker):
        context = um.Mock()
        context.acd_db.sync = u.AsyncMock()
        context.acd_db.resolve_path = u.AsyncMock()
        context.acd_db.get_children = u.AsyncMock(return_value=[])
        dc = ctrl.DownloadController(context)
        u.async_call(dc._download_from, '/tmp')
