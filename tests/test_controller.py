import unittest
from unittest import mock as um

from acddl import controller as ctrl


class TestDownloadController(unittest.TestCase):

    @um.patch('acddl.worker.AsyncWorker', autospec=True)
    def testDownloadLater(self, FakeAsyncWorker):
        context = um.Mock()
        dc = ctrl.DownloadController(context)
        node = um.Mock()
        dc.download_later(node)
        dc._worker.do_later.assert_called_once_with(um.ANY)

    @um.patch('acddl.worker.AsyncWorker', autospec=True)
    def testMultipleDownloadLater(self, FakeAsyncWorker):
        context = um.Mock()
        dc = ctrl.DownloadController(context)
        dc.multiple_download_later('123', '456')
        dc._worker.do_later.assert_called_once_with(um.ANY)
