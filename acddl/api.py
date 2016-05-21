from tornado import web


class DownloadHandler(web.RequestHandler):

    def post(self):
        node_id = self.get_argument('node_id', None)
        priority = self.get_argument('priority', None)
        need_mtime = self.get_argument('need_mtime', None)

        if any(_ is None for _ in (node_id, need_mtime, priority)):
            self.set_status(400)
            return

        controller = self.settings['controller']
        priority = int(priority)
        need_mtime = bool(need_mtime)
        controller.download(node_id, priority, need_mtime)
