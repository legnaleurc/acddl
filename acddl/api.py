from tornado import web


class CacheHandler(web.RequestHandler):

    def post(self):
        acd_paths = self.get_arguments('acd_paths[]')

        controller = self.settings['controller']
        controller.update_cache_from(acd_paths)


class DownloadHandler(web.RequestHandler):

    def post(self):
        node_id = self.get_argument('node_id', None)

        if node_id is None:
            self.set_status(400)
            return

        controller = self.settings['controller']
        controller.download(node_id)
