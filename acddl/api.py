from tornado import web


class NodesHandler(web.RequestHandler):

    def post(self):
        acd_paths = self.get_arguments('acd_paths[]')

        controller = self.settings['controller']
        controller.update_cache_from(acd_paths)

    def put(self, id_):
        if id_ is None:
            self.set_status(400)
            return

        controller = self.settings['controller']
        controller.download(id_)
