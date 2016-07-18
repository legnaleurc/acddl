import json

from tornado import web


class NodesHandler(web.RequestHandler):

    async def get(self):
        pattern = self.get_argument('pattern', None)
        if not pattern:
            self.set_status(400)
            return

        controller = self.settings['controller']
        nodes = await controller.search(pattern)
        self.write(nodes)

    async def post(self):
        controller = self.settings['controller']
        await controller.sync_db()

    def delete(self, id_):
        if id_ is None:
            self.set_status(400)
            return

        controller = self.settings['controller']
        controller.trash(id_)


class CacheHandler(web.RequestHandler):

    async def get(self):
        nodes = self.get_arguments('nodes[]')

        controller = self.settings['controller']
        result = await controller.compare(nodes)
        # iDontCare
        result = json.dumps(result)
        self.write(result)

    async def post(self):
        controller = self.settings['controller']

        acd_paths = self.get_arguments('acd_paths[]')
        if not acd_paths:
            controller.abort_pending()
            return

        await controller.download_low(acd_paths)

    async def put(self, id_):
        if id_ is None:
            self.set_status(400)
            return

        controller = self.settings['controller']
        await controller.download_high(id_)
