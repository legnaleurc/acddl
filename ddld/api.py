import json

from tornado import web as tw, websocket as tws, ioloop as ti


class NodesHandler(tw.RequestHandler):

    async def get(self):
        pattern = self.get_argument('pattern', None)
        if not pattern:
            self.set_status(400)
            return

        controller = self.settings['controller']
        nodes = await controller.search(pattern)
        nodes = [{'id': k, 'name': v} for k, v in nodes.items()]
        nodes = sorted(nodes, key=lambda _: _['name'])
        nodes = json.dumps(nodes)
        self.write(nodes + '\n')

    async def post(self):
        controller = self.settings['controller']
        await controller.sync_db()

    def delete(self, id_):
        if id_ is None:
            self.set_status(400)
            return

        controller = self.settings['controller']
        controller.trash(id_)


class CacheHandler(tw.RequestHandler):

    async def get(self):
        nodes = self.get_arguments('nodes[]')

        controller = self.settings['controller']
        result = await controller.compare(nodes)
        # iDontCare
        result = json.dumps(result)
        self.write(result)

    def post(self):
        controller = self.settings['controller']

        acd_paths = self.get_arguments('acd_paths[]')
        if not acd_paths:
            controller.sync_db()
            return

        controller.download_low(acd_paths)

    def put(self, id_):
        if id_ is None:
            self.set_status(400)
            return

        controller = self.settings['controller']
        controller.download_high(id_)


class LogHandler(tw.RequestHandler):

    def get(self):
        logs = self.settings['logs']
        # iDontCare
        result = json.dumps(logs.get_recent())
        self.write(result)


class LogSocketHandler(tws.WebSocketHandler):

    _counter = 0

    def open(self):
        self._id = self._counter
        self._counter = self._counter + 1
        self._beat = ti.PeriodicCallback(self._ping, 20 * 1000)
        self._beat.start()

        logs = self.settings['logs']
        logs.add(self._id, self)

    def on_close(self):
        logs = self.settings['logs']
        logs.remove(self._id)
        self._beat.stop()

    def _ping(self):
        self.ping(b'_')
