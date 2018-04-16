import argparse
import asyncio
import logging
import os.path as op
import signal
import sys

from aiohttp import web as aw
import aiohttp_jinja2 as aj
import jinja2
from wcpan.logger import setup as setup_logger, INFO

from . import util, api, view
from .controller import RootController


class Daemon(object):

    def __init__(self, args):
        self._kwargs = parse_args(args[1:])
        self._loop = asyncio.get_event_loop()
        self._finished = asyncio.Event()
        self._loggers = setup_logger((
            'aiohttp',
            'wcpan.drive.google',
            'wcpan.worker',
            'ddld',
        ), '/tmp/ddld.log')

    def __call__(self):
        self._loop.create_task(self._guard())
        self._loop.add_signal_handler(signal.SIGINT, self._close)
        self._loop.run_forever()
        self._loop.close()

        return 0

    async def _guard(self):
        try:
            return await self._main()
        finally:
            self._loop.stop()
        return 1

    async def _main(self):
        app = aw.Application()

        setup_static_and_view(app)
        setup_api_path(app)

        async with ControllerContext(app, self._kwargs.root), \
                   LoggerContext(app, self._loggers), \
                   ServerContext(app, self._kwargs.listen):
            INFO('ddld') << 'ready'
            await self._until_finished()

        return 0

    async def _until_finished(self):
        await self._finished.wait()

    def _close(self):
        self._finished.set()


class ControllerContext(object):

    def __init__(self, app, root_path):
        self._app = app
        self._ctrl = RootController(root_path)

    async def __aenter__(self):
        await self._ctrl.__aenter__()
        self._app['controller'] = self._ctrl
        return self._ctrl

    async def __aexit__(self, exc_type, exc, tb):
        await self._ctrl.__aexit__(exc_type, exc, tb)


class ServerContext(object):

    def __init__(self, app, port):
        self._runner = aw.AppRunner(app)
        self._port = port

    async def __aenter__(self):
        await self._runner.setup()
        site = aw.TCPSite(self._runner, port=self._port)
        await site.start()
        return self._runner

    async def __aexit__(self, exc_type, exc, tb):
        await self._runner.cleanup()


class LoggerContext(object):

    def __init__(self, app, loggers):
        self._lsh = api.LogSocketHandler(app)
        self._app = app
        self._loggers = loggers

    async def __aenter__(self):
        lq = util.LogQueue(logging.DEBUG)
        for logger in self._loggers:
            logger.addHandler(lq)

        self._app['log_queue'] = lq
        self._app.router.add_view(r'/api/v1/socket', self._lsh.handle)

        return self._lsh

    async def __aexit__(self, exc_type, exc, tb):
        await self._lsh.close()


def parse_args(args):
    parser = argparse.ArgumentParser('ddld')

    parser.add_argument('-l', '--listen', required=True, type=int)
    parser.add_argument('-r', '--root', required=True, type=str)

    args = parser.parse_args(args)

    return args


def setup_static_and_view(app):
    root = op.dirname(__file__)
    static_path = op.join(root, 'static')
    template_path = op.join(root, 'templates')

    app.router.add_static('/static/', path=static_path, name='static')
    app['static_root_url'] = '/static'

    aj.setup(app, loader=jinja2.FileSystemLoader(template_path))

    app.router.add_view(r'/', view.IndexHandler)


def setup_api_path(app):
    app.router.add_view(r'/api/v1/nodes', api.NodesHandler)
    app.router.add_view(r'/api/v1/nodes/{id:[a-zA-Z0-9\-_]+}', api.NodesHandler)
    app.router.add_view(r'/api/v1/cache', api.CacheHandler)
    app.router.add_view(r'/api/v1/cache/{id:[a-zA-Z0-9\-_]+}', api.CacheHandler)
    app.router.add_view(r'/api/v1/log', api.LogHandler)


main = Daemon(sys.argv)
exit_code = main()
sys.exit(exit_code)
