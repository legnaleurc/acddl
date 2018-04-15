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


class DaemonHelper(object):

    def __init__(self):
        self._finished = asyncio.Event()

    def on_stop(self):
        self._finished.set()

    async def start(self):
        await self._finished.wait()


def main(args=None):
    if args is None:
        args = sys.argv

    main_loop = asyncio.get_event_loop()
    main_loop.create_task(amain(args))
    main_loop.run_forever()
    main_loop.close()
    return 0


async def amain(args):
    args = parse_args(args[1:])

    loggers = setup_logger((
        'aiohttp',
        'wcpan.drive.google',
        'wcpan.worker',
        'ddld',
    ), '/tmp/ddld.log')
    logs = util.LogQueue(logging.DEBUG)
    for logger in loggers:
        logger.addHandler(logs)

    main_loop = asyncio.get_event_loop()

    application = aw.Application()

    setup_static_and_view(application)
    setup_api_path(application)

    lsh = api.LogSocketHandler(application)
    application.router.add_view(r'/api/v1/socket', lsh.handle)

    daemon = DaemonHelper()
    main_loop.add_signal_handler(signal.SIGINT, daemon.on_stop)

    async with RootController(args.root) as controller:
        application['controller'] = controller
        application['logs'] = logs

        runner = aw.AppRunner(application)
        await runner.setup()
        site = aw.TCPSite(runner, port=args.listen)
        await site.start()

        INFO('ddld') << 'ready'

        await daemon.start()

        await lsh.close()
        await runner.cleanup()

    main_loop.stop()

    return 0


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


exit_code = main()
sys.exit(exit_code)
