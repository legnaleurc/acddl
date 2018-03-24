import argparse
import logging
import os.path as op
import signal
import sys

from tornado import ioloop as ti, web as tw, httpserver as ths
from wcpan.logger import setup as setup_logger, INFO

from . import util, api, view
from .controller import RootController


def main(args=None):
    if args is None:
        args = sys.argv

    main_loop = ti.IOLoop.instance()
    main_loop.add_callback(amain, args)
    main_loop.start()
    main_loop.close()
    return 0


async def amain(args):
    args = parse_args(args[1:])

    loggers = setup_logger((
        'tornado.access',
        'tornado.application',
        'tornado.general',
        'requests.packages.urllib3.connectionpool',
        'wcpan.drive.google',
        'wcpan.worker',
        'ddld',
    ), '/tmp/ddld.log')
    logs = util.LogQueue(logging.DEBUG)
    for logger in loggers:
        logger.addHandler(logs)

    main_loop = ti.IOLoop.instance()
    controller = RootController(args.root)

    await controller.initialize()

    async def close_root():
        await controller.close()
        main_loop.stop()
    def close_signal(signum, frame):
        main_loop.add_callback_from_signal(close_root)
    signal.signal(signal.SIGINT, close_signal)

    static_path = op.join(op.dirname(__file__), 'static')
    application = tw.Application([
        (r'/api/v1/nodes', api.NodesHandler),
        (r'/api/v1/nodes/([a-zA-Z0-9\-_]+)', api.NodesHandler),
        (r'/api/v1/cache', api.CacheHandler),
        (r'/api/v1/cache/([a-zA-Z0-9\-_]+)', api.CacheHandler),
        (r'/', view.IndexHandler),
        (r'/api/v1/log', api.LogHandler),
        (r'/api/v1/socket', api.LogSocketHandler),
    ], static_path=static_path, controller=controller, logs=logs)
    server = ths.HTTPServer(application)
    server.listen(args.listen)

    INFO('ddld') << 'ready'

    return 0


def parse_args(args):
    parser = argparse.ArgumentParser('ddld')

    parser.add_argument('-l', '--listen', required=True, type=int)
    parser.add_argument('-r', '--root', required=True, type=str)

    args = parser.parse_args(args)

    return args


exit_code = main()
sys.exit(exit_code)
