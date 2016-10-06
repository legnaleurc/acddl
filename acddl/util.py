import argparse
import signal
import sys

from tornado import ioloop, web, httpserver
from wcpan.logger import setup as setup_logger, INFO

from . import api
from .controller import RootController


def parse_args(args):
    parser = argparse.ArgumentParser('acddl')

    parser.add_argument('-l', '--listen', required=True, type=int)
    parser.add_argument('-r', '--root', required=True, type=str)

    args = parser.parse_args(args)

    return args


def main(args=None):
    if args is None:
        args = sys.argv

    args = parse_args(args[1:])
    loggers = setup_logger('/tmp/acddl.log', (
        'tornado.access',
        'tornado.application',
        'tornado.general',
        'requests.packages.urllib3.connectionpool',
        'wcpan.worker',
        'acddl',
    ))
    main_loop = ioloop.IOLoop.instance()

    controller = RootController(args.root)
    signal.signal(signal.SIGINT, controller.close)

    application = web.Application([
        (r'/nodes', api.NodesHandler),
        (r'/nodes/([a-zA-Z0-9\-_]{22})', api.NodesHandler),
        (r'/cache', api.CacheHandler),
        (r'/cache/([a-zA-Z0-9\-_]{22})', api.CacheHandler),
    ], controller=controller)
    server = httpserver.HTTPServer(application)
    server.listen(args.listen)

    INFO('acddl') << 'ready'

    main_loop.start()
    main_loop.close()

    return 0
