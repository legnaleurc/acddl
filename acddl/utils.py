import argparse
import signal
import sys

from tornado import ioloop, web, httpserver

from . import api
from .log import setup_logger, INFO
from .task import Controller


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
    setup_logger('/tmp/acddl.log')
    main_loop = ioloop.IOLoop.instance()

    controller = Controller(args.root)
    signal.signal(signal.SIGINT, controller.stop)

    application = web.Application([
        (r'/cache', api.CacheHandler),
        (r'/nodes', api.NodesHandler),
    ], controller=controller)
    server = httpserver.HTTPServer(application)
    server.listen(args.listen)

    INFO('acddl') << 'ready'

    main_loop.start()
    main_loop.close()

    return 0
