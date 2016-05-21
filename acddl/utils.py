import argparse
import signal
import sys

from tornado import ioloop, web, httpserver

from .log import setup_logger, INFO


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
    log.setup_logger('/tmp/acddl.log')
    main_loop = ioloop.IOLoop.instance()

    controller = Controller(args.root)
    signal.signal(signal.SIGINT, controller.stop)

    application = web.Application([
        (r'/download', api.DownloadHandler),
    ], controller=controller)
    server = httpserver.HTTPServer(application)
    server.listen(args.listen)

    INFO('acddl') << 'ready'

    main_loop.start()
    main_loop.close()

    return 0
