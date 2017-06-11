from tornado import web as tw

from .templates import index_html


class IndexHandler(tw.RequestHandler):

    def get(self):
        self.render(index_html)
