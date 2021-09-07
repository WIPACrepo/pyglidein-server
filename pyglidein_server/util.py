
from tornado.web import HTTPError

class Error(HTTPError):
    def __init__(self, reason):
        super().__init__(400, reason=reason)
