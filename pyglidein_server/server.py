"""
Server for pyglidein
"""

import json
import logging

from tornado.web import HTTPError
from rest_tools.server import (RestServer, RestHandler, RestHandlerSetup,
                               from_environment, role_authorization)

from . import __version__ as version
from .condor import CondorCache
from .clients import Clients


logger = logging.getLogger('server')


class BaseHandler(RestHandler):
    def initialize(self, condor, clients, **kwargs):
        super().initialize(**kwargs)
        self.condor = condor
        self.clients = clients


class StatusHandler(BaseHandler):
    async def get(self):
        self.write({
            'condor': self.condor.get_cached(),
            'clients': self.clients.get_all(),
        })


class APITokens(BaseHandler):
    @role_authorization(roles=['admin'])
    async def post(self):
        data = json.loads(self.request.body)
        if (not data) or 'client' not in data:
            raise HTTPError(400, reason='Missing "client" in body')

        token = self.auth.create_token(data['client'], type='client',
                                       payload={'role': 'client'})
        self.write({'client': data['client'], 'token': token})


class APIClient(BaseHandler):
    @role_authorization(roles=['admin', 'client'])
    async def put(self, client):
        if self.auth_data.get('role', None) == 'client' and client != self.auth_data.get('sub', None):
            raise HTTPError(403, reason='Cannot update a different client than your own')

        if self.request.body:
            data = json.loads(self.request.body)
            self.clients.update(client, data)
        else:
            self.clients.update(client, {})

        self.write({})


class APIClientQueue(BaseHandler):
    @role_authorization(roles=['admin', 'client'])
    async def post(self, client):
        if self.auth_data.get('role', None) == 'client' and client != self.auth_data.get('sub', None):
            raise HTTPError(403, reason='Cannot update a different client than your own')

        if self.request.body:
            status = json.loads(self.request.body)
            self.clients.update(client, status)

        try:
            self.clients.get(client)
        except KeyError:
            raise HTTPError(400, reason='Need to provide client queue status')

        ret = self.clients.match(client, self.condor)
        if not ret:
            self.write({})
        else:
            self.write({
                'queues': ret,
                'token': self.condor.get_startd_token()
            })


def create_server():
    # static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    # template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')

    default_config = {
        'HOST': 'localhost',
        'PORT': 8080,
        'DEBUG': False,
        # 'COOKIE_SECRET': binascii.hexlify(b'secret').decode('utf-8'),
        'AUTH_SECRET': '',
        'AUTH_EXPIRATION': -1,  # seconds for token lifetime
        'CONDOR_COLLECTOR': None,
        'CONDOR_CACHE_TIMEOUT': None,
    }
    config = from_environment(default_config)

    rest_cfg = {
        'debug': config['DEBUG'],
        'server_header': f'pyglidein_server {version}',
    }
    if config['AUTH_SECRET']:
        rest_cfg['auth'] = {
            'secret': config['AUTH_SECRET'],
            'issuer': 'pyglidein',
        }
        if config['AUTH_EXPIRATION'] > 0:
            rest_cfg['auth']['expiration'] = config['AUTH_EXPIRATION']
    args = RestHandlerSetup(rest_cfg)

    condor_args = {}
    if config['CONDOR_COLLECTOR']:
        condor_args['collector_address'] = config['CONDOR_COLLECTOR']
    if config['CONDOR_CACHE_TIMEOUT']:
        condor_args['cache_timeout'] = int(config['CONDOR_CACHE_TIMEOUT'])
    args['condor'] = CondorCache(**condor_args)
    args['clients'] = Clients()

    server = RestServer(debug=config['DEBUG'],
                        # static_path=static_path, template_path=template_path,
                        # cookie_secret=config['COOKIE_SECRET'],
                        )

    server.add_route(r'/status', StatusHandler, args)
    server.add_route(r'/api/tokens', APITokens, args)
    server.add_route(r'/api/clients/(?P<client>\w+)', APIClient, args)
    server.add_route(r'/api/clients/(?P<client>\w+)/actions/queue', APIClientQueue, args)

    server.startup(address=config['HOST'], port=config['PORT'])

    return server
