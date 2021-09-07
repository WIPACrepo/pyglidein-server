import pytest
import socket
import asyncio

import requests
from rest_tools.client import RestClient
from rest_tools.server import Auth

from pyglidein_server.server import create_server

@pytest.fixture
def port():
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return ephemeral_port

@pytest.fixture
async def server(monkeypatch, port, request):
    marker = request.node.get_closest_marker('role')
    role = marker.args[0] if marker else 'client'

    monkeypatch.setenv('DEBUG', 'True')
    monkeypatch.setenv('PORT', str(port))

    secret = 'secret'
    monkeypatch.setenv('AUTH_SECRET', secret)
    a = Auth(secret, issuer='pyglidein')
    token = a.create_token('user', type='client', payload={'role': role})

    s = create_server()

    try:
        yield RestClient(f'http://localhost:{port}', token=token, timeout=0.1, retries=0)
    finally:
        await s.stop()

@pytest.mark.asyncio
@pytest.mark.role('foo')
async def test_status_noauth(server):
    ret = await server.request('GET', '/status')
    assert 'condor' in ret
    assert 'clients' in ret

@pytest.mark.asyncio
@pytest.mark.role('client')
async def test_tokens_fail(server):
    with pytest.raises(Exception):
        ret = await server.request('POST', '/api/tokens')

@pytest.mark.asyncio
@pytest.mark.role('admin')
async def test_tokens(server):
    ret = await server.request('POST', '/api/tokens', {'client': 'foo'})
    assert 'token' in ret
    assert ret['client'] == 'foo'

@pytest.mark.asyncio
@pytest.mark.role('client')
async def test_put_empty(server):
    ret = await server.request('PUT', '/api/clients/user')

@pytest.mark.asyncio
@pytest.mark.role('client')
async def test_put(server):
    ret = await server.request('PUT', '/api/clients/user', {
        'foo': {
            'resources': {},
            'num_queued': 0,
            'num_processing': 1,
        }
    })

@pytest.mark.asyncio
@pytest.mark.role('client')
async def test_put_fail(server):
    with pytest.raises(Exception):
        ret = await server.request('PUT', '/api/clients/foo')

@pytest.mark.asyncio
@pytest.mark.role('admin')
async def test_put_admin(server):
    ret = await server.request('PUT', '/api/clients/foo')

@pytest.mark.asyncio
@pytest.mark.role('client')
async def test_client_queue(server):
    ret = await server.request('POST', '/api/clients/user/actions/queue', {
        'foo': {
            'resources': {},
            'num_queued': 0,
            'num_processing': 1,
        }
    })
    assert ret == {}

@pytest.mark.asyncio
@pytest.mark.role('client')
async def test_client_queue_fail(server):
    with pytest.raises(Exception):
        await server.request('POST', '/api/clients/foo/actions/queue', {
            'foo': {
                'resources': {},
                'num_queued': 0,
                'num_processing': 1,
            }
        })
