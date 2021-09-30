from collections import defaultdict
import json

import pytest

from pyglidein_server import clients, resources
from pyglidein_server.condor import JobCounts
from pyglidein_server.util import Error


def test_clients_init():
    clients.Clients()

def test_clients_update_single():
    queues = {
        'foo': {
            'resources': {},
            'num_processing': 10,
            'num_queued': 0,
        }
    }

    cl = clients.Clients()
    cl.update('foo', queues)

    data = cl.get('foo')
    assert len(data) == 1

def test_clients_update_multi():
    queues = {
        'bar': {
            'resources': {'memory': 2},
            'num_processing': 10,
            'num_queued': 11,
        },
        'baz': {
            'resources': {'memory': 4},
            'num_processing': 12,
            'num_queued': 13,
        }
    }

    cl = clients.Clients()
    cl.update('foo', queues)

    data = cl.get('foo')
    assert len(data) == 2

def test_clients_bad_resource():
    queues = {
        'foo': {
            'resources': {'foo': 1},
            'num_processing': 10,
            'num_queued': 0,
        }
    }

    cl = clients.Clients()
    with pytest.raises(Error) as exc_info:
        cl.update('foo', queues)
    assert 'resources' in exc_info.value.reason

def test_get_json():
    queues = {
        'foo': {
            'resources': {},
            'num_processing': 10,
            'num_queued': 0,
        }
    }

    cl = clients.Clients()
    cl.update('foo', queues)

    data = cl.get_json()
    assert len(data) == 1
    values = list(data.values())
    assert len(values) == 1
    json.dumps(data)


#######  Matching tests  #######

testdata = [
    # single site, single resource
    ({'site': {'q1': {'resources': {}, 'num_processing': 0, 'num_queued': 0}}},
     [{'resources': {}, 'processing': 0, 'queued': 1}],
     'site',
     {'q1': 1}),
    ({'site': {'q1': {'resources': {}, 'num_processing': 5, 'num_queued': 0}}},
     [{'resources': {}, 'processing': 5, 'queued': 10}],
     'site',
     {'q1': 8}),
    ({'site': {'q1': {'resources': {}, 'num_processing': 5, 'num_queued': 1}}},
     [{'resources': {}, 'processing': 5, 'queued': 10}],
     'site',
     {'q1': 4}),
    ({'site': {'q1': {'resources': {}, 'num_processing': 5, 'num_queued': 2}}},
     [{'resources': {}, 'processing': 5, 'queued': 10}],
     'site',
     {'q1': 2}),
    ({'site': {'q1': {'resources': {}, 'num_processing': 5, 'num_queued': 3}}},
     [{'resources': {}, 'processing': 5, 'queued': 10}],
     'site',
     {}),
    ({'site': {'q1': {'resources': {}, 'num_processing': 50, 'num_queued': 20}}},
     [{'resources': {}, 'processing': 50, 'queued': 100}],
     'site',
     {'q1': 12}),
    # single site, jobs of different sizes
    ({'site': {'q1': {'resources': {'memory':2}, 'num_processing': 50, 'num_queued': 20}}},
     [{'resources': {}, 'processing': 25, 'queued': 50},
      {'resources': {'memory':2}, 'processing': 25, 'queued': 50},
     ],
     'site',
     {'q1': 2}),
    ({'site': {'q1': {'resources': {'memory':2}, 'num_processing': 50, 'num_queued': 10}}},
     [{'resources': {}, 'processing': 45, 'queued': 90},
      {'resources': {'memory':2}, 'processing': 5, 'queued': 10},
     ],
     'site',
     {'q1': 14}),
    # multi-site
    ({'site': {'q1': {'resources': {'memory':2}, 'num_processing': 20, 'num_queued': 10}},
      'site2': {'q2': {'resources': {}, 'num_processing': 30, 'num_queued': 20}},
     },
     [{'resources': {}, 'processing': 45, 'queued': 90},
      {'resources': {'memory':2}, 'processing': 5, 'queued': 10},
     ],
     'site',
     {'q1': 1}),
    ({'site': {'q1': {'resources': {'memory':2}, 'num_processing': 20, 'num_queued': 10}},
      'site2': {'q2': {'resources': {}, 'num_processing': 30, 'num_queued': 20}},
     },
     [{'resources': {}, 'processing': 45, 'queued': 90},
      {'resources': {'memory':2}, 'processing': 5, 'queued': 10},
     ],
     'site2',
     {}),
    ({'site': {'q1': {'resources': {'memory':2}, 'num_processing': 20, 'num_queued': 10}},
      'site2': {'q2': {'resources': {}, 'num_processing': 30, 'num_queued': 20}},
     },
     [{'resources': {}, 'processing': 45, 'queued': 500},
      {'resources': {'memory':2}, 'processing': 5, 'queued': 10},
     ],
     'site',
     {'q1': 45}),
    ({'site': {'q1': {'resources': {'memory':2}, 'num_processing': 20, 'num_queued': 10}},
      'site2': {'q2': {'resources': {}, 'num_processing': 30, 'num_queued': 20}},
     },
     [{'resources': {}, 'processing': 45, 'queued': 500},
      {'resources': {'memory':2}, 'processing': 5, 'queued': 10},
     ],
     'site2',
     {'q2': 73}),
    # larger resources
    ({'site': {'q1': {'resources': {'memory':1}, 'num_processing': 20, 'num_queued': 10}}},
     [{'resources': {}, 'processing': 50, 'queued': 10},
      {'resources': {'memory':2}, 'processing': 0, 'queued': 1000},
     ],
     'site',
     {}),
]

class FakeCondor:
    def __init__(self, data):
        self.data = defaultdict(JobCounts)
        for d in data:
            res = resources.Resources(d['resources'])
            self.data[res]['_sum']['processing'] += d['processing']
            self.data[res]['_sum']['queued'] += d['queued']
    def get(self):
        return self.data

@pytest.mark.parametrize('glideins,condor,name,expected', testdata)
def test_clients_match(glideins, condor, name, expected):
    cl = clients.Clients()
    for site in glideins:
        cl.update(site, glideins[site])

    ret = cl.match(name, FakeCondor(condor))
    assert ret == expected
