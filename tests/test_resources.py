import pytest

from pyglidein_server.resources import Resources

def test_resources_no_ads():
    Resources({})

def test_resources_condor_cpu():
    r = Resources.from_condor({'RequestCPUs': 2})
    assert r.resources['cpu'] == 2

def test_resources_condor_gpu():
    r = Resources.from_condor({'RequestGPUs': 2})
    assert r.resources['gpu'] == 2

def test_resources_condor_memory():
    r = Resources.from_condor({'RequestMemory': 2000})
    assert r.resources['memory'] == 2

def test_resources_condor_disk():
    r = Resources.from_condor({'RequestDisk': 2000000})
    assert r.resources['disk'] == 2

def test_resources_condor_time():
    r = Resources.from_condor({'OriginalTime': 7200})
    assert r.resources['time'] == 2

def test_resources_condor_singularity():
    r = Resources.from_condor({'SingularityImage': True})
    assert r.resources['singularity'] == True

def test_resources_time_binning():
    r1 = Resources({'time': Resources.RESOURCE_BINS['time'][3]})
    r2 = Resources({'time': Resources.RESOURCE_BINS['time'][3]+0.01})
    assert r1 == r2

def test_resources_time_tolerance():
    r1 = Resources({'time': Resources.RESOURCE_BINS['time'][3]}, tolerance=1)
    r2 = Resources({'time': Resources.RESOURCE_BINS['time'][3]+0.01}, tolerance=1)
    assert r1 != r2

def test_resources_sort():
    r1 = Resources({})
    r2 = Resources({'cpu': 2})
    ret = sorted([r1, r2])

    assert ret[0] == r1
    assert ret[1] == r2

def test_resources_sort_reverse():
    r1 = Resources({})
    r2 = Resources({'cpu': 2})
    ret = sorted([r1, r2], reverse=True)

    assert ret[0] == r2
    assert ret[1] == r1

def test_resources_sort_complex():
    res = [
        Resources({'cpu': 1, 'memory': 2}),
        Resources({'cpu': 2, 'memory': 4}),
        Resources({'cpu': 2, 'memory': 1}),
        Resources({'cpu': 1, 'memory': 1.5}),
        Resources({'cpu': 1, 'memory': 3}),
    ]
    ret = sorted(res)

    assert ret[0] == res[3]
    assert ret[-1] == res[1]

def test_resources_mismatch():
    r1 = Resources({'cpu': 2})
    r2 = Resources({'cpu': 1})

    assert r1.mismatch(r2) == 0.5

def test_resources_mismatch_complex():
    r1 = Resources({'cpu': 2, 'memory': 8, 'time': 10})
    r2 = Resources({'cpu': 1, 'memory': 6, 'time': 8})

    assert .2 < r1.mismatch(r2) < .4

def test_resources_mismatch_misfit():
    r1 = Resources({'cpu': 2})
    r2 = Resources({'cpu': 1})

    with pytest.raises(Exception):
        r2.mismatch(r1)
