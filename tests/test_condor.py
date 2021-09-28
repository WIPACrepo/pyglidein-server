import time
import htcondor
import subprocess

from pyglidein_server import condor
import pytest

from .util import condor_bootstrap, CONDOR_REQUIRED_ADS


def submit_job(schedd, **ads):
    ads.update({
        'executable': '/bin/sleep',
        'arguments': '2',
    })
    ads.update(CONDOR_REQUIRED_ADS)
    sub = htcondor.Submit(ads)
    with schedd.transaction() as txn:
        sub.queue(txn, 1)


def test_job_counts():
    jc = condor.JobCounts()
    assert list(jc) == []
    assert list(jc.keys()) == []
    assert list(jc.values()) == []
    assert jc['_sum']['queued'] == 0
    jc['foo']['bar']['baz'] = 1
    assert jc['foo']['bar']['baz'] == 1
    jc['foo']['bar']['foo'] += 2
    assert jc['foo']['bar']['foo'] == 2

def test_empty_pool(condor_bootstrap):
    cc = condor.CondorCache()
    assert cc.get_cached() == {}

def test_pool_submit_before_cache(condor_bootstrap):
    submit_job(condor_bootstrap, Requirements='Machine =?= "foo"')
    cc = condor.CondorCache()
    cache = cc.get_cached()
    assert cache != {}
    assert len(cache) == 1
    assert list(cache.values())[0]['_sum']['queued'] == 1

def test_pool_submit_after_cache(condor_bootstrap):
    cc = condor.CondorCache(cache_timeout=1)
    submit_job(condor_bootstrap, Requirements='Machine =?= "foo"')
    assert cc.cache == {}
    time.sleep(1)
    cache = cc.get()
    assert cache != {}
    assert len(cache) == 1
    assert list(cache.values())[0]['_sum']['queued'] == 1

def test_pool_submit_job_lifecycle(condor_bootstrap):
    submit_job(condor_bootstrap)
    cc = condor.CondorCache(cache_timeout=.1)

    for _ in range(60):
        cache = cc.get()
        if list(cache.values())[0]['_sum']['queued'] == 0:
            break
        time.sleep(.2)
    else:
        raise Exception('job queued for over 1 minute')

    for _ in range(60):
        cache = cc.get()
        jobs = list(cache.values())
        if (not jobs) or jobs[0]['_sum']['processing'] == 0:
            break
        time.sleep(.2)
    else:
        raise Exception('job processing for over 1 minute')

    time.sleep(.1)
    cache = cc.get()
    jobs = list(cache.values())
    assert (not jobs) or jobs[0]['_sum']['processing'] == 0

    jobs = list(condor_bootstrap.history(constraint='true',
                                         projection=['JobStatus','ExitCode'],
                                         match=1))
    assert jobs
    job = jobs[0]
    assert htcondor.JobStatus(job['JobStatus']) == htcondor.JobStatus.COMPLETED
    assert job['ExitCode'] == 0


def htcondor_installed():
    try:
        subprocess.check_call(['which', 'condor_token_fetch'])
    except subprocess.CalledProcessError:
        return True
    return False

@pytest.mark.skipif(htcondor_installed(), reason="requires HTCondor binaries to be installed")
def test_get_startd_token(condor_bootstrap):
    cc = condor.CondorCache()
    cc.get_startd_token()
