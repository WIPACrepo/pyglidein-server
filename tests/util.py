import pytest

import htcondor

@pytest.fixture
def condor_bootstrap():
    htcondor.param['SEC_CLAIMTOBE_USER'] = 'submituser'
    coll_query = htcondor.Collector('localhost').locateAll(htcondor.DaemonTypes.Schedd)
    for schedd_ad in coll_query:
        schedd = htcondor.Schedd(schedd_ad)

    schedd.act(htcondor.JobAction.Remove, 'Owner == "submituser"')
    try:
        yield schedd
    finally:
        schedd.act(htcondor.JobAction.Remove, 'Owner == "submituser"')

CONDOR_REQUIRED_ADS = {
    'iwd': '/tmp',
}