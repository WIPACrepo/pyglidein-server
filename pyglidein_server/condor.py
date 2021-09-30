from collections import defaultdict
from copy import deepcopy
from functools import partial
import subprocess
import time

import htcondor
# import classad

from .resources import Resources


class CondorCache:
    CONDOR_CLASSADS = [
        'JobStatus', 'SingularityImage',
        'RequestCPUs', 'RequestGPUs', 'RequestMemory',
        'RequestDisk', 'OriginalTime',
        'MachineAttrGLIDEIN_Site0', 'MachineAttrGLIDEIN_ResourceName0',
    ]

    def __init__(self, collector_address='localhost', cache_timeout=60):
        self.collector_address = collector_address
        self.cache = {}
        self.cache_age = -1
        self.cache_timeout = cache_timeout

        self._refresh_cache()

    @classmethod
    def convert_classads(cls, ads):
        ret = {}
        for k in ads.keys():
            try:
                ret[k] = ads.eval(k)
            except TypeError:
                ret[k] = ads[k]
        return ret

    def _refresh_cache(self):
        """Ask HTcondor about the jobs on the queue"""
        queries = []
        coll_query = htcondor.Collector(self.collector_address).locateAll(htcondor.DaemonTypes.Schedd)
        for schedd_ad in coll_query:
            schedd_obj = htcondor.Schedd(schedd_ad)
            queries.append(schedd_obj.xquery(projection=CondorCache.CONDOR_CLASSADS))

        job_counts = defaultdict(JobCounts)
        for query in htcondor.poll(queries):
            jobs = query.nextAdsNonBlocking()
            for job in jobs:
                ads = self.convert_classads(job)

                site = ads.get('MachineAttrGLIDEIN_Site0', None)
                resource = ads.get('MachineAttrGLIDEIN_ResourceName0', None)
                status = htcondor.JobStatus(ads.get('JobStatus', 1))
                if status == htcondor.JobStatus.IDLE:
                    status = 'queued'
                elif status == htcondor.JobStatus.RUNNING:
                    status = 'processing'
                else:
                    status = 'unknown'

                res = Resources.from_condor(ads)
                job_counts[res][site][resource][status] += 1
                job_counts[res]['_sum'][status] += 1

        self.cache = job_counts
        self.cache_age = time.time()

    def get(self):
        if self.cache_age + self.cache_timeout < time.time():
            self._refresh_cache()

        return self.get_cached()

    def get_cached(self):
        return deepcopy(self.cache)

    def get_json(self):
        """Get json version of job cache"""
        ret = {}
        for res in self.cache:
            key = hash(res)
            ret[key] = deepcopy(self.cache[res])
            ret[key]['_resources'] = res.resources
        return ret

    def get_startd_token(self):
        """Get an HTCondor auth token"""
        # currently, the pybindings cannot create a token. so run manually
        cmd = ['condor_token_fetch', '-authz', 'READ', '-authz', 'WRITE', '-authz', 'ADVERTISE_STARTD', '-authz', 'ADVERTISE_MASTER', '-pool', self.collector_address, '-type', 'COLLECTOR']
        out = subprocess.check_output(cmd)
        return out.strip()


class JobCounts(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['_sum'] = defaultdict(int)

    def __iter__(self):
        return iter(self.keys())

    def keys(self):
        return (k for k in super().keys() if k != '_sum')

    def values(self):
        return (self[k] for k in super().keys() if k != '_sum')

    def __missing__(self, key):
        v = defaultdict(partial(defaultdict, int))
        self[key] = v
        return v
