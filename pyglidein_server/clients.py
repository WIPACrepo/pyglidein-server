import math
import logging

from .resources import Resources
from .util import Error

logger = logging.getLogger(__name__)


class Clients:
    """
    Stores client (site) information.

    Each client may have N resource queues. Lookups are by `Resources`,
    specifically separating queued and processing resources.
    """
    def __init__(self):
        self.data = {}

    def update(self, name, queues):
        """
        Update a client.

        Valid queues are a dict of queue information, including
        `resources`, `num_queued`, and `num_processing` for each client queue.
        The keys are used as references, since the resources may be binned
        and alter slightly.

        Args:
            name (str): name of client
            queues (dict): queue information
        """
        if not isinstance(queues, dict):
            raise Error('client data must be a dict of queue statuses')

        ret = {}
        for ref in queues:
            queue = queues[ref]
            # validate
            if set(queue.keys()) != {'resources', 'num_queued', 'num_processing'}:
                raise Error('client data must have keys: resources, num_queued, num_processing')
            if set(queue['resources']) - set(Resources.RESOURCE_DEFAULTS):
                raise Error(f'client data resources must be: {set(Resources.RESOURCE_DEFAULTS)}')

            # set resources
            ret[Resources(queue['resources'], tolerance=1)] = {
                'ref': ref,
                'num_queued': queue['num_queued'],
                'num_processing': queue['num_processing'],
            }

        # do update
        self.data[name] = ret

    def get(self, name):
        """Get client data"""
        return self.data[name]

    def match(self, name, condor_queue):
        """
        Perform matching for a client.

        This matches a client against the condor queue and other clients,
        to determine if it should submit more glideins on any of its queues.

        Args:
            name (str): name of client
            condor_queue (CondorCache): condor queue

        Returns:
            dict: name of queue and number of jobs to submit
        """
        condor_jobs = condor_queue.get()

        ret = {}
        for res in self.data[name]:
            queue = self.data[name][res]

            jobs_queued = 0.
            jobs_processing = 0.
            for r in condor_jobs:
                if r <= res:
                    mismatch = res.mismatch(r)
                    jobs_queued += mismatch * condor_jobs[r]['_sum']['queued']
                    jobs_processing += mismatch * condor_jobs[r]['_sum']['processing']

            if jobs_processing > 0:
                job_ratio = jobs_processing / (jobs_processing + jobs_queued)
            else:
                job_ratio = 1.

            logger.debug(f'jobs_queued: {jobs_queued}')
            logger.debug(f'jobs_processing: {jobs_processing}')
            logger.debug(f'job_ratio: {job_ratio}')

            glideins_queued = 0.
            glideins_processing = 0.
            for site in self.data:
                for r in self.data[site]:
                    if r <= res:
                        mismatch = res.mismatch(r)
                        glideins_queued += mismatch * self.data[site][r]['num_queued']
                        glideins_processing += mismatch * self.data[site][r]['num_processing']

            if glideins_processing > 0:
                glidein_util = glideins_processing / (glideins_processing + glideins_queued)
            else:
                glidein_util = 1.

            logger.debug(f'glideins_queued: {glideins_queued}')
            logger.debug(f'glideins_processing: {glideins_processing}')
            logger.debug(f'glidein_util: {glidein_util}')

            global_queue = (jobs_queued - glideins_queued) * math.pow(job_ratio, 1/4) * math.pow(glidein_util, 2)

            logger.debug(f'global_queue: {global_queue}')

            local_queue = max(global_queue - queue['num_queued'], 0)
            #if glideins_queued > 0:
            #    local_queue /= glideins_queued

            logger.debug(f'local_queue: {local_queue}')

            if local_queue > 0:
                ret[queue['ref']] = math.ceil(local_queue)

        return ret
