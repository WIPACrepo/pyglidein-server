
from tornado.web import HTTPError
import htcondor

class Error(HTTPError):
    def __init__(self, reason):
        super().__init__(400, reason=reason)


class Resources:
    """
    Resources

        cpu: num cpus
        gpu: num gpus
        memory: in GB
        disk: in GB
        time: in hours
        singularity: true/false if available

    Args:
        resources (dict): resources
        tolerance (float): distance from bin edge before rounding up (default: 1.05 or 5%)
    """
    # rounding bins for resources
    RESOURCE_BINS = {
        'cpu': list(range(1, 1000)),
        'gpu': list(range(0, 100)),
        'memory': [x/10. for x in list(range(5, 50, 5)) + list(range(50, 200, 10)) + list(range(200, 1000, 40)) + list(range(1000, 40000, 100))],
        'disk': list(range(1, 10)) + list(range(10, 50, 4)) + list(range(50, 100, 10)) + list(range(100, 2000, 100)),
        'time': list(range(0, 12)) + list(range(12, 24, 3)) + list(range(24, 72, 12)) + list(range(72, 1000, 48)),
        'singularity': [True, False],
    }
    # defaults
    RESOURCE_DEFAULTS = {
        'cpu': 1,
        'gpu': 0,
        'memory': 1,
        'disk': 1,
        'time': 1,
        'singularity': False,
    }
    DEFAULT_ROUND_TOLERANCE = 1.05

    def __init__(self, resources, tolerance=None):
        self.resources = self.RESOURCE_DEFAULTS.copy()
        self.resources.update(self.round({r: resources[r] for r in resources if resources[r]},
                                         tolerance=tolerance))

    @classmethod
    def from_condor(cls, ads):
        resources = {
            'cpu': ads.get('RequestCPUs', None),
            'gpu': ads.get('RequestGPUs', None),
            'memory': ads['RequestMemory']//1000 if 'RequestMemory' in ads else None,
            'disk': ads['RequestDisk']//1000000 if 'RequestDisk' in ads else None,
            'time': ads['OriginalTime']/3600. if 'OriginalTime' in ads else None,
            'singularity': ads.get('SingularityImage', None),
        }
        return cls(resources)

    @classmethod
    def round(cls, resources, tolerance=None):
        """
        Round resources into bins for matching.

        Discards unknown resource types.

        Args:
            resources (dict): dict of resources to round
            tolerance (float): distance from bin edge before rounding up (default: 1.05 or 5%)

        Returns:
            dict: rounded resources
        """
        if not tolerance:
            tolerance = cls.DEFAULT_ROUND_TOLERANCE

        def round_up(num, bins):
            """Round up to the next bin value"""
            for b in bins:
                if num <= b*tolerance:  # within %
                    return b
            raise Exception('num too big for bin sizes')

        ret = {}
        for k in resources:
            v = resources[k]
            if k in cls.RESOURCE_BINS:
                if k == 'singularity':
                    v = bool(v)
                else:
                    v = round_up(v, cls.RESOURCE_BINS[k])
                ret[k] = v
        return ret

    def __eq__(self, rhs):
        return self.resources == rhs.resources

    def __lt__(self, rhs):
        return self.resources_tuple < rhs.resources_tuple

    def __le__(self, rhs):
        return self.resources_tuple <= rhs.resources_tuple

    def __hash__(self):
        """
        Hash a set of resources
        """
        return hash(self.resources_tuple)

    @property
    def resources_tuple(self):
        return tuple(self.resources[k] for k in Resources.RESOURCE_BINS)

    def mismatch(self, res):
        """
        Compute the mismatch % between this resource and another one.

        Compares resource bins, not absolute values.

        If the current resource is larger, return a number < 1.
        If the current resource is smaller, return a number > 1.

        Args:
            res: a second resource to compare against

        Returns:
            float: how much larger the current resource is

        Raises:
            Exception: if the other resource is larger (would not fit)
        """
        ret = 1.
        for k in Resources.RESOURCE_BINS:
            if k == 'singularity':
                if res.resources['singularity'] and not self.resources['singularity']:
                    raise Exception('other resource does not fit')
            else:
                if res.resources[k] > self.resources[k]:
                    raise Exception('other resource does not fit')
                bin = Resources.RESOURCE_BINS[k]
                ret *= (bin.index(res.resources[k])+1.)/(bin.index(self.resources[k])+1.)
        return ret

