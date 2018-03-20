import json
from math import ceil


class PlacementFactory(object):
    def __init__(self, policy):
        self.policy = policy

    def get_policy(self):
        if self.policy == "AllInOne":
            return AllInOnePolicy()

        if self.policy == "MajorityinOne":
            return MajorityInOnePolicy()

        if self.policy == "UniformDistribution":
            return UniformDistributionPolicy()

        raise Exception("No placement policy found")


class PlacementPolicy(object):
    """ Parent class for placement policies.
        Abstract class - Do not use directly"""
    def __init__(self):
        self.config = json.load(open('config.json'))
        self.write_nodes = self.config["write_nodes"]
        self.num_local = 0
        self.num_remote = 0


class AllInOnePolicy(PlacementPolicy):
    def __init__(self):
        super().__init__()
        self.num_local = self.write_nodes
        self.num_remote = 0


class MajorityInOnePolicy(PlacementPolicy):
    def __init__(self):
        super().__init__()
        self.divide = lambda n: ceil(n/2) if n%2 else int(n/2) + 1
        self.num_local = self.divide(self.write_nodes)
        self.num_remote = self.write_nodes - self.num_local


class UniformDistributionPolicy(PlacementPolicy):
    def __init__(self):
        super().__init__()
        self.num_local = 1
        self.num_remote = self.write_nodes - 1
