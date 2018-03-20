import json
from copy import deepcopy
from collections import Counter
from random import sample
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
        self.write_nodes = self.config['write_nodes']
        self.dc_ids = [dc['id'] for dc in self.config['datacenters']]
        self.num_dc = len(self.dc_ids)
        self.num_local = 0
        self.num_remote = 0

    def get_local_dc_list(self, local_node_id):
        return {local_node_id: self.num_local}

    def get_remote_dc_list(self, local_node_id):
        raise NotImplementedError()


class AllInOnePolicy(PlacementPolicy):
    def __init__(self):
        super().__init__()
        self.num_local = self.write_nodes

    def get_remote_dc_list(self, local_node_id):
        return {}


class MajorityInOnePolicy(PlacementPolicy):
    def __init__(self):
        super().__init__()
        divide = lambda n: ceil(n/2) if n % 2 else int(n/2) + 1
        self.num_local = divide(self.write_nodes)
        self.num_remote = self.write_nodes - self.num_local

    def get_remote_dc_list(self, local_node_id):
        dc_ids = deepcopy(self.dc_ids)
        dc_ids.remove(local_node_id)
        num_dc = self.num_dc - 1
        if num_dc >= self.num_remote:
            return Counter(sample(dc_ids, self.num_remote))
        else:
            return Counter(dc_ids * (self.num_remote // num_dc) +
                           dc_ids[:self.num_remote % num_dc])


class UniformDistributionPolicy(PlacementPolicy):
    def __init__(self):
        super().__init__()
        self.num_local = 1
        self.num_remote = self.write_nodes - 1

    def get_remote_dc_list(self, local_node_id):
        dc_ids = deepcopy(self.dc_ids)
        dc_ids.remove(local_node_id)
        num_dc = self.num_dc - 1
        return Counter(dc_ids * (self.num_remote // num_dc) +
                       dc_ids[:self.num_remote % num_dc])
