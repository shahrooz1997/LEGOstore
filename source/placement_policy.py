from copy import deepcopy
from collections import Counter
from random import sample
from math import ceil
import hashlib


class PlacementFactory(object):
    def __init__(self, policy):
        self.policy = policy

    def get_policy(self):
        if self.policy == "AllInOne":
            return AllInOnePolicy

        if self.policy == "MajorityinOne":
            return MajorityInOnePolicy

        if self.policy == "UniformDistribution":
            return UniformDistributionPolicy

        raise Exception("No placement policy found")


class PlacementPolicy(object):
    """ Parent class for placement policies.
        Abstract class - Do not use directly"""
    @staticmethod
    def get_dc(write_servers, dc_list, local_dc_id, key):
        raise NotImplementedError()

    @staticmethod
    def get_local_dc(write_servers, dc_list, local_dc_id, key):
        raise NotImplementedError()


    @staticmethod
    def get_remote_dc(write_servers, dc_list, local_dc_id, key):
        raise NotImplementedError()

    @staticmethod
    def get_server_list(server_list, dc_list, key):
        for dc_id, number_of_servers in server_list.items():
            servers_in_dc = len(dc_list[dc_id]["servers"])
            server_id = int(hashlib.sha1(key.encode('utf-8')).hexdigest(),
                            16) % servers_in_dc
            server_list[dc_id] = [str(((server_id + i) % servers_in_dc) + 1) for i in range(0, number_of_servers)]

        return server_list


class AllInOnePolicy(PlacementPolicy):

    @staticmethod
    def get_dc(write_servers, dc_list, local_dc_id, key):
        server_list = {local_dc_id: write_servers}

        return PlacementPolicy.get_server_list(server_list, dc_list, key)


class MajorityInOnePolicy(PlacementPolicy):

    @staticmethod
    def get_dc(write_servers, dc_list, local_dc_id, key):
        divide = lambda n: ceil(n/2) if n % 2 else int(n/2) + 1

        num_local = divide(write_servers)
        num_remote = write_servers - num_local

        new_dc_ids = deepcopy(dc_list)
        new_dc_ids.remove(local_dc_id)

        num_dc = len(dc_list) - 1

        if num_dc >= num_remote:
            server_list = Counter(sample(new_dc_ids, num_remote))
        else:
            server_list =  Counter(new_dc_ids * (num_remote // num_dc) +
                                   new_dc_ids[:num_remote % num_dc])

        server_list[local_dc_id] = num_local

        return super().get_server_list(server_list, dc_list, key)


class UniformDistributionPolicy(PlacementPolicy):

    @staticmethod
    def get_remote_dc(write_servers, dc_list, local_dc_id, key):

        new_dc_ids = deepcopy(dc_list)
        new_dc_ids.remove(local_dc_id)

        num_local = max((write_servers/len(dc_list)), 1)
        num_remote = write_servers - num_local

        num_dc = len(dc_list) - 1

        server_list = Counter(new_dc_ids * (num_remote // num_dc) +
                             new_dc_ids[:num_remote % num_dc])

        server_list[local_dc_id] = num_local

        return super().get_server_list(server_list, dc_list, key)
