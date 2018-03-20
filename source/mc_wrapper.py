from pymemcache.client.base import Client
import threading
import hashlib
from quorum_policy import Quorum
from placement_policy import PlacementFactory


class MCWrapper:
    def __init__(self, properties):
        # Read the config file and setup MC router as per its settings
        self.mc = Client(('127.0.0.1', 5000))
        self.number_of_servers = int(properties["quorum"]["total_nodes"])
        self.read_servers = int(properties["quorum"]["read_nodes"])
        self.write_servers = int(properties["quorum"]["write_nodes"])
        self.dimension = int(properties["quorum"]["dimensions"])
        self.quorum = Quorum(properties["quorum_policy"])
        self.ping_policy = self.quorum.get_ping_policy()
        self.placement_class = PlacementFactory(properties["placement_policy"]).get_policy()

    def put(self,
            key,
            value,
            dimension=None,
            number_of_servers=None,
            write_servers=None):
        # Put the value
        if not number_of_servers:
            number_of_servers = self.number_of_servers
    
        if not write_servers:
            write_servers = self.write_servers

        num_servers_to_ping, num_servers_to_wait = self.ping_policy.fetch_metrics(number_of_servers, write_servers)
        dc_to_ping = self.placement_class(num_servers_to_ping).get_dc()

        # Prefix for the keys to ensure storing them on different servers
        key_prefix = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16) % number_of_servers

        sem = threading.Barrier(num_servers_to_wait + 1, timeout=120)
        thread_list = []

        for i in range(num_servers_to_ping):
            new_key = str(key_prefix) + ":" + key
            thread_list.append(threading.Thread(target=self._put, args=(new_key, value, sem)))
            thread_list[i].start()
            key_prefix = (key_prefix + 1) % number_of_servers

        sem.wait()

        # XXX: Need an implementation to safely kill other threads
        return True

    def _put(self, key, value, sem):
        self.mc.set(key, value)
        sem.wait()

    def get(self,
            key,
            dimension=None,
            number_of_servers=None,
            read_servers=None):
        # Get the value
        if not number_of_servers:
            number_of_servers = self.number_of_servers
        
        if not read_servers:
            read_servers = self.read_servers

        num_servers_to_ping, num_servers_to_wait = self.ping_policy.fetch_metrics(number_of_servers, write_servers)
        dc_to_ping = self.placement_class(num_servers_to_ping).get_dc()

        # Prefix for the keys to ensure storing them on different servers
        key_prefix = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16) % number_of_servers

        sem = threading.Barrier(num_servers_to_wait + 1, timeout=120)

        thread_list = []
        lock = threading.Lock()

        output = []
        for i in range(num_servers_to_ping):
            new_key = str(key_prefix) + ":" + key
            thread_list.append(threading.Thread(target=self._get, args=(new_key, lock, sem, output)))
            key_prefix = (key_prefix + 1) % number_of_servers
        
        sem.wait()

        # XXX: Need an implementation to safely kill other threads
        return output[0]

    def _get(self, key, lock, sem,  output):
        data = self.mc.get(key)
        lock.acquire()
        output.append(data)
        lock.release()
        sem.wait()
