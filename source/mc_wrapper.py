import threading
import hashlib
import socket
import json

from pymemcache.client.base import Client
from quorum_policy import Quorum
from placement_policy import PlacementFactory
from key import Key


class MCWrapper:
    def __init__(self, properties, parent_datacenter):
        # Read the config file and setup MC router as per its settings
        self.mc = Client(('127.0.0.1', 5000))
        self.number_of_servers = int(properties["quorum"]["total_nodes"])
        self.read_servers = int(properties["quorum"]["read_nodes"])
        self.write_servers = int(properties["quorum"]["write_nodes"])
        self.dimension = int(properties["quorum"]["dimensions"])
        self.quorum = Quorum(properties["quorum_policy"])
        self.ping_policy = self.quorum.get_ping_policy()
        self.placement_class = PlacementFactory(properties["placement_policy"]).get_policy()
        self.datacenter_id = None
        self.key_metadata = {}
        self.parent_datacenter = parent_datacenter
        self.dimension = int(properties["dimension"])
        self.history_in_cache = int(properties["history_version_in_cache"])


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

        if not dimension:
            dimension = self.dimension

        num_servers_to_ping, num_servers_to_wait = self.ping_policy.fetch_metrics(number_of_servers,
                                                                                  write_servers)
        dc_to_ping = self.placement_class(num_servers_to_ping).get_dc(self.datacenter_id)

        # Number of local servers to ping
        key_info = Key(dimension)
        local_servers = dc_to_ping[self.datacenter_id]

        # Remove local data ceter from list to have only external data center
        dc_to_ping.pop(self.datacenter_id)

        # Prefix for the keys to ensure storing them on different servers
        server_id = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16) % number_of_servers

        sem = threading.Barrier(num_servers_to_wait + 1, timeout=120)
        thread_list = []

        for i in range(local_servers):
            new_key = str(server_id) + ":" + key
            thread_list.append(threading.Thread(target=self._put, args=(new_key, value, sem, server_id, key_info)))
            thread_list[i].deamon = True
            thread_list[i].start()
            server_id = (server_id + 1) % number_of_servers

        for datacenter_id, replication_number in dc_to_ping.items():
            thread_list.append(threading.Thread(target=self.external_put, args=(datacenter_id,
                                                                                key,
                                                                                [value]*replication_number,
                                                                                sem,
                                                                                key_info)))

        sem.wait()
        self.key_metadata[key] = key_info
        # XXX: Need an implementation to safely kill other threads although since deamon = True we can exit

        return {"received": True}


    def _put(self, key, value, lock, sem, server_id, key_info, connection=None):
        data = self.mc.get(key)

        if data and len(data) == self.history_in_cache:
            # TODO: Storing value on disk in later configuration
            prev_value = data.pop()
            data.append(value)
            self.mc.set(key, [data])
        else:
            self.mc.set(key, [data])

        # XXX: TODO: Not thread safe now , make it.
        # Lock here per key wise since taking a huge lock on key metadata doesn't seem like a good idea
        key_info.update_local_server_list(server_id)

        if connection:
            connection.send(bytes(json.dumps({"received": True})))

        sem.wait()
        return


    def external_put(self, datacenter_id, key, values, sem, key_info):
        # TODO: Update the server
        datacenter = self.parent_datanceter.get_datacenter_by_id(datacenter_id)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((datacenter["host"], datacenter["port"]))
        data = {"method": "internal_put",
                "key": key,
                "values": values,
                "dimension": key_info["dimension"]}

        sock.send(json.dumps(data))

        for i in range(len(values)):
            data = sock.recv(1024)
            data = json.loads(data)
            key_info.update_external_server_list(datacenter_id)
            if data["recieved"]:
                sem.wait()

        return


    def internal_put(self, key, values, dimension, connection):
        sem = threading.Barrier(len(values) + 1, timeout=120)
        server_id = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16) % self.number_of_servers

        key_info = Key(dimension)

        thread_list = []
        for value in values:
            new_key = str(server_id) + ":" + key
            thread_list.append(threading.Thread(target=self._put, args=(new_key, value, sem, server_id, key_info, connection)))
            thread_list[-1].deamon = True
            thread_list[-1].start()
            server_id = (server_id + 1) % self.number_of_servers

        sem.wait()
        return


    def external_get(self, datacenter, server_id, key, lock, sem, output):
        # External get call make a call to other server and get the data
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((datacenter["host"], datacenter["port"]))

        data = {"method": "internal_get",
                "key": str(server_id) + ":" + key}

        sock.send(json.dumps(data))

        out = sock.recv(1024)
        out = json.loads(out)

        if out["found"]:
            lock.acquire()
            output.append(out["result"])
            lock.release()
            sem.wait()
        else:
            print(f"No data found on the required server {server_id} of datacenter {datacenter[id]}")

        return


    def internal_get(self, key, connection):
        data = self.mc.get(key)
        if data:
            output = {"found": True,
                      "result": data}

        else:
            output = {"found": False,
                      "result": None}

        connection.send(json.dumps(output))
        return


    def _get(self, key, lock, sem,  output):
        data = self.mc.get(key)
        lock.acquire()
        output.append(data)
        lock.release()
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

        num_servers_to_ping, num_servers_to_wait = self.ping_policy.fetch_metrics(number_of_servers, read_servers)

        if key not in self.key_metadata:
            return {"output": "Key: Not Found"}

        # Prefix for the keys to ensure storing them on different servers
        key_info = self.key_metadata[key]

        local_list = key_info.get_local_server_list()

        sem = threading.Barrier(num_servers_to_wait + 1, timeout=120)

        count = 0
        thread_list = []
        lock = threading.Lock()

        output = []

        for server_id in local_list:
            if count > num_servers_to_wait:
                break

            new_key = str(server_id) + ":" + key
            thread_list.append(threading.Thread(target=self._get, args=(new_key, lock, sem, output)))
            server_id = (server_id + 1) % number_of_servers
            count += 1

        # Local has high priority
        # we will always check if the nodes are available locally before sending external request
        if count > num_servers_to_ping:
            sem.wait()
            return {"output": output}

        dc_list = key_info.get_external_server_list()

        for dc_id, server_list in dc_list.items():
            # TODO: Only send to the required servers not to all
            datacenter = self.parent_datacenter.get_data_center_with_id(dc_id)
            for server_id in server_list:
                if count > num_servers_to_wait:
                    break

                thread_list.append(threading.Thread(target=self.external_get, args=(datacenter,
                                                                                    server_id,
                                                                                    key,
                                                                                    lock,
                                                                                    sem,
                                                                                    output)))
            count += len(server_list)



        sem.wait()
        # XXX: Need an implementation to safely kill other threads
        return {"output": output}