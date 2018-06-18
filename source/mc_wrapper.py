import threading
import hashlib
import socket
import json

from pymemcache.client.base import Client
from adb_protocol import ABD


class MCWrapper:
    def __init__(self, properties, parent_datacenter):
        # Read the config file and setup MC router as per its settings
        self.mc = Client(('127.0.0.1', 5000))
        # Initially everyone belongs to default properties
        self.properties = properties
        properties_ = self.properties["classes"]["default"]
        self.number_of_servers = int(properties_["quorum"]["total_nodes"])
        self.datacenter_id = None
        self.key_metadata = {}
        self.parent_datacenter = parent_datacenter
        self.class_name_to_object = {}

        self.initiate_key_classes()


    def initiate_key_classes(self):
        for class_name, value in self.properties:
            if class_name == "ABD":
                self.class_name_to_object[class_name] = ABD(self.properties["classes"]["ABD"],
                                                            self.number_of_servers,
                                                            self.parent_datacenter,
                                                            self.mc,
                                                            self.key_metadata)

            # Add more class intiations here

        return


    def get_class_locally(self, key, connection=None):
        # TODO: Check what needs to be done for number_of_servers
        # TODO: For now implemented that pinging one server for key could be multiple in future
        server_id = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16) % self.number_of_servers
        new_key = key + ":" + str(server_id)

        data = self.mc.get(new_key)
        if not data:
            if connection:
                connection.send(bytes(json.dumps({"key_class": None})))

            return None

        # Split data into class, server_list
        _, key_class, server_list = data.split(":")

        # Result to return
        result = {}
        result["key_class"] = key_class
        result["server_list"] = json.loads(server_list)

        if connection:
            connection.send(bytes(json.dumps(result)))
            return

        return result


    def _get_class_globally(self, key, data_center, lock, count, output):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((data_center["host"], data_center["port"]))

        data = {"method": "get_class", "key": key}
        sock.send(json.dumps(data))

        out = sock.recv(1024)
        out = json.loads(out)


        if not out["key_class"]:
            lock.acquire()
            count += 1
            lock.release()
            return

        lock.acquire()
        output.append(out["key_class"])
        lock.release()

        return


    def get_class_globally(self, key):
        thread_list = []
        lock = threading.Lock()
        count = 0
        output = []
        data_center_list  = self.parent_datacenter.get_data_center_list()
        for data_center in data_center_list:
            if data_center["host"] == "localhost":
                continue

            thread_list.append(threading.Thread(target=self._get_class_globally, args=(key,
                                                                                       data_center,
                                                                                       lock,
                                                                                       count,
                                                                                       output)))
            thread_list[-1].deamon = True
            thread_list[-1].start()

        # Wait till either all threads respond with no data or atleast one of them respond with data
        while count < len(data_center_list) and len(output) == 0:
            continue

        result = None
        if output:
            lock.acquire()
            result = output[-1]
            lock.release()

        return result


    def get_class_details_from_key(self, key, first):
        # We may want to implement it like which key belong to which class here
        # Client indicated that key is called first time
        if first:
            return self.properties["classes"]["default"]
        # Key is present in global key_metadata
        elif key in self.key_metadata:
            return self.key_metadata["key"]
        # Try to get key class locally from metadata
        else:
            data = self.get_class_locally(key)
            if data["key_class"]:
                return data

        # Try to get key class from other data centers
        data = self.get_class_globally(key)

        if data["key_class"]:
            return data

        data = {}
        data["key_class"] = "default"
        data["server_list"] = None

        return data



    def put(self, key, value, first = False):
        # If client has prior information that it is an insert,
        # then first = True should be marked to avoid unncessary round trips.

        # Step1: Get the class details about the key
        data = self.get_class_details_from_key(key, first)
        class_info = data["key_class"]
        server_list = data["server_list"]

        # Step2 : Call the appropriate class based on the class name
        return self.class_name_to_object[class_info].put(key, value, server_list)


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


    def internal_put(self, key, values, key_info, connection):
        sem = threading.Barrier(len(values) + 1, timeout=120)
        server_id = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16) % self.number_of_servers

        thread_list = []
        protocol = self.class_name_to_object[key_info["key_class"]]
        for value in values:
            new_key = str(server_id) + ":" + key
            thread_list.append(threading.Thread(target=protocol._put, args=(new_key,
                                                                            value,
                                                                            sem,
                                                                            key_info,
                                                                            connection)))

            thread_list[-1].deamon = True
            thread_list[-1].start()
            server_id = (server_id + 1) % self.number_of_servers

        self.key_metadata[key] = key_info

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


    def get(self, key):
        # Get the value
        number_of_servers = self.number_of_servers
        read_servers = self.read_servers

        num_servers_to_ping, num_servers_to_wait = self.ping_policy.fetch_metrics(number_of_servers, read_servers)

        if key not in self.key_metadata:
            # TODO: Implement the fetch key from other servers
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
