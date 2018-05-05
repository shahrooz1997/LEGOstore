from quorum_policy import Quorum
from lamport_timestamp import TimeStamp

import hashlib
import threading
import json
import socket
import copy
from placement_policy import PlacementFactory
from protocol_interface import ProtocolInterface

# TODO: INCOPERATE THE CLASS FOR EACH CALL AS DATASERVER WILL BE NEEDING IT
class ABD(ProtocolInterface):

    def __init__(self, properties, local_datacenter_id, data_center, client_id):

        self.timeout_per_request = int(properties["timeout_per_request"])

        # Quorum relevant properties
        self.total_nodes  = int(properties["quorum"]["total_nodes"])
        self.read_nodes = int(properties["quorum"]["read_nodes"])
        self.write_nodes = int(properties["quorum"]["write_nodes"])

        # Generic timeout for everything
        self.timeout = int(properties["timeout_per_request"])
        self.data_center = data_center
        self.id = client_id
        self.placement_policy = PlacementFactory(properties["placement_policy"]).get_policy()
        self.local_datacenter_id = local_datacenter_id
        self.current_class = "ABD"

        # TODO: For now only implementing ping all and wait for read or write quorum to respond
        # self.ping_policy = Quorum(properties["ping_policy"]).get_ping_policy()
        # self.num_nodes_to_ping_for_write, self.num_nodes_to_wait_for_write = \
        #     self.ping_policy.fetchx_metrics(self.total_nodes, self.write_nodes)
        #
        # self.num_nodes_to_ping_for_read, self.num_nodes_to_wait_for_read = \
        #     self.ping_policy.fetchx_metrics(self.total_nodes, self.read_nodes)



    def _get_timestamp(self, key, sem, server, output, lock):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))

        data = {"method": "get_timestamp",
                "key": key,
                "class": self.current_class}

        sock.sendall(json.dumps(data).encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except:
            print("Server with host: {1} and port: {2} timeout for getTimestamp in ABD", (server["host"],
                                                                                          server["port"]))
        else:
            data = json.loads(data.decode("utf-8"))
            print("data is here:")
            print(data)
            lock.acquire()
            output.append(data["timestamp"])
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def get_timestamp(self, key, server_list):
        ######################
        # Call for get timestamp
        # @ Returns the next for the call
        # @ Raises Exception in case of timeout or socket error
        ######################

        # Step 1 : getting the timestamp
        sem = threading.Barrier(self.read_nodes + 1, timeout=self.timeout)
        lock = threading.Lock()
        thread_list = []

        output = []
        for data_center_id, servers in server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get_timestamp, args=(key,
                                                                                      sem,
                                                                                      copy.deepcopy(server_info),
                                                                                      output,
                                                                                      lock)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        # Removing barrier for all the waiting threads
        sem.abort()

        lock.acquire()
        if (len(output) < self.read_nodes):
            lock.release()
            raise Exception("Timeout during timestamps")

        time = TimeStamp.get_next_timestamp(output, self.id)
        lock.release()
        return time


    def get_final_value(self, values):
        result, max_time = values[0]["value"]
        for data in values:
            temp_result, temp_time = data["value"]
            if temp_time > max_time:
                result = temp_result
                max_time = temp_time

        return (result, max_time)


    def _put(self, key, value, timestamp, sem, server, output, lock):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))
        print(server)

        data = json.dumps({"method": "put",
                           "key": key,
                           "value": value,
                           "timestamp": timestamp,
                           "class": self.current_class})

        sock.sendall(data.encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except:
            print("Server with host: {1} and port: {2} timeout for put request in ABD", (server["host"],
                                                                                         server["port"]))
        else:
            print(data)
            data = json.loads(data.decode("utf-8"))
            lock.acquire()
            output.append(data)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def put(self, key, value, server_list=None, insert=False):
        ######################
        # API call for ABD Put:
        # @ Returns the dict with {
        #   "server_list" : if new insert,
        #   "timestamp" : timestampt used for insert,
        #   "message" (optional) : In case of failure
        #   "status": OK / FAILED }
        #
        # @ Raises Exception in case of timeout or socket error
        ######################

        thread_list = []

        # If INSERT that means first time else Step1 i.e. get_timestamp from servers with the key
        if insert:
            timestamp = TimeStamp.get_current_time(self.id)
            server_list = self.placement_policy.get_dc(self.total_nodes,
                                                       self.data_center.get_datacenter_list(),
                                                       self.local_datacenter_id,
                                                       key)
        else:
            try:
                timestamp = self.get_timestamp(key, server_list)
            except:
                return {"status": "TimeOut", "message": "Timeout during get timestamp call of ABD"}

        # Step2 : Send the message to put
        sem = threading.Barrier(self.write_nodes + 1, timeout=self.timeout)
        lock = threading.Lock()

        output = []

        for data_center_id, servers in server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put, args=(key,
                                                                            value,
                                                                            timestamp,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            lock)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        # Removing barrier for all the waiting threads
        sem.abort()

        lock.acquire()
        if (len(output) < self.write_nodes):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout during put call of ABD"}

        lock.release()

        return {"status": "OK", "value": value, "timestamp": timestamp, "server_list": server_list}


    def _get(self, key, sem, server, output, lock):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))

        data = {"method": "get",
                "key": key,
		"timestamp": None,
                "class": self.current_class}

        sock.sendall(json.dumps(data).encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except:
            print("Server with host: {1} and port: {2} timeout for get request in ABD", (server["host"],
                                                                                         server["port"]))
        else:
            data = json.loads(data.decode("utf-8"))
            lock.acquire()
            output.append(data)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def get(self, key, server_list):
        ######################
        # API call for ABD Get:
        # @ Returns the dict with {
        #   "status": OK / FAILED.
        #   "message" (optional) : In case of failure
        #   "value" : Returned value
        # }
        #
        # @ Raises Exception in case of timeout or socket error
        ######################

        thread_list = []

        sem = threading.Barrier(self.read_nodes + 1, timeout=self.timeout)
        lock = threading.Lock()

        # Step1: get the timestamp with recent value
        output = []
        for data_center_id, servers in server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get, args=(key,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            lock)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        sem.abort()

        lock.acquire()
        if (len(output) < self.read_nodes):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout during get call of ABD"}

        value, timestamp = self.get_final_value(output)

        lock.release()

        # After abort can't use the same barrier
        sem = threading.Barrier(self.write_nodes + 1, timeout=self.timeout)
        result = []
        for data_center_id, servers in server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put, args=(key,
                                                                            value,
                                                                            timestamp,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            result,
                                                                            lock)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        sem.abort()

        return {"status": "OK", "value": value}
