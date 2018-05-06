from lamport_timestamp import TimeStamp

import hashlib
import threading
import json
import socket
import copy
from placement_policy import PlacementFactory
from protocol_interface import ProtocolInterface
from pyeclib.ec_iface import ECDriver


class Viveck_1(ProtocolInterface):

    def __init__(self, properties, local_datacenter_id, data_center, client_id):
        self.timeout_per_request = int(properties["timeout_per_request"])

        ##########################
        ## Quorum relevant properties
        ## Q1 + Q3 > N
        ## Q1 + Q4 > N
        ## Q2 + Q4 >= N+k
        ## Q4 >= k
        ##########################
        self.total_nodes  = int(properties["quorum"]["total_nodes"])
        self.quorum_1  = int(properties["quorum"]["Q1"])
        self.quorum_2 = int(properties["quorum"]["Q2"])
        self.quorum_3 = int(properties["quorum"]["Q3"])
        self.quorum_4 = int(properties["quorum"]["Q4"])

        # Erasure coding parameters
        self.k = int(properties["k"])
        self.m = int(properties["m"])
        # https://github.com/openstack/pyeclib . Check out the supported types.
        self.ec_type = properties["erasure_coding_type"]

        self.ec_driver = ECDriver(k=self.k, m=self.m, ec_type=self.ec_type)


        # Generic timeout for everything
        self.timeout = int(properties["timeout_per_request"])
        self.data_center = data_center
        self.id = client_id
        self.placement_policy = PlacementFactory(properties["placement_policy"]).get_policy()
        self.local_datacenter_id = local_datacenter_id
        self.current_class = "Viveck_1"


    def _get_timestamp(self, key, sem, server, output, lock):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))

        data = {"method": "get_timestamp",
                "key": key}

        sock.sendall(json.dumps(data).encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except:
            print("Server with host: {1} and port: {2} timeout for getTimestamp in ABD", (server["host"],
                                                                                          server["port"]))
        else:
            data = json.loads(data)
            lock.acquire()
            output.append(data["timestamp"])
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def get_timestamp(self, key, server_list):
        # Step 1 : getting the timestamp
        sem = threading.Barrier(self.quorum_1 + 1, timeout=self.timeout)
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
                                                                                      lock,
                                                                                      self.current_class)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        # Removing barrier for all the waiting threads
        sem.abort()

        lock.acquire()
        if (len(output) < self.quorum_1):
            lock.release()
            raise Exception("Timeout during timestamps")


        time = TimeStamp.get_max_timestamp(output)
        lock.release()
        return time


    def _put(self, key, value, timestamp, sem, server, output, lock):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))

        data = json.dumps({"method": "put",
                           "key": key,
                           "value": value.decode("latin-1"),
                           "timestamp": timestamp,
                           "class": self.current_class})

        sock.sendall(data.encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except:
            print("Server with host: {1} and port: {2} timeout for put request in Viceck_1", (server["host"],
                                                                                              server["port"]))
        else:
            data = json.loads(data)
            lock.acquire()
            output.append(data)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def encode(self, value, codes):
        codes.extend(self.ec_driver.encode(value.encode('ASCII')))
        return


    def _put_fin(self, key, timestamp, sem, server, output, lock):
        # TODO : Generic function to send all get put request rather than different for all
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))
        print(server)

        data = json.dumps({"method": "put_fin",
                           "key": key,
                           "timestamp": timestamp,
                           "class": self.current_class})

        sock.sendall(data.encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except:
            print("Server with host: {1} and port: {2} timeout for put fin request in Viveck",
                  (server["host"], server["port"]))
        else:
            data = json.loads(data)
            lock.acquire()
            output.append(data)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def put(self, key, value, server_list=None, insert=False):
        # Step1 : Concurrently encode while getting the latest timestamp from the servers
        codes = []
        thread_list = []
        erasure_coding_thread = threading.Thread(target=self.encode, args=(value, codes))
        erasure_coding_thread.deamon = True
        erasure_coding_thread.start()

        # If INSERT that means first time else Step1 i.e. get_timestamp from servers with the key
        if insert:
            timestamp = TimeStamp.get_current_time(self.id)
            server_list = self.placement_policy.get_dc(self.total_nodes,
                                                       self.data_center.get_datacenter_list(),
                                                       self.local_datacenter_id,
                                                       key)
        else:
            try:
                # Unlike in ABD, here it returns the current timestamp
                timestamp = self.get_timestamp(key, server_list)
                timestamp = TimeStamp.get_next_timestamp([timestamp], self.id)
            except:
                return {"status": "TimeOut", "message": "Timeout during get timestamp call of Viveck_1"}


        # Wait for erasure coding to finish
        erasure_coding_thread.join()
        print(type(codes[0]))


        # Step2 : Send the message with codes
        sem = threading.Barrier(self.quorum_2 + 1, timeout=self.timeout)
        lock = threading.Lock()

        output = []

        index = 0
        for data_center_id, servers in server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put, args=(key,
                                                                            codes[index],
                                                                            timestamp,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            lock)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

                index += 1

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        # Removing barrier for all the waiting threads
        sem.abort()

        lock.acquire()
        if (len(output) < self.quorum_2):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout during put code call of Viceck_1"}

        lock.release()

        # Step3: Send the fin label to all servers
        output = []
        sem_1 = threading.Barrier(self.quorum_3 + 1, timeout=self.timeout)
        lock = threading.Lock()

        for data_center_id, servers in server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put_fin, args=(key,
                                                                            timestamp,
                                                                            sem_1,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            lock)))

                thread_list[-1].deamon = True
                thread_list[-1].start()

                index += 1

        try:
            sem_1.wait()
        except threading.BrokenBarrierError:
            pass

        # Removing barrier for all the waiting threads
        sem_1.abort()

        lock.acquire()
        if (len(output) < self.quorum_3):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout during put fin label call of Viceck_1"}
        lock.release()

        return {"status": "OK", "value": value, "timestamp": timestamp, "server_list": server_list}


    def _get(self, key, timestamp, sem, server, output, lock):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))

        data = {"method": "get",
                "key": key,
                "timestamp": timestamp}

        sock.sendall(json.dumps(data).encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except:
            print("Server with host: {1} and port: {2} timeout for get request in ABD", (server["host"],
                                                                                         server["port"]))
        else:
            data = json.loads(data)
            if data["value"]:
                lock.acquire()
                output.append(data)
                lock.release()
            try:
                sem.wait()
            except threading.BrokenBarrierError:
                pass

        return


    def decode(self, chunk_list):
        return self.ec_driver.decode(chunk_list)

    def get(self, key, server_list):
        # Step1: Get the timestamp for the key
        # Error can imply either server busy or key doesn't exist
        try:
            timestamp = self.get_timestamp(key, server_list)
        except:
            return {"status": "TimeOut", "message": "Timeout during get timestamp call of Viveck"}

        thread_list = []

        sem = threading.Barrier(self.k + 1, timeout=self.timeout)
        lock = threading.Lock()

        # Step1: get the timestamp with recent value
        output = []
        for data_center_id, servers in server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get, args=(key,
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

        sem.abort()

        lock.acquire()
        if (len(output) < self.k):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout/Concurrency error during get call"}
        try:
            value = self.decode(output)
        except Exception as e:
            lock.relase()
            return {"status": "TimeOut", "message": e}

        lock.relase()
        return {"status": "OK", "value": value, "timestamp": timestamp}
