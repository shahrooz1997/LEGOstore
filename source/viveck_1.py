from lamport_timestamp import TimeStamp

import hashlib
import threading
import json
import socket
import copy
from placement_policy import PlacementFactory
from protocol_interface import ProtocolInterface
from pyeclib.ec_iface import ECDriver
import time


class Viveck_1(ProtocolInterface):

    def __init__(self, properties, local_datacenter_id, data_center, client_id,
                 latency_between_DCs, dc_cost):
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

        self.manual_servers = {}

        # This is only added for the prototype. In real system you would never use it.
        self.dc_to_latency_map = copy.deepcopy(latency_between_DCs[self.local_datacenter_id])
        self.latency_delay = copy.deepcopy(latency_between_DCs[self.local_datacenter_id])

        # Converting string values to float first
        # Could also be done in next step eaisly but just for readability new step
        for key, value in self.latency_delay.items():
            self.latency_delay[key] = float(value)
            self.dc_to_latency_map[key] = float(value)

        # Sorting the datacenters as per the latency
        self.latency_delay = [k for k in sorted(self.latency_delay, key=self.latency_delay.get,
                                                reverse=False)]


        if "manual_dc_server_ids" in properties:
            self.manual_servers = copy.deepcopy(properties["manual_dc_server_ids"])

        self.dc_cost = dc_cost

        # Converting the data center cost as per latency
        self.dc_cost[local_datacenter_id] = 0
        for key, value in self.dc_cost.items():
            self.dc_cost[key] = float(value)

        # Sorting the datacenter id as per transfer cost
        self.dc_cost = [k for k in sorted(self.dc_cost, key=self.dc_cost.get,
                                                reverse=False)]


    def _get_cost_effective_server_list(self, server_list):
        # Sort the DCs as per the minimal cost of data trasnfer for get
        # Returns [(DC_ID, SERVER_ID), ]
        output = []
        for data_center_id in self.dc_cost:
            if data_center_id in server_list:
                output.append((data_center_id, server_list[data_center_id]))

        return output


    def _get_closest_servers(self, server_list, quorum_size):
        # Select the DC and servers with minimal latencies for the quourm
        total_servers = 0
        expected_server_list = {}

        for data_center_id in self.latency_delay:
            if data_center_id in server_list:
                expected_server_list[data_center_id] = server_list[data_center_id]
                total_servers += len(server_list[data_center_id])

            if total_servers == quorum_size:
                break
            # We want exact number of servers quorum size Q1
            elif total_servers > quorum_size:
                expected_server_list[data_center_id] = \
                    server_list[data_center_id][:total_servers - quorum_size]
                break

        return expected_server_list


    def _get_timestamp(self, key, sem, server, output, lock, current_class, delay=0):
        print("delay is : " + str(delay*0.001))
        time.sleep(delay * 0.001)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))

        data = {"method": "get_timestamp",
                "key": key,
                "class": current_class}


        sock.sendall(json.dumps(data).encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except Exception as e:
            print("Server with host: {1} and port: {2} timeout for getTimestamp in ABD", (server["host"],
                                                                                          server["port"]))
        else:
            data = json.loads(data.decode("utf-8"))
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
        new_server_list = self._get_closest_servers(server_list, self.quorum_1)
        print("timestamp server: " + str(new_server_list))
        for data_center_id, servers in new_server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get_timestamp,
                                                    args=(key,
                                                          sem,
                                                          copy.deepcopy(server_info),
                                                          output,
                                                          lock,
                                                          self.current_class,
                                                          self.dc_to_latency_map[data_center_id])))
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


    def _put(self, key, value, timestamp, sem, server, output, lock, delay=0):
        time.sleep(delay * 0.001)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))

        data = json.dumps({"method": "put",
                           "key": key,
                           "value": value,
                           "timestamp": timestamp,
                           "class": self.current_class})

        sock.sendall(data.encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except Exception as e:
            print("Server with host: {1} and port: {2} timeout for put request in Viceck_1", (server["host"],
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


    def encode(self, value, codes):
        codes.extend(self.ec_driver.encode(value.encode("utf-8")))
        for index in range(len(codes)):
           #print(type(codes[index]))
           codes[index] = codes[index].decode("latin-1")

        return


    def _put_fin(self, key, timestamp, sem, server, output, lock, delay=0):
        # TODO : Generic function to send all get put request rather than different for all
        time.sleep(delay * 0.001)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))

        data = json.dumps({"method": "put_fin",
                           "key": key,
                           "timestamp": timestamp,
                           "class": self.current_class})

        sock.sendall(data.encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except Exception as e:
            print("Server with host: {1} and port: {2} timeout for put fin request in Viveck",
                  (server["host"], server["port"]))
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
            if self.manual_servers and self.placement_policy.__name__ == "Manual":
                server_list = self.manual_servers
            else:
                server_list = self.placement_policy.get_dc(self.total_nodes,
                                                           self.data_center.get_datacenter_list(),
                                                           self.local_datacenter_id,
                                                           key)
        else:
            try:
                # Unlike in ABD, here it returns the current timestamp
                timestamp = self.get_timestamp(key, server_list)
                timestamp = TimeStamp.get_next_timestamp([timestamp], self.id)
            except Exception as e:
                return {"status": "TimeOut", "message": "Timeout during get timestamp call of Viveck_1"}


        # Wait for erasure coding to finish
        erasure_coding_thread.join()

        # Step2 : Send the message with codes
        sem = threading.Barrier(self.quorum_2 + 1, timeout=self.timeout)
        lock = threading.Lock()

        output = []

        index = 0
        # Sorting the servers as per the DC latencies and selecting nearest Q2.
        # TODO: Retry feature with timeouts with any of those didn't respond.
        # Although this might not be a bottleneck for now
        new_server_list = self._get_closest_servers(server_list, self.quorum_2)

        # Still sending to all for the request but wait for only required quorum to respond.
        # If needed ping to only required quorum and then retry
        print("q2: " + str(new_server_list))
        for data_center_id, servers in new_server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put,
                                                    args=(key,
                                                          codes[index],
                                                          timestamp,
                                                          sem,
                                                          copy.deepcopy(server_info),
                                                          output,
                                                          lock,
                                                          self.dc_to_latency_map[data_center_id])))
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

        new_server_list = self._get_closest_servers(server_list, self.quorum_3)

        print("q3: " + str(new_server_list))
        for data_center_id, servers in new_server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put_fin, args=(key,
                                                                            timestamp,
                                                                            sem_1,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            lock,
                                                                            self.dc_to_latency_map[data_center_id])))

                thread_list[-1].deamon = True
                thread_list[-1].start()

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


    def _get(self, key, timestamp, sem, server, output, lock, delay=0, value_required=False):
        time.sleep(delay * 0.001)
        print("get_delay: " + str(delay*0.001))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server["host"], int(server["port"])))

        data = {"method": "get",
                "key": key,
                "timestamp": timestamp,
                "class": self.current_class,
                "value_required": value_required}

        sock.sendall(json.dumps(data).encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(1024)
        except Exception as e:
            print("Server with host: {1} and port: {2} timeout for get request in ABD", (server["host"],
                                                                                         server["port"]))
        else:
            data = json.loads(data.decode("utf-8"))
            if data["value"]:
                lock.acquire()
                output.append(data["value"])
                lock.release()
            try:
                sem.wait()
            except threading.BrokenBarrierError:
                pass

        return


    def decode(self, chunk_list):
        for i in range(len(chunk_list)):
            chunk_list[i] = chunk_list[i].encode("latin-1")
        return self.ec_driver.decode(chunk_list).decode("utf-8")


    def get(self, key, server_list):
        # Step1: Get the timestamp for the key
        # Error can imply either server busy or key doesn't exist
        try:
            timestamp = self.get_timestamp(key, server_list)
        except Exception as e:
            return {"status": "TimeOut", "message": "Timeout during get timestamp call of Viveck"}

        thread_list = []

        sem = threading.Barrier(self.quorum_4 + 1, timeout=self.timeout)
        lock = threading.Lock()

        new_server_list = self._get_closest_servers(server_list, self.quorum_4)
        print("get list: " + str(new_server_list))
        minimum_cost_list = self._get_cost_effective_server_list(new_server_list)
        print("get optimal cost: " + str(minimum_cost_list))
        # Step2: Get the encoded value
        index = 0
        output = []
        # Sending the get request to the selected servers
        for data_center_id, servers in minimum_cost_list:
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get,
                                                    args=(key,
                                                          timestamp,
                                                          sem,
                                                          copy.deepcopy(server_info),
                                                          output,
                                                          lock,
                                                          self.dc_to_latency_map[data_center_id],
                                                          index < self.k)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

                index += 1
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
            lock.release()
            return {"status": "TimeOut", "message": e}

        lock.release()
        return {"status": "OK", "value": value, "timestamp": timestamp}
