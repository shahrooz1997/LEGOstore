from quorum_policy import Quorum
from lamport_timestamp import TimeStamp

import hashlib
import threading
import json
import socket
import copy
import time
import struct

from placement_policy import PlacementFactory
from protocol_interface import ProtocolInterface


# TODO: INCOPERATE THE CLASS FOR EACH CALL AS DATASERVER WILL BE NEEDING IT
class ABD(ProtocolInterface):

    def __init__(self, properties, local_datacenter_id, data_center, client_id, latency_between_DCs, dc_cost):

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
        self.manual_servers = {}


        self.encoding_byte = "latin-1"
        
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

        # Output files initilization
        latency_log_file_name = "individual_times_" + str(self.id) + ".txt"
        socket_log_file_name = "socket_times_" + str(self.id) + ".txt"
        self.latency_log = open(latency_log_file_name, "a+")
        self.socket_log  = open(socket_log_file_name, "a+")
        self.lock_latency_log = threading.Lock()
        self.lock_socket_log = threading.Lock()

        # TODO: For now only implementing ping required numbers and wait for all to respond
        # TODO: Please note that there is infracstructure for different policies too. Please look quorum_policy.
        # self.ping_policy = Quorum(properties["ping_policy"]).get_ping_policy()
        # self.num_nodes_to_ping_for_write, self.num_nodes_to_wait_for_write = \
        #     self.ping_policy.fetchx_metrics(self.total_nodes, self.write_nodes)
        #
        # self.num_nodes_to_ping_for_read, self.num_nodes_to_wait_for_read = \
        #     self.ping_policy.fetchx_metrics(self.total_nodes, self.read_nodes)


    def recvall(self, sock):
        fragments = []
        while True:
            chunck = sock.recv(64000)
            if not chunck:
                break
            fragments.append(chunck)

        return b''.join(fragments)


    def send_msg(self, sock, msg):
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(msg)) + msg
        sock.sendall(msg)


    def _get_cost_effective_server_list(self, server_list):
        # Sort the DCs as per the minimal cost of data trasnfer for get
        # Returns [(DC_ID, SERVER_ID), ]
        output = []
        for data_center_id in self.dc_cost:
            if data_center_id in server_list:
                output.append((data_center_id, self.dc_cost[data_center_id]))

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


    def _get_timestamp(self, key, sem, server, output, lock, delay=0):
      #  print("timestamp time: " + str(delay))
      #  time.sleep(float(delay) * 0.001)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()

        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        self.socket_log.write(server["host"] + str(delta_time) + "\n")
        self.lock_socket_log.release()

        data = {"method": "get_timestamp",
                "key": key,
                "class": self.current_class}
        data_to_send = "get_timestamp" + "+:--:+" + key + "+:--:+" + self.current_class
        self.send_msg(sock, data_to_send.encode(self.encoding_byte))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(640000)
        except:
            print("Server with host: {1} and port: {2} timeout for getTimestamp in ABD", (server["host"],
                                                                                          server["port"]))
        else:
            data = json.loads(data.decode(self.encoding_byte))
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

        new_server_list = self._get_closest_servers(server_list, self.read_nodes)
        output = []
        print(self.latency_delay)
        for data_center_id, servers in new_server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get_timestamp, args=(key,
                                                                                      sem,
                                                                                      copy.deepcopy(server_info),
                                                                                      output,
                                                                                      lock,
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
        if (len(output) < self.read_nodes):
            lock.release()
            raise Exception("Timeout during timestamps")

        time = TimeStamp.get_next_timestamp(output, self.id)
        lock.release()
        return time


    def get_final_value(self, values):
        max_time = "0"
        result = None
        for data in values:
            if data[0] != "OK":
                 continue

            temp_result = data[1]
            temp_time = data[2]
            if temp_time > max_time:
                result = temp_result
                max_time = temp_time

        return (result, max_time)


    def _put(self, key, value, timestamp, sem, server, output, lock, delay=0):
     #   print("put time: " + str(delay))
     #   time.sleep(int(delay) * 0.001)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()

        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        self.socket_log.write(server["host"] + str(delta_time) + "\n")
        self.lock_socket_log.release()

        print(server)

        data = json.dumps({"method": "put",
                           "key": key,
                           "value": value,
                           "timestamp": timestamp,
                           "class": self.current_class})
        data_to_send = "put" + "+:--:+" + key + "+:--:+" + value +\
                             "+:--:+" +timestamp + "+:--:+" + self.current_class
        self.send_msg(sock, data_to_send.encode(self.encoding_byte))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(640000)
        except:
            print("Server with host: {1} and port: {2} timeout for put request in ABD", (server["host"],
                                                                                         server["port"]))
        else:
            data = json.loads(data.decode(self.encoding_byte))
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
            if self.manual_servers and self.placement_policy.__name__ == "Manual":
                server_list = self.manual_servers
            else:
                # It shouldn't be here if manual policy is used.
                # It will throw exception in that case. NOT CATCHING IT AS PROGRAM SHOULD STOP.
                server_list = self.placement_policy.get_dc(self.total_nodes,
                                                           self.data_center.get_datacenter_list(),
                                                           self.local_datacenter_id,
                                                           key)
        else:
            start_time = time.time()
            try:
                timestamp = self.get_timestamp(key, server_list)
            except:
                return {"status": "TimeOut", "message": "Timeout during get timestamp call of ABD"}
            end_time = time.time()
            self.lock_latency_log.acquire()
            delta_time = int((end_time - start_time)*1000)
            self.latency_log.write("put:Q1:" + str(delta_time) + "\n")
            self.lock_latency_log.release()

        if timestamp == "0-" + self.id:
            return {"status": "Failure", "message": "Unable to get valid time from the quorum"}

        # Step2 : Send the message to put
        sem = threading.Barrier(self.write_nodes + 1, timeout=self.timeout)
        lock = threading.Lock()

        output = []

        new_server_list = self._get_closest_servers(server_list, self.write_nodes)
        #print(self.latency_delay)
        start_time = time.time()
        for data_center_id, servers in new_server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                # Added the latency delay purely for porpuse of prototyping.
                # Use default value of 0 otherwise.
                thread_list.append(threading.Thread(target=self._put, args=(key,
                                                                            value,
                                                                            timestamp,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            lock,
                                                                            self.dc_to_latency_map[data_center_id])))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        end_time = time.time()
        self.lock_latency_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        self.latency_log.write("put:Q2:" + str(delta_time) + "\n")
        self.lock_latency_log.release()

        # Removing barrier for all the waiting threads
        sem.abort()

        lock.acquire()
        if (len(output) < self.write_nodes):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout during put call of ABD"}

        lock.release()

        return {"status": "OK", "value": value, "timestamp": timestamp, "server_list": server_list}


    def _get(self, key, sem, server, output, lock, delay=0):
      #  print("get time: " + str(delay))
      #  time.sleep(float(delay) * 0.001)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()

        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        self.socket_log.write(server["host"] + str(delta_time) + "\n")
        self.lock_socket_log.release()

        data = {"method": "get",
                "key": key,
                "timestamp": None,
                "class": self.current_class}

        data_to_send = "get" + "+:--:+" + key + "+:--:+" + "None" + "+:--:+" + self.current_class

        self.send_msg(sock, data_to_send.encode(self.encoding_byte))
        sock.settimeout(self.timeout_per_request)

        try:
            data = self.recvall(sock)
        except:
            print("Server with host: {1} and port: {2} timeout for get request in ABD", (server["host"],
                                                                                         server["port"]))
        else:
            data = data.decode("latin-1").split("+:--:+")
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
        new_server_list = self._get_closest_servers(server_list, self.read_nodes)
        start_time = time.time()
        for data_center_id, servers in new_server_list.items():
            for server_id in servers:

                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get, args=(key,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            lock,
                                                                            self.dc_to_latency_map[data_center_id])))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        end_time = time.time()
        self.lock_latency_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        self.latency_log.write("get:Q1:" + str(delta_time) + "\n")
        self.lock_latency_log.release()

        sem.abort()
        lock.acquire()
        if (len(output) < self.read_nodes):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout during get call of ABD"}
        value, timestamp = self.get_final_value(output)

        lock.release()

        if timestamp == "0":
            return {"status": "Error", "message": "No timestap found. Check quorum again"}

        # After abort can't use the same barrier
        sem = threading.Barrier(self.write_nodes + 1, timeout=self.timeout)
        result = []
        new_server_list = self._get_closest_servers(server_list, self.write_nodes)
        start_time = time.time()
        for data_center_id, servers in new_server_list.items():
            for server_id in servers:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put, args=(key,
                                                                            value,
                                                                            timestamp,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            result,
                                                                            lock,
                                                                            self.dc_to_latency_map[data_center_id])))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass
        end_time = time.time()
        self.lock_latency_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        self.latency_log.write("get:Q2:" + str(delta_time) + "\n")
        self.lock_latency_log.release()

        sem.abort()

        return {"status": "OK", "value": value}
