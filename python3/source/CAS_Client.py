from lamport_timestamp import Timestamp

import struct
import hashlib
import threading
import json
import socket
import copy
from placement_policy import PlacementFactory
from protocol_interface import ProtocolInterface
from pyeclib.ec_iface import ECDriver
import time
import sys

#ata for logging
import logging
#from logging import StreamHandler
#import logstash


class CAS_Client(ProtocolInterface):
    def __init__(self, properties, local_datacenter_id, data_center, client_id):
        self.timeout_per_request = int(properties["timeout_per_request"])
        ##########################
        ## Quorum relevant properties
        ## Q1 + Q3 > N
        ## Q1 + Q4 > N
        ## Q2 + Q4 >= N+k
        ## Q4 >= k
        ##########################

        # https://github.com/openstack/pyeclib . Check out the supported types.
        #TODO: us class for different groups where ecdriver is intilizied per group
        self.ec_type = "liberasurecode_rs_vand"

        # Generic timeout for everything
        self.timeout = int(properties["timeout_per_request"])
        self.data_center = data_center
        self.id = client_id
        self.local_datacenter_id = local_datacenter_id
        self.current_class = "Viveck_1"
        self.encoding_byte = "latin-1"

        latency_log_file_name = "individual_times_" + str(self.id) + ".txt"
        socket_log_file_name = "socket_times_" + str(self.id) + ".txt"
        coding_log_file_name = "coding_times_" + str(self.id) + ".txt"
        self.latency_log = open("cas_latency.log", "w")
        self.socket_log  = open("cas_socket.log", "w")
        self.coding_log = open("cas_coding.log", "w")
        self.lock_latency_log = threading.Lock()
        self.lock_socket_log = threading.Lock()
        self.lock_coding_log = threading.Lock()


    def _get_cost_effective_server_list(self, server_list):
        # Sort the DCs as per the minimal cost of data trasnfer for get
        # Returns [(DC_ID, SERVER_ID), ]
        output = []
        for data_center_id in self.dc_cost:
            if data_center_id in server_list:
                output.append((data_center_id, server_list[data_center_id]))

        return output


    def recvall(self, sock):
        fragments = []
        while True:
            chunck = sock.recv(64000)
            if not chunck:
                break
            fragments.append(chunck)

        return b''.join(fragments)


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


    def _get_timestamp(self, key, sem, server, output, socket_times,  lock, current_class, delay=0):
        #time.sleep(delay * 0.001)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()

        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        socket_time = delta_time
        self.socket_log.write(server["host"] + ":" + str(delta_time) + "\n")
        self.socket_log.flush()
        self.lock_socket_log.release()


        data = {"method": "get_timestamp",
                "key": key,
                "class": current_class}
        data_to_send = "get_timestamp" + "+:--:+" + key + "+:--:+" + current_class
        self.send_msg(sock, data_to_send.encode(self.encoding_byte))
        #sock.sendall(json.dumps(data).encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(640000)
            sock.close()
        except Exception as e:
            print("Server with host: {1} and port: {2} timeout for getTimestamp in ABD", (server["host"],
                                                                                          server["port"]))
        else:
            data = json.loads(data.decode(self.encoding_byte))
            lock.acquire()
            output.append(data["timestamp"])
            socket_times.append(socket_time)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def get_timestamp(self, key, datacenter_list):
        # Step 1 : getting the timestamp
        
        sem = threading.Barrier(len(datacenter_list) + 1, timeout=self.timeout)
        lock = threading.Lock()
        thread_list = []

        output = []
        socket_times = []
        for data_center_id in datacenter_list:
            #TODO: assume single server in every datacenter
            for server_id in ["1"]:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get_timestamp,
                                                    args=(key,
                                                          sem,
                                                          copy.deepcopy(server_info),
                                                          output,
                                                          socket_times,
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
        if (len(output) < len(datacenter_list)):
            lock.release()
            raise Exception("Timeout during timestamps")
        timestamp = Timestamp.get_max_timestamp(output)
        lock.release()
        
        return timestamp, socket_times


    def send_msg(self, sock, msg):
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(msg)) + msg
        sock.sendall(msg)


    def _put(self, key, value, timestamp, sem, server, output, socket_times,  lock, delay=0):
        #time.sleep(delay * 0.001)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()
        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        socket_time = delta_time
        self.socket_log.write(server["host"] + ":" + str(delta_time) + "\n")
        self.lock_socket_log.release()

        data_to_put = "put" + "+:--:+" + key + "+:--:+" + value + "+:--:+" + timestamp + "+:--:+" + self.current_class
        self.send_msg(sock, data_to_put.encode(self.encoding_byte))
        #sock.sendall(data.encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(640000)
        #    sock.close()
        except Exception as e:
            print("Server with host: {1} and port: {2} timeout for put request in Viceck_1", (server["host"],
                                                                                              server["port"]))
        else:
            data = json.loads(data.decode(self.encoding_byte))
            lock.acquire()
            output.append(data)
            socket_times.append(socket_time)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def _put_fin(self, key, timestamp, sem, server, output, socket_times,  lock, delay=0):
        # TODO : Generic function to send all get put request rather than different for all
        #time.sleep(delay * 0.001)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()

        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        socket_time = delta_time
        self.socket_log.write(server["host"] + ":" + str(delta_time) + "\n")
        # self.socket_log.flush()
        self.lock_socket_log.release()

        data_to_send = "put_fin" + "+:--:+" + key + "+:--:+" + timestamp + "+:--:+" + self.current_class

        self.send_msg(sock, data_to_send.encode(self.encoding_byte))
        #sock.sendall(data.encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = sock.recv(640000)
            sock.close()
        except Exception as e:
            print("Server with host: {1} and port: {2} timeout for put fin request in Viveck",
                  (server["host"], server["port"]))
        else:
            data = json.loads(data.decode(self.encoding_byte))
            lock.acquire()
            output.append(data)
            socket_times.append(socket_time)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def put(self, key, value, placement, insert=False):

        q1_dc_list = placement["Q1"]
        q2_dc_list = placement["Q2"]
        q3_dc_list = placement["Q3"]
        q4_dc_list = placement["Q4"]
        k = placement["k"]
        m = placement["m"]

        # Step1 : Concurrently encode while getting the latest timestamp from the servers
        codes = []
        thread_list = []
        erasure_coding_thread = threading.Thread(target=self.encode, args=(value, codes, m, k))
        erasure_coding_thread.deamon = True
        erasure_coding_thread.start()


        datacenters_list = list(set(q1_dc_list) | set(q2_dc_list) | set(q3_dc_list) | set(q4_dc_list))



        # If INSERT that means first time else Step1 i.e. get_timestamp from servers with the key
        if insert:
            
            timestamp = Timestamp.create_new_timestamp(self.id)
            '''
            if self.manual_servers and self.placement_policy.__name__ == "Manual":
                server_list = self.manual_servers
            else:
                server_list = self.placement_policy.get_dc(self.total_nodes,
                                                           self.data_center.get_datacenter_list(),
                                                           self.local_datacenter_id,
                                                           key)
            '''
        else:
            try:
                # Unlike in ABD, here it returns the current timestamp
                start_time = time.time()
                timestamp, socket_times = self.get_timestamp(key, q2_dc_list)
                timestamp = Timestamp.increment_timestamp(timestamp, self.id)
                end_time = time.time()

                self.lock_latency_log.acquire()
                max_socket_time = max(socket_times)
                delta_time = int((end_time - start_time)*1000)
                q1_time = delta_time - max_socket_time
                self.latency_log.write("put:Q1:" + str(q1_time) + "\n")
                self.lock_latency_log.release()
            except Exception as e:
                return {"status": "TimeOut", "message": "Timeout during get timestamp call of Viveck_1"}


        # Wait for erasure coding to finish
        erasure_coding_thread.join()

        # Step2 : Send the message with codes
        sem = threading.Barrier(len(q2_dc_list) + 1, timeout=self.timeout)
        lock = threading.Lock()

        output = []
        socket_times = []

        index = 0
        

        # Still sending to all for the request but wait for only required quorum to respond.
        # If needed ping to only required quorum and then retry
        start_time = time.time()
        for data_center_id in q2_dc_list:
            #TODO: for now assume single server on every datacenter
            for server_id in ["1"]:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put,
                                                    args=(key,
                                                          codes[index],
                                                          timestamp,
                                                          sem,
                                                          copy.deepcopy(server_info),
                                                          output,
                                                          socket_times,
                                                          lock)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

                index += 1

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass
        end_time = time.time()
        self.lock_latency_log.acquire()
        max_socket_time = max(socket_times)
        delta_time = int((end_time - start_time)*1000)
        q2_time = delta_time - max_socket_time
        self.latency_log.write("put:Q2:" + str(q2_time) + "\n")
        self.lock_latency_log.release()
        # Removing barrier for all the waiting threads
        sem.abort()

        lock.acquire()
        if (len(output) < len(q2_dc_list)):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout during put code call of Viceck_1"}

        lock.release()

        # Step3: Send the fin label to all servers
        output = []
        socket_times = []
        sem_1 = threading.Barrier(len(q3_dc_list) + 1, timeout=self.timeout)
        lock = threading.Lock()

        start_time = time.time()
        for data_center_id in q3_dc_list:
            #TODO: assume single server in every datacenter
            for server_id in ["1"]:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put_fin, args=(key,
                                                                            timestamp,
                                                                            sem_1,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            socket_times,
                                                                            lock)))

                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem_1.wait()
        except threading.BrokenBarrierError:
            pass
        end_time =  time.time()
        self.lock_latency_log.acquire()
        max_socket_time = max(socket_times)
        delta_time = int((end_time - start_time)*1000)
        q3_time = delta_time - max_socket_time
        self.latency_log.write("put:Q3:" + str(q3_time) + "\n")
        # self.latency_log.flush()
        self.lock_latency_log.release()
        # Removing barrier for all the waiting threads
        sem_1.abort()

        lock.acquire()
        if (len(output) < len(q3_dc_list)):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout during put fin label call of Viceck_1"}
        lock.release()
        
        if insert:
            request_time = q2_time + q3_time
        else:
            request_time = q1_time + q2_time + q3_time

        return {"status": "OK", "timestamp": timestamp}, request_time


    def _get(self, key, timestamp, sem, server, output, socket_times, lock, delay=0, value_required=False):
        #time.sleep(delay * 0.001)
        #print("get_delay: " + str(delay*0.001))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()

        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        socket_time = delta_time
        self.socket_log.write(server["host"] + ":" + str(delta_time) + "\n")
        # self.socket_log.flush()
        self.lock_socket_log.release()

        data_to_send = "get" + "+:--:+" + key + "+:--:+" + timestamp + "+:--:+" + self.current_class + "+:--:+" + str(value_required)

        self.send_msg(sock, data_to_send.encode(self.encoding_byte))
       # sock.sendall(json.dumps(data).encode("utf-8"))
        sock.settimeout(self.timeout_per_request)

        try:
            data = self.recvall(sock)
            sock.close()
        except Exception as e:
            print("Server with host: {1} and port: {2} timeout for get request in ABD", (server["host"],
                                                                                         server["port"]))
        else:
            data = data.decode(self.encoding_byte)
            data_list = data.split("+:--:+")

            if data_list[1] != "None":
                lock.acquire()
                output.append(data_list[1])
                socket_times.append(socket_time)
                lock.release()
            try:
                sem.wait()
            except threading.BrokenBarrierError:
                pass

        return


    def decode(self, chunk_list, m, k):

        assert(m > k)
        ec_driver = ECDriver(k=k, m=m, ec_type=self.ec_type)
        start_time = time.time()
        if k == 1:
            return chunk_list[0]

        for i in range(len(chunk_list)):
            chunk_list[i] = chunk_list[i].encode(self.encoding_byte)
        decoded_data = ec_driver.decode(chunk_list).decode(self.encoding_byte)
        end_time = time.time()
        self.lock_coding_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        self.coding_log.write("decode:" + str(delta_time) + "\n")
        self.lock_coding_log.release()
        return decoded_data


    def encode(self, value, codes, m, k):

        assert(m > k)
        ec_driver = ECDriver(k=k, m=m, ec_type=self.ec_type)
        # Updated for viveck
        start_time = time.time()
        if k == 1:
            codes.extend([value]*m)
            return codes

        codes.extend(ec_driver.encode(value.encode(self.encoding_byte)))
        for index in range(len(codes)):
           #print(type(codes[index]))
           codes[index] = codes[index].decode(self.encoding_byte)
           #print(sys.getsizeof(codes[index]))
        end_time = time.time()
        self.lock_coding_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        self.coding_log.write("encode:" + str(delta_time) + "\n")
        self.lock_coding_log.release()
        return

    #TODO: TEST WHEN DEPLOYED IF NEEDED
    def _get_from_remaining(self, key, timestamp, server_list, called_data_center):
        # Get the data from the servers except for called_data_center
        # Please not here server_list is [(DC_ID, [server_list])]

        thread_list = []
        output = []
        # Waiting for remaining all to return the value
        sem = threading.Barrier(self.quorum_4 - self.k + 1, timeout=self.timeout)
        lock = threading.Lock()
        for data_center_id, servers in server_list:
            for server_id in servers:
                # If already called then just skip
                if (data_center_id, server_id) in called_data_center:
                    continue

                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get,
                                                    args=(key,
                                                          timestamp,
                                                          sem,
                                                          copy.deepcopy(server_info),
                                                          output,
                                                          lock,
                                                          True)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return output


    def get(self, key, placement):

        q1_dc_list = placement["Q1"]
        q4_dc_list = placement["Q4"]
        m = placement["m"]
        k = placement["k"]
        
        # Step1: Get the timestamp for the key
        # Error can imply either server busy or key doesn't exist
        try:
            start_time = time.time()
            timestamp, socket_times = self.get_timestamp(key, q1_dc_list)
            end_time = time.time()

            self.lock_latency_log.acquire()
            max_socket_time = max(socket_times)
            delta_time = int((end_time - start_time)*1000)
            q1_time = delta_time - max_socket_time
            self.latency_log.write("get:Q1:" + str(q1_time) + "\n")
            self.lock_latency_log.release()
        except Exception as e:
            return {"status": "TimeOut", "message": "Timeout during get timestamp call of Viveck"}

        thread_list = []

        sem = threading.Barrier(len(q4_dc_list) + 1, timeout=self.timeout)
        lock = threading.Lock()

        # Step2: Get the encoded value
        index = 0
        output = []
        socket_times = []
        # This parameter is to mark the datacenter, servers which have already sent an request
    
        called_data_center = set()
        # Sending the get request to the selected servers
        start_time = time.time()
        for data_center_id in q4_dc_list:
            #TODO: assume single server in every datacenter
            for server_id in ["1"]:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get,
                                                    args=(key,
                                                          timestamp,
                                                          sem,
                                                          copy.deepcopy(server_info),
                                                          output,
                                                          socket_times,
                                                          lock,
                                                          0,
                                                          True)))
                thread_list[-1].deamon = True
                thread_list[-1].start()
                if index < k:
                    called_data_center.add((data_center_id, server_id))
                index += 1
        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass
        end_time = time.time()
        self.lock_latency_log.acquire()
        max_socket_time = max(socket_times)
        delta_time = int((end_time - start_time)*1000)
        q4_time = delta_time - max_socket_time
        self.latency_log.write("get:Q4:" + str(q4_time) + "\n")
        self.lock_latency_log.release()
        sem.abort()

        lock.acquire()
        # If we doesn't get requested data. We will try to ping all other servers for it.
#        if (len(output) < self.k):
#            additional_codes = self._get_from_remaining(key,
#                                                        timestamp,
#                                                        minimum_cost_list,
#                                                        called_data_center)
#            output.extend(additional_codes)

        # We didn't get it from all other servers too. So basically we raise an error
        if (len(output) < k):
            lock.release()
            return {"status": "TimeOut", "value": "None", "message": "Timeout/Concurrency error during get call"}
        try:
            value = self.decode(output, m, k)
        except Exception as e:
            lock.release()
            return {"status": "TimeOut", "value": "None","message": e}

        lock.release()
        request_time = q1_time + q4_time
        return {"status": "OK", "value": value, "timestamp": timestamp}, request_time

#    def get_logger(self, tag):
#        logger_ = logging.getLogger(tag)
#        logger_.setLevel(logging.INFO)
#        logger_.addHandler(logstash.TCPLogstashHandler("localhost", 8080,  version=1))
#        logger_.addHandler(StreamHandler())
#        return logger_


    def get_logger(self,log_path):
        logger_ = logging.getLogger('log')
        logger_.setLevel(logging.INFO)
        handler = logging.FileHandler(log_path)
        # XXX: TODO: Check if needed
        handler.setFormatter(logging.Formatter('%(message)s'))
        logger_.addHandler(handler)
        return logger_
