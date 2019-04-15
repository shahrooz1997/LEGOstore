from quorum_policy import Quorum
from lamport_timestamp import Timestamp

import hashlib
import threading
import json
import socket
import copy
import time
import struct

from placement_policy import PlacementFactory
from protocol_interface import ProtocolInterface
#ata for logging
import logging
#from logging import StreamHandler
#import logstash

# TODO: INCOPERATE THE CLASS FOR EACH CALL AS DATASERVER WILL BE NEEDING IT
class ABD_Client(ProtocolInterface):

    def __init__(self, properties, local_datacenter_id, data_center, client_id):

        self.timeout_per_request = int(properties["timeout_per_request"])

        # Generic timeout for everything
        self.timeout = int(properties["timeout_per_request"])
        self.data_center = data_center
        self.id = client_id
        self.local_datacenter_id = local_datacenter_id
        self.current_class = "ABD"

        self.encoding_byte = "latin-1"

        # Output files initilization
        latency_log_file_name = "individual_times_" + str(self.id) + ".txt"
        socket_log_file_name = "socket_times_" + str(self.id) + ".txt"
        self.latency_log = open("abd_latency.log", "w")
        self.socket_log  = open("abd_socket.log", "w")
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


    def _get_timestamp(self, key, sem, server, output, socket_times,  lock, delay=0):
      #  print("timestamp time: " + str(delay))
      #  time.sleep(float(delay) * 0.001)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()

        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        socket_time = delta_time
        self.socket_log.write(server["host"] + ":" + str(delta_time) + "\n")
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
            socket_times.append(socket_time)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def get_timestamp(self, key, datacenter_list):
        ######################
        # Call for get timestamp
        # @ Returns the next for the call
        # @ Raises Exception in case of timeout or socket error
        ######################

        # Step 1 : getting the timestamp
        sem = threading.Barrier(len(datacenter_list) + 1, timeout=self.timeout)
        lock = threading.Lock()
        thread_list = []
        
        socket_times = []

        output = []
        for data_center_id in datacenter_list:
            '''assume single server in datacenter'''
            for server_id in ["1"]:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get_timestamp, args=(key,
                                                                                      sem,
                                                                                      copy.deepcopy(server_info),
                                                                                      output,
                                                                                      socket_times,
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
        if (len(output) < len(datacenter_list)):
            lock.release()
            raise Exception("Timeout during timestamps")

        timestamp = Timestamp.get_max_timestamp(output)
        lock.release()
        return timestamp, socket_times


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


    def is_all_timestamps_the_same(self, values, qourum_size):
        count = 0
        temp_time = None
        for data in values:
            if data[0] != "OK":
                continue
            if temp_time == None:
                count += 1
                temp_time = data[2]
            elif temp_time != data[2]:
                continue
            else:
                count += 1
        assert(temp_time is not None)
        if count < qourum_size:
            return False
        return True 
    
    def dcs_with_highest_timestamp(self, values, highest_timestamp):
        rtn_dcs = []
        for data in values:
            if data[0] != "OK":
                continue
            elif data[2] == highest_timestamp:
                rtn_dcs.append(data[3])
        return rtn_dcs 
    
        
    def _put(self, key, value, timestamp, sem, server, output,  socket_times,  lock, delay=0):
     #   print("put time: " + str(delay))
     #   time.sleep(int(delay) * 0.001)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()

        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        socket_time = delta_time
        self.socket_log.write(server["host"] + ":"+str(delta_time) + "\n")
        self.lock_socket_log.release()


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
            socket_times.append(socket_time)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def put(self, key, value, placement, insert=False):
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

        q1_dc_list = placement["Q1"]
        q2_dc_list = placement["Q2"]

        dataceneters_list      = list(set(q1_dc_list) | set(q2_dc_list))
        # if INSERT, assume first write value ==> don't neeed to get timestamp from servers
        
        if insert:
            # timstamp format 0-client_id
            timestamp = Timestamp.create_new_timestamp(self.id)
            #sever_list = q1uq2
            '''
            if self.manual_servers and self.placement_policy.__name__ == "Manual":
                server_list = self.manual_servers
            else:
                # It shouldn't be here if manual policy is used.
                # It will throw exception in that case. NOT CATCHING IT AS PROGRAM SHOULD STOP.
                server_list = self.placement_policy.get_dc(self.total_nodes,
                                                           self.data_center.get_datacenter_list(),
                                                           self.local_datacenter_id,
                                                           key)
            '''
        else:
            start_time = time.time()
            try:
                timestamp, socket_times = self.get_timestamp(key, q1_dc_list)
            except Exception as e:
                return {"status": "TimeOut", "message": "Timeout during get timestamp call of ABD"}
            end_time = time.time()
            self.lock_latency_log.acquire()
            max_socket_time = max(socket_times)
            delta_time = int((end_time - start_time)*1000)
            q1_time = delta_time - max_socket_time
            self.latency_log.write("put:Q1:" + str(q1_time) + "\n")
            self.lock_latency_log.release()
            timestamp = Timestamp.increment_timestamp(timestamp, self.id)
        ##################################################################
        #TODO: Test this condition  
        if timestamp is None:
            return {"status": "Failure", "message": "Unable to get valid time from the quorum"}

        ##################################################################
        


        # Step2 : Send the message to put
        sem = threading.Barrier(len(q2_dc_list) + 1, timeout=self.timeout)
        lock = threading.Lock()

        output = []
        socket_times = []
        #XXX: this needs to be changed
        #new_server_list = self._get_closest_servers(server_list, self.write_nodes)
        

        start_time = time.time()
        for data_center_id in q2_dc_list:
            #assume single server in every datacenter
            for server_id in ["1"]:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                # Added the latency delay purely for porpuse of prototyping.
                # Use default value of 0 otherwise.
                thread_list.append(threading.Thread(target=self._put, args=(key,
                                                                            value,
                                                                            timestamp,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            socket_times,
                                                                            lock)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

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
            return {"status": "TimeOut", "message": "Timeout during put call of ABD"}

        lock.release()
        if insert:
            request_time = q2_time
        else:
            request_time = q1_time + q2_time
        return {"status": "OK", "timestamp": timestamp}, request_time


    def _get(self, key, sem, server, output, socket_times, lock, dc_id, delay=0):
      #  print("get time: " + str(delay))
      #  time.sleep(float(delay) * 0.001)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start_time = time.time()
        sock.connect((server["host"], int(server["port"])))
        end_time = time.time()

        self.lock_socket_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        socket_time = delta_time
        self.socket_log.write(server["host"] + ":" + str(delta_time) + "\n")
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
            data = data.decode("latin-1").split('+:--:+')
            data.append(dc_id)
            lock.acquire()
            output.append(data)
            socket_times.append(socket_time)
            lock.release()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass

        return


    def get(self, key, placement):
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
        q1_dc_list = placement["Q1"]
        q2_dc_list = placement["Q2"]
        
        thread_list = []

        sem = threading.Barrier(len(q1_dc_list) + 1, timeout=self.timeout)
        lock = threading.Lock()
        

        #
        # Step1: get the timestamp with recent value
        output = []
        socket_times = []
        
        # new_server_list = self._get_closest_servers(server_list, self.read_nodes)
        start_time = time.time()
        for data_center_id in q1_dc_list:
            #XXX: assume that there is only one server in every dc
            for server_id in ["1"]:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._get, args=(key,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            output,
                                                                            socket_times, 
                                                                            lock,
                                                                            data_center_id)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass


        sem.abort()
        lock.acquire()
        if (len(output) < len(q1_dc_list)):
            lock.release()
            return {"status": "TimeOut", "message": "Timeout during get call of ABD"}
        
        end_time = time.time()
        self.lock_latency_log.acquire()
           
        max_socket_time = max(socket_times)
        
        delta_time = int((end_time - start_time)*1000)
        #####
        q1_time = delta_time - max_socket_time
        #####
        self.latency_log.write("get:Q1:" + str(q1_time) + "\n")
        self.lock_latency_log.release()
        value, timestamp = self.get_final_value(output)
        lock.release()
       
        if self.is_all_timestamps_the_same(output, len(q1_dc_list)):
            return {"status": "OK", "value":value}, q1_time
        
        dcs_highest_timestamp = self.dcs_with_highest_timestamp(output, timestamp)
        q2_dc_list = list( set(q2_dc_list) - set(dcs_highest_timestamp) )
        #TODO: check if all timestamp recived is the same ==> then no need to do phase 2



        #XXX: need to change this condition
        #if timestamp == "0":
        #    return {"status": "Error", "message": "No timestap found. Check quorum again"}

        # After abort can't use the same barrier
        sem = threading.Barrier(len(q2_dc_list) + 1, timeout=self.timeout)
        result = []
        socket_times = []
        #new_server_list = self._get_closest_servers(server_list, self.write_nodes)
        start_time = time.time()
        for data_center_id in q2_dc_list:
            #XXX: assume single server in every dc
            for server_id in ["1"]:
                server_info = self.data_center.get_server_info(data_center_id, server_id)
                thread_list.append(threading.Thread(target=self._put, args=(key,
                                                                            value,
                                                                            timestamp,
                                                                            sem,
                                                                            copy.deepcopy(server_info),
                                                                            result,
                                                                            socket_times,
                                                                            lock)))
                thread_list[-1].deamon = True
                thread_list[-1].start()

        try:
            sem.wait()
        except threading.BrokenBarrierError:
            pass
        end_time = time.time()
        self.lock_latency_log.acquire()
        delta_time = int((end_time - start_time)*1000)
        max_socket_time = max(socket_times)
        q2_time = delta_time - max_socket_time
        self.latency_log.write("get:Q2:" + str(q2_time)+ "\n")
        self.lock_latency_log.release()

        sem.abort()
        request_time = q1_time + q2_time

        return {"status": "OK", "value": value}, request_time 


#    def get_logger(self, tag):
#        logger_ = logging.getLogger(tag)
#        logger_.setLevel(logging.INFO)
#        logger_.addHandler(logstash.TCPLogstashHandler("host-ip", 8080,  version=1))
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
