import socket
import json
import time
import copy

from protocol import Protocol
from data_center import Datacenter
from datetime import datetime
from multiprocessing import Process
import uuid
import threading
from workload import Workload

import logging


class Client:
    def __init__(self, properties, _id=None):
        ''' Setting up basic properties of the server
        '''
        self.class_properties = properties["classes"]
        self.local_datacenter = properties["local_datacenter"]

        self.datacenter_list = properties["datacenters"]

        # Same LRU data structure can be used for the same but for now using dictionary
        # https://stackoverflow.com/questions/7143746/difference-between-memcache-and-python-dictionary
        self.local_cache = {}

        self.class_name_to_object = {}
        self.default_class = self.class_properties["default_class"]
        self.metadata_server = self.datacenter_list[self.local_datacenter]["metadata_server"]
        # TODO: Update Datacenter to store local datacenter too
        self.datacenter_info = Datacenter(self.datacenter_list)

        if _id == None:
            self.id = uuid.uuid4().hex
        else:
            self.id = str(_id)
        self.retry_attempts = int(properties["retry_attempts"])
        self.metadata_timeout = int(properties["metadata_server_timeout"])

        self.latency_between_DCs = properties["latency_between_DCs"]
        self.dc_cost = properties["DC_cost"]

        self.initiate_key_classes()


    def initiate_key_classes(self):
        for class_name, class_info in self.class_properties.items():
            self.class_name_to_object[class_name] = Protocol.get_class_protocol(class_name,
                                                                                class_info,
                                                                                self.local_datacenter,
                                                                                self.datacenter_info,
                                                                                self.id,
                                                                                self.latency_between_DCs,
                                                                                self.dc_cost)
        return


    def check_validity(self, output):
        # Checks if the status is OK or not
        # @ Returns true pr false based on the status

        if output["status"] == "OK":
            return True

        return False


    def look_up_metadata(self, key):
        ######################
        # Internal call for the client
        # @ This method searches the metadata(class, server_list for the key.
        # @ Returns tuple (class_name, server_list ) if exist else return (None, None)
        # @ Raises Exception in case of timeout or socket error
        ######################
        if key in self.local_cache:
            data = self.local_cache[key]
            return (data["class_name"], data["server_list"])

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.metadata_server["host"], int(self.metadata_server["port"])))
        data = json.dumps({"method": "get_metadata", "key": key})

        sock.sendall(data.encode("utf-8"))
        sock.settimeout(self.metadata_timeout)

        try:
            data = sock.recv(640000)
        except socket.error:
            raise Exception("Unbale to get value from metadata server")
        else:
            data = data.decode("utf-8")
            data = json.loads(data)
            if data["status"] == "OK":
                self.local_cache[key] = data["value"]
                return (data["value"]["class_name"], data["value"]["server_list"])

        return (None, None)


    def insert(self, key, value, class_name=None):
        ######################
        # Insert API for client
        # @ This API is for insert or first put for a particular key.
        # @ We assume that client has this prior knowledge if its put or insert.
        #
        # @ Returns dict with {"status" , "message" } keys
        ######################

        if not class_name:
            class_name = self.default_class

        total_attempts = self.retry_attempts

        # Step1 : Putting key, value as per provided class protocol
        while total_attempts:
            output = self.class_name_to_object[class_name].put(key, value, None, True)

            if self.check_validity(output):
                break

            total_attempts -= 1

        if not total_attempts:
            return {"status": "FAILURE",
                    "message": "Unable to insert message after {0} attempts".format(
                        self.retry_attempts)}

        return {"status": "OK"}
#        # Step2 : Insert the metadata into the metadata server
#        total_attempts = self.retry_attempts
#        server_list = output["server_list"]
#        meta_data = {"class_name": class_name, "server_list": server_list}
#
#        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#        try:
#            sock.connect((self.metadata_server["host"], int(self.metadata_server["port"])))
#        except:
#            return {"status": "FAILURE",
#                    "message": "Unable to connect the socket to metadata server"}
#
#        data = json.dumps({"method": "set_metadata",
#                           "key": key,
#                           "value": meta_data})
#
#        while total_attempts:
#            sock.send(data.encode("utf-8"))
#
#            # Setting up timeout as per class policies
#            sock.settimeout(self.metadata_timeout)
#            try:
#                data = sock.recv(640000)
#                data = json.loads(data.decode("utf-8"))
#            except socket.error:
#                print("Failed attempt to send data to metadata server...")
#            else:
#                if data["status"] == "OK":
#                    # TODO: Check once if the key is required else remove it
#                    self.local_cache[key] = meta_data
#                    return {"status" : "OK"}
#
#            total_attempts -= 1
#
#        return {"status": "Failure",
#                "message": "Unable to insert message after {0} attemps into the metadataserver".format(
#                    self.retry_attempts)}


    def put(self, key, value):
        ######################
        # Put API for client
        # @ This API is for put key and value based on assinged key.
        # @ Returns dict with {"status" , "message" } keys
        ######################

        # Step1: Look for the class details and server details for key.
        try:
        # TODO: Last moment changes to disable metadata server
            class_name = self.default_class
            server_list = copy.deepcopy(self.class_properties[self.default_class]["manual_dc_server_ids"])
            #class_name, server_list = self.look_up_metadata(key)
        except Exception as e:
            return {"status": "FAILURE", "message" : "Timeout or socket error for getting metadata" + str(e)}

        if not class_name or not server_list:
            return {"status": "FAILURE", "message" : "Key doesn't exist in system"}

        # Step2: Call the relevant protocol for the same.
        total_attempts = self.retry_attempts
        while total_attempts:
            output = self.class_name_to_object[class_name].put(key, value, server_list)
            if self.check_validity(output):
                return {"status" : "OK"}

            total_attempts -= 1

        return {"status": "FAILURE",
                "message": "Unable to put message after {0} attemps".format(self.retry_attempts)}


    def get(self, key):
        ######################
        # Get API for client
        # @ This API is for get key based on assigned class.
        # @ Returns dict with {"status" , "value" } keys
        ######################

        # Step1: Get the relevant metadata for the key
        try:
            class_name = self.default_class
            server_list = copy.deepcopy(self.class_properties[self.default_class]["manual_dc_server_ids"])
            #class_name, server_list = self.look_up_metadata(key)
        except Exception as e:
            return {"status": "FAILURE", "message" : "Timeout or socket error for getting metadata" + str(e)}

        if not class_name or not server_list:
            return {"status": "FAILURE", "message": "Key doesn't exist in system"}

        total_attempts = self.retry_attempts

        # Step2: call the relevant protocol for the same
        while total_attempts:
            output = self.class_name_to_object[class_name].get(key, server_list)

            if self.check_validity(output):
                return {"status": "OK", "value": None}

            total_attempts -= 1

        return {"status": "FAILURE",
                "message": "Unable to put message after {0} attemps".format(
                    self.retry_attempts)}



# def call(key, value):
#     print(json.dumps(client.put(key, str(value))))
#
#
# def thread_wrapper(method, latency_file, output_logger, lock, key, value=None):
#     # We will assume fixed class for wrapper for now
#
#     output = method + key + "Empty"
#     a = datetime.now()
#     if method == "insert":
#         return
#     if method == "put":
#         output  = key + json.dumps(client.put(key, value))
#     else:
#         output = key + json.dumps(client.get(key))
#   #  else:
#    #     output = "Invalid Call"
#     b = datetime.now()
#     c = b - a
#
#     # Flush every latency on disk. May not be a good idea in long term.
#     lock.acquire()
#     latency_file.write(method + ":" + str(c.seconds * 1000 + c.microseconds * 0.001) + '\n')
#     latency_file.flush()
#     output_logger.info(method + ":" + str(output))
#     lock.release()
#
#     return
#

def get_logger(log_path):
    logger_ = logging.getLogger('log')
    logger_.setLevel(logging.INFO)
    handler = logging.FileHandler(log_path)
    # XXX: TODO: Check if needed
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger_.addHandler(handler)
    return logger_

def run_session(arrival_rate, workload, properties, experiment_duration, session_id=None):


    if not session_id:
        session_id = uuid.uuid4().int


    filename = "output_" + str(session_id) + ".txt"
    output_file = open(filename, "w")
    client = Client(properties, session_id)
    request_count = 0

    while request_count < arrival_rate * experiment_duration:
        inter_arrival_time, request_type, key, value = workload.next()

        start_time = time.time()
        if request_type == "insert":
            continue
        elif request_type == "put":
            output = key + json.dumps(client.put(key, value))
        else:
            output = key + json.dumps(client.get(key))

        # Since each process has its own file no need to even flush it.
        output_file.write(request_type + ":" + str(output) + "\n")
        output_file.flush()
        request_time = time.time() - start_time

        if request_time < arrival_rate:
            # Multiply with 1.0 just to make sure float ubstraction happens (not int float conflicts)
            time.sleep(arrival_rate * 1.0 - request_time)

        request_count += 1


    output_file.close()


if __name__ == "__main__":
    properties = json.load(open('client_config.json'))
    #client = Client(properties, properties["local_datacenter"])

    # General properties for this group of requests
    arrival_rate = 22
    experiment_duration = 10800
    read_ratio = 0.9
    write_ratio = 0.1
    insert_ratio = 0
    initial_count = 990000
    value_size = 10000

    #workload = Workload("uniform", arrival_rate, read_ratio, write_ratio, insert_ratio, initial_count, value_size)
    ## don't need latency file, let class generate it instead
    #latency_file = open("latency.txt", "w+")
    #lock = threading.Lock()
    #each each client has its own request count and output log
    #request_count = 0
    #output_logger = get_logger("output.log")

    # socket_logger = get_logger("socket_times.log")
    # individual_logger = get_logger("individual_times.log")

    process_list = []
    # XXX: ASSUMPTION: IN WORST CASE IT TAKES 1 SECOND TO SERVE THE REQUEST
    workload = Workload("uniform", 1, read_ratio, write_ratio, insert_ratio, initial_count, value_size)

    for i in range(arrival_rate):
        client_uid = properties['local_datacenter'] + str(i)
        process_list.append(Process(target=run_session, args=(1,
                                                              workload,
                                                              properties,
                                                              experiment_duration,
                                                              client_uid)))
    for process in process_list:
        process.start()

    for process in process_list:
        process.join()

    # Merge quorum latency files
    files_to_combine = "individual_times_*.txt"
	individual_times_files = glob.glob(files_to_combine)
	combined_file_name = properties["local_datacenter"] + "_individual_times.txt"
	with open(combined_file_name, "wb") as latency_file:
		for f in individual_times_files:
			with open(f, "rb") as infile:
				latency_file.write(infile.read())

    # Merge socket connection recorded Time
    files_to_combine = "socket_times_*.txt"
	socket_files = glob.glob(files_to_combine)
	combined_file_name = properties["local_datacenter"] + "_socket_times.txt"
	with open(combined_file_name, "wb") as socket_file:
		for f in socket_files:
			with open(f, "rb") as infile:
				socket_file.write(infile.read())

    # Merge coding time files if protocol is Viveck's
    if self.default_class != "ABD":
        files_to_combine = "coding_times_*.txt"
        coding_files = glob.glob(files_to_combine)
        combined_file_name = properties["local_datacenter"] + "_coding_times.txt"
        with open(combined_file_name, "wb") as coding_file:
        	for f in coding_files:
        		with open(f, "rb") as infile:
        			coding_file.write(infile.read())



#  XXX: Previous code : TODO: Delete it once we know new one is working
#        inter_arrival_time, request_type, key, value = workload.next()
#        time.sleep(inter_arrival_time * 0.001)
#
#        new_thread = threading.Thread(target = thread_wrapper, args=(request_type,
#                                                                     latency_file,
#                                                                     output_logger,
#                                                                     lock,
#                                                                     key,
#                                                                     value))
#
#        request_count += 1
#        new_thread.start()

#    while 1:
#         data = input()
#         method, key, value = data.split(",")
#         a = datetime.now()
#         if method == "insert":
#             print(json.dumps(client.insert(key, value, "ABD")))
#         elif method == "put":
#             print(json.dumps(client.put(key, value)))
#         elif method == "get":
#             print(json.dumps(client.get(key)))
#         else:
#             print("Invalid Call")
#         b = datetime.now()
#         c = b - a
#         print(c.seconds * 1000 + c.microseconds * 0.001)
#    # #
    # threadlist = []
    # for i in range(1,100):
    #     threadlist.append(threading.Thread(target=call, args=("chetan", i,)))
    #     threadlist[-1].deamon = True
    #     threadlist[-1].start()
    #
    # for i in range(1,100):
    #     threadlist[i].join()
