import socket
import json
import time
import copy
import glob
import sys
from protocol import Protocol
from data_center import Datacenter
from datetime import datetime
from multiprocessing import Process
import uuid
import threading
from workload import Workload

import logging
from Performance import *


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

def get_logger(log_path):
	logger_ = logging.getLogger('log')
	logger_.setLevel(logging.INFO)
	handler = logging.FileHandler(log_path)
	# XXX: TODO: Check if needed
	handler.setFormatter(logging.Formatter('%(message)s'))
	logger_.addHandler(handler)
	return logger_

def run_session(workload, properties, running_time, session_id=None):

	if not session_id:
		session_id = uuid.uuid4().int
	filename = "output_" + str(session_id) + ".txt"
	output_file = open(filename, "a+")
	client = Client(properties, session_id)
	request_count = 0
	_end_time = time.time() + running_time
	while time.time() < _end_time:
		inter_arrival_time, request_type, key, value = workload.next()
		start_time = time.time()
		if request_type == "insert":
			continue
		elif request_type == "put":
			output = key + json.dumps(client.put(key, value))
		else:
			output = key + json.dumps(client.get(key))

		# Since each process has its own file no need to even flush it.
		# flushing the file is for monitoring the status
		output_file.write(request_type + ":" + str(output) + "\n")
		output_file.flush()
		request_time = time.time() - start_time
		if request_time < inter_arrival_time * 0.001:
			#interarrival time is in ms
			time.sleep(inter_arrival_time * 0.001 - request_time)

		request_count += 1

	output_file.close()

def merge_files(files_to_combine, output_file_name):
    # Merge quorum latency files
    allfiles = glob.glob(files_to_combine)
    combined_file_name = properties["local_datacenter"] + "_" + output_file_name
    with open(combined_file_name, "wb") as combined_file:
    	for f in allfiles:
    		with open(f, "rb") as infile:
    			combined_file.write(infile.read())
    return


if __name__ == "__main__":
	properties = json.load(open("client_config.json"))
	#client = Client(properties, properties["local_datacenter"])

	performance_model = None
	if properties["classes"]["default_class"]:
		performance_model = Performance_ABD(properties)


	# Parse trace file
	# Trace corrosponds to points [ 'time', 'arrival rate per second' ]
	trace = []
	trace_file = open("temp_trace.dat")
	for line in trace_file.readlines():
		p = line.strip('\n').split(' ')
		trace.append([int(p[0]),int(p[1])])
	trace_file.close()
	
	# TODO: get values from client config file
	read_ratio = 0.9
	write_ratio = 0.1
	insert_ratio = 0.0
	total_number_of_requests = int(sum(row[1] for row in trace))
	current_trace_time = 0
	initial_count = 10
	value_size = 100
	arrival_process = "poisson"

	start_time = time.time()
	process_list = []
	# XXX: ASSUMPTION: IN WORST CASE IT TAKES 1 SECOND TO SERVE THE REQUEST
	workload = Workload(arrival_process, 1, read_ratio, write_ratio, insert_ratio, initial_count, value_size)
	current_trace_time = 0
	for point in trace:
		next_trace_time = point[0]
		running_time = next_trace_time - current_trace_time
		arrival_rate = point[1]
		number_of_requests = arrival_rate*(next_trace_time - current_trace_time)
        # Client creates sessions that sends 1 request per second
		# Process are used instead of threads to prevent from bottlenecks when writing to a single file
		for i in range(arrival_rate):
			client_uid = properties['local_datacenter'] + str(i)
			process_list.append(Process(target=run_session, args=(workload,
																  properties,
																  running_time,
																  client_uid)))


		for process in process_list:
			process.start()

		for process in process_list:
			process.join()
		
		process_list.clear()
		current_trace_time = next_trace_time
	    
		performance_model.calculateLatencies(arrival_rate,value_size)
        
        # Merge socket connection recorded Time
		merge_files("output_*.txt", "output.txt")
	    # Merge coding time files if protocol is Viveck's
		if properties["classes"]["default_class"] == "Viveck_1":
			merge_files("coding_times_*.txt", "coding_times.txt")
	

	client_time = time.time() - start_time
	print("Client Time: ", client_time)
