import numpy as np
import glob
import os



def delete_session_files(properties, arrival_rate, running_time):
	if properties["classes"]["defaultclass"] == "ABD":
		os.system("rm calculated_latencies.txt output_* individual_times_* socket_times_* timestamp_order.json insert.log")
	else:
		os.system("rm calculated_latencies.txt output_* individual_times_* socket_times_* coding_times_* timestamp_order.json insert.log")
	
	if ans == 'y':
		os.system("rm *_individual_times.txt *_socket_times.txt *_coding_times.txt *_output.txt")




class Performance(object):
	def __init__(self):
		self.mean_get_latency = 0
		self.mean_put_latency = 0
		self._90_per_get = 0
		self._99_per_get = 0
		self._90_per_put = 0
		self._99_per_put = 0
		self.datacenter_latency = [
		[0,43,136,247,237,165,293,128,109],
		[43,0,169,280,261,186,319,156,144],
		[143,171,0,131,138,194,326,267,236],
		[241,269,177,0,25,90,214,148,162],
		[224,251,133,24,0,76,189,138,140],
		[161,186,194,92,79,0,126,71,83],
		[305,332,324,213,188,123,0,203,191],
		[122,150,253,149,139,68,197,0,24],
		[108,137,229,163,139,82,186,25,0]
		]

class Performance_ABD(Performance):
	def __init__(self, properties):
		super(Performance_ABD, self).__init__()
		self.get_q1 = []
		self.get_q2 = []
		self.put_q1 = []
		self.put_q2 = []	
		self.individual_time_file_name = "individial_times.txt"
		self.socket_time_file_name = "socket_times.txt"
		self.properties = properties
		self.datacenter_id = properties["local_datacenter"]
		# placement start from DC_id = 0
		
		self.total_nodes = int(properties["classes"]["ABD"]["quorum"]["total_nodes"])
		self.size_q1 = int(properties["classes"]["ABD"]["quorum"]["read_nodes"])
		self.size_q2 = int(properties["classes"]["ABD"]["quorum"]["write_nodes"])
		placement_dict = self.properties["classes"]["ABD"]["manual_dc_server_ids"]  
		placement = []
		for DC_id in placement_dict:
			placement.append(int(DC_id) - 1)
		self.placement = placement
		

		#file to log the arrival rate, size of packets, and the latency
		self.performance_error_log = open("performance_error_log.txt", "w+")
		self.calculation_file = open("calculated_latencies.txt", "a+")
		self._90_per_calculation_file = open("_90_per_calculated_latencies.txt", "a+")
		self._99_per_calculation_file = open("_99_per_calculated_latencies.txt", "a+")
		return
	

	
	def calculateLatencies(self,arrival_rate, value_size,  filenames = ["individual_times.txt", "socekt_times.txt"]):
		if len(filenames) == 0:
			return

		self.clear_results()

		self.merge_result_files()
		socket_cost = self.calculate_socket_costs()
						
		_possible_latencies = []
		for _dc_id, latency  in enumerate(self.datacenter_latency[int(self.datacenter_id) - 1]):
			if _dc_id in self.placement:
				_possible_latencies.append((_dc_id, latency))
		possible_latencies = sorted(_possible_latencies, key = lambda x: x[1])

		combined_file_name = self.datacenter_id + "_individual_times.txt"
		latency_file = open(combined_file_name)
		for line in latency_file.readlines():
			p = line.split(':')
			if p[0] == "get":
				if p[1] == "Q1":
					distenation_dc_id = possible_latencies[self.size_q1 - 1][0]
					self.get_q1.append(int(p[2]) - socket_cost[distenation_dc_id])
				else:
					distenation_dc_id = possible_latencies[self.size_q2 - 1][0]
					self.get_q2.append(int(p[2])  - socket_cost[distenation_dc_id])
			else:
				if p[1] == "Q1":
					distenation_dc_id = possible_latencies[self.size_q1 - 1][0]
					self.put_q1.append(int(p[2]) - socket_cost[distenation_dc_id])
				else:
					distenation_dc_id = possible_latencies[self.size_q2 - 1][0]
					self.put_q2.append(int(p[2]) - socket_cost[distenation_dc_id])
		self.mean_get_latency = int(np.average(self.get_q1) + np.average(self.get_q2))
		self.mean_put_latency = int(np.average(self.put_q1) + np.average(self.put_q2))
		self._90_per_get = int(np.percentile(self.get_q1, 90) + np.percentile(self.get_q2,90))
		self._99_per_get = int(np.percentile(self.get_q1, 99) + np.percentile(self.get_q2,99))
		self._90_per_put = int(np.percentile(self.put_q1, 90) + np.percentile(self.put_q2,90))
		self._99_per_put = int(np.percentile(self.put_q1, 99) + np.percentile(self.put_q2,99))

		####### format: dc_id:arrival_rate:value_size(bytes):get_latency(ms):put_latency(ms) ######
		#log mean latencies
		_row = [self.datacenter_id, arrival_rate, value_size,\
						self.mean_get_latency, self.mean_put_latency]
		self.log_to_file(self.calculation_file, _row)

		#log 90th per latencies
		_row = [self.datacenter_id, arrival_rate, value_size,\
						self._90_per_get, self._90_per_put]
		self.log_to_file(self._90_per_calculation_file, _row)

		#log 99th per latencies
		_row = [self.datacenter_id, arrival_rate, value_size,\
						self._99_per_get, self._99_per_put]
		self.log_to_file(self._99_per_calculation_file, _row)
 

	def log_to_file(self,fd,list_of_info = []):
		if len(list_of_info) == 0:
			return
		for info in list_of_info:
			_row = str(info) + ":"
			fd.write(_row)
		fd.write("\n")
		return

	def calculate_socket_costs(self):		
		datacenters = self.properties["datacenters"]
		ip_to_id = {}
		for dc_id in datacenters.keys():
			dc = datacenters.get(dc_id)
			ip_to_id.update({dc["servers"]["1"]["host"]:int(dc_id)})

		combined_file_name = self.datacenter_id + "_socket_times.txt"
		socket_file = open(combined_file_name)
		socket_list = [[] for _ in range(9)]
		for line in socket_file.readlines():
			p = line.split(':')
			dc_index = ip_to_id[p[0]] - 1
			socket_list[dc_index].append(int(p[1]))
		socket_costs = []
		for i,s_list in enumerate(socket_list):
			## this means that is has never been contatcted
			average_socket = float('inf')
			if len(s_list) != 0:
				average_socket = int(np.average(s_list))
			socket_costs.append(average_socket)

		return socket_costs
		
	def merge_result_files(self):
		# Merge quorum latency files
		output_file_name = "individual_times.txt"
		files_to_combine = "individual_times_*.txt"
		self.merge_files(files_to_combine, output_file_name)

		#merge socket times
		output_file_name = "socket_times.txt"
		files_to_combine = "socket_times_*.txt"
		self.merge_files(files_to_combine, output_file_name)
		
		#merge output files
		output_file_name = "output.txt"
		files_to_combine = "output_*.txt"
		self.merge_files(files_to_combine, output_file_name)

		return

	def merge_files(self,files_to_combine, output_file_name):
		# Merge quorum latency files
		allfiles = glob.glob(files_to_combine)
		combined_file_name = self.datacenter_id + "_" + output_file_name
		with open(combined_file_name, "wb") as combined_file:
			for f in allfiles:
				with open(f, "rb") as infile:
					combined_file.write(infile.read())
		return
	
	def aggregate_results(self, arrival_rate, value_size):	
			
		self.merge_result_files()
		exprt_file_name = self.datacenter_id + "_" + str(arrival_rate) + "_" + str(value_size)
		result_directory = "ExpirementResults/" + exprt_file_name
		os.system("mkdir -p " + result_directory)
		cmd = "mv " + self.datacenter_id +"_* " + result_directory
		os.system(cmd)
		
		#TODO: create a member function to delete files specified by the performance
		os.system("rm individual_times_*.txt output_*.txt socket_times_*.txt coding_times_*.txt")
		
		return

	#use to clear all calculations before calculating new
	def clear_results(self):
		self.mean_get_latency = 0
		self.mean_put_latency = 0
		self._90_per_get = 0
		self._99_per_get = 0
		self._90_per_put = 0
		self._99_per_put = 0
		self.get_q1.clear()
		self.get_q2.clear()
		self.put_q1.clear()
		self.put_q2.clear()	
		








