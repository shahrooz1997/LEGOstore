#!/usr/bin/python3

import start_servers
import subprocess
from subprocess import PIPE

class Machine:

	existing_info = ''

	def __init__(self, **kwargs):
		self.name = kwargs['name']
		# self.type = kwargs['type']
		# self.zone = kwargs['zone']
		# if 'internal_ip' in kwargs and 'external_ip' in kwargs:
		# 	self.internal_ip = kwargs['internal_ip']
		# 	self.external_ip = kwargs['external_ip']
		# 	self.state = 1
		# else:
		self.state = 0
		self.set_ip_info()

	def set_ip_info(self):
		just_ran = False
		if Machine.existing_info == '':
			proc = subprocess.run(["gcloud", "compute", "instances", "list"], stdout=PIPE, stderr=PIPE)
			if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
				print(proc.stderr.decode("utf-8"))
				raise Exception("Error in gathering information ")
			Machine.existing_info = proc.stdout.decode("utf-8")
			just_ran = True
			print(Machine.existing_info)
		
		found = False
		for line in Machine.existing_info.split('\n'):
			line = " ".join(line.split())
			if self.name == line.split(' ')[0]:
				self.zone = line.split(' ')[1]
				self.type = line.split(' ')[2]
				self.internal_ip = line.split(' ')[3]
				self.external_ip = line.split(' ')[4]
				self.state = 1
				found = True
				break

		if not found and not just_ran:
			proc = subprocess.run(["gcloud", "compute", "instances", "list"], stdout=PIPE, stderr=PIPE)
			if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
				print(proc.stderr.decode("utf-8"))
				raise Exception("Error in gathering information ")
			Machine.existing_info = proc.stdout.decode("utf-8")
			# print(Machine.existing_info)

			for line in Machine.existing_info.split('\n'):
				line = " ".join(line.split())
				if self.name == line.split(' ')[0]:
					self.zone = line.split(' ')[1]
					self.type = line.split(' ')[2]
					self.internal_ip = line.split(' ')[3]
					self.external_ip = line.split(' ')[4]
					self.state = 1
					found = True
					break

		if not found:
			if(self.state == 0): # The machine does not exist
				start_servers.start_server(self.name, self.type, self.zone)
				self.state = 1
			proc = subprocess.run(["gcloud", "compute", "instances", "list"], stdout=PIPE, stderr=PIPE)
			if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
				print(proc.stderr.decode("utf-8"))
				raise Exception("Error in gathering information ")
			Machine.existing_info = proc.stdout.decode("utf-8")

			for line in Machine.existing_info.split('\n'):
				line = " ".join(line.split())
				if self.name == line.split(' ')[0]:
					self.zone = line.split(' ')[1]
					self.type = line.split(' ')[2]
					self.internal_ip = line.split(' ')[3]
					self.external_ip = line.split(' ')[4]
					self.state = 1
					found = True
					break
		if not found:
			raise Exception("Error in creating server and setting infor for " + name)

	def __str__(self):
		ret = ""
		ret += '"'
		ret += self.name[1]
		ret += '": {\n'
		ret += '    "metadata_server": {\n'
		ret += '        "host": "'
		ret += self.external_ip
		ret += '",\n'
		ret += '        "port": "30000"\n    },\n    "servers": {\n        "1": {\n            "host": "'
		ret += self.external_ip
		ret += '",\n            "port": "10000"\n        }\n    }\n}'
		return ret




def check_if_server_exist():
	pass

def dump_machines_ip_info(machines):
	file = open('setup_config.json', 'w')
	file.write('{\n')
	for machine in machines[:-1]:
		machine_info = machine.__str__()
		for line in machine_info.split('\n')[:-1]:
			file.write('    ')
			file.write(line)
			file.write('\n')
		file.write('    ')
		file.write(machine_info.split('\n')[-1])
		file.write(',\n')

	machine_info = machines[-1].__str__()
	for line in machine_info.split('\n'):
		file.write('    ')
		file.write(line)
		file.write('\n')
	# file.write('    ')
	# file.write(line)

	file.write('}')
	file.close()


# start_servers.main()
machines = []
for i in range(1, 10):
	name = 's' + str(i)
	machines.append(Machine(name=name))

dump_machines_ip_info(machines)

# print(machines[0])