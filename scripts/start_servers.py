#!/usr/bin/python3

import os
import sys
import threading
import subprocess
from subprocess import PIPE

# Reading zones to create the servers
def read_zones(zones_file_name):
	zones_file = open(zones_file_name, "r")
	zones = []
	counter = 0
	for line in zones_file.readlines():
		# print(line)
		if(line[0] != "#" and line[0] != "\n"):
			zones.append(line[0:line.index(' ')]);
		if(line[0] == "\n"):
			counter += 1
			if(counter == 2):
				break
		else:
			counter = 0

	return zones


def start_server(name, machine_type, zone):
	proc = subprocess.run(["gcloud", "compute", "instances", "create", name, "--machine-type=" + machine_type, "--zone=" + zone], stdout=PIPE, stderr=PIPE)
	if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
		print(proc.stderr.decode("utf-8"))
		raise Exception("Error in creating server " + name)

def start_all_servers(zones):
	threads = []
	i = 1
	for zone in zones:
		name = "s" + str(i)
		i += 1
		threads.append(threading.Thread(target=start_server, args=(name, "e2-standard-2", zone)))
		threads[-1].start()

	for thread in threads:
		thread.join()

if __name__ == "__main__":
	if(len(sys.argv) == 2):
		zones_file_name = sys.argv[1]
	else:
		zones_file_name = "zones.txt"
	zones = read_zones(zones_file_name)

	start_all_servers(zones)

	print("Done")
