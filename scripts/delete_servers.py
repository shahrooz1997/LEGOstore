#!/usr/bin/python3

import os
import sys
import threading
import subprocess
from subprocess import PIPE

def get_uris():
	proc = subprocess.run(["gcloud", "compute", "instances", "list", "--uri"], stdout=PIPE, stderr=PIPE)
	if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
		print(proc.stderr.decode("utf-8"))
		raise Exception("Error in getting uris")

	uris = proc.stdout.decode("utf-8").split("\n")

	try:
		uris.remove("")
	except:
		pass
	return uris


def delete_server(uri):
	proc = subprocess.run(["gcloud", "compute", "instances", "delete", uri], stdin=PIPE, stdout=PIPE, stderr=PIPE)
	# proc.stdin.write(b'Y\n')
	if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
		print(proc.stderr.decode("utf-8"))
		raise Exception("Error in deleting server " + uri)

def delete_all_servers(uris):
	threads = []
	for uri in uris:
		threads.append(threading.Thread(target=delete_server, args=(uri,)))
		threads[-1].start()

	for thread in threads:
		thread.join()

if __name__ == "__main__":
	uris = get_uris()
	delete_all_servers(uris)

