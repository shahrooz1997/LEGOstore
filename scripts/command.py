# !/usr/bin/python3

import os
import sys
import threading
import subprocess
from subprocess import PIPE

gcloud = "/opt/google-cloud-sdk/bin/gcloud"
make = "/usr/bin/make"


# Reading zones to create the servers
def read_zones(zones_file_name='zones.txt'):
    zones_file = open(zones_file_name, "r")
    zones = []
    counter = 0
    for line in zones_file.readlines():
        # print(line)
        if (line[0] != "#" and line[0] != "\n"):
            zones.append(line[0:line.index(' ')]);
        if (line[0] == "\n"):
            counter += 1
            if (counter == 2):
                break
        else:
            counter = 0

    return zones


def start_server(name, machine_type, zone):
    proc = subprocess.run(
        [gcloud, "compute", "instances", "create", name, "--machine-type=" + machine_type, "--zone=" + zone],
        stdout=PIPE, stderr=PIPE)
    if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
        print(proc.stdout.decode("utf-8"), "\n---\n", proc.stderr.decode("utf-8"))
        raise Exception("Error in creating server " + name)


def get_instances_info():
    proc = subprocess.run([gcloud, "compute", "instances", "list"], stdout=PIPE, stderr=PIPE)
    if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
        print(proc.stdout.decode("utf-8"), "\n---\n", proc.stderr.decode("utf-8"))
        raise Exception("Error in gathering information ")
    return proc.stdout.decode("utf-8")


def execute(name, zone, cmd):
    proc = subprocess.run([gcloud, "compute", "ssh", name, "--zone", zone, "--command", cmd], stdout=PIPE, stderr=PIPE)
    if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
        print(proc.stdout.decode("utf-8"), "\n---\n", proc.stderr.decode("utf-8"))
        raise Exception("Error in gathering information ")
    return proc.stdout.decode("utf-8"), proc.stderr.decode("utf-8")


def scp_from(name, zone, file_from, file_to):
    proc = subprocess.run([gcloud, "compute", "scp", "--zone", zone, name + ":" + file_from, file_to], stdout=PIPE,
                          stderr=PIPE)
    if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
        print(proc.stdout.decode("utf-8"), "\n---\n", proc.stderr.decode("utf-8"))
        raise Exception("Error in gathering information ")


def scp_to(name, zone, file_from, file_to):
    proc = subprocess.run([gcloud, "compute", "scp", "--zone", zone, file_from, name + ":" + file_to], stdout=PIPE,
                          stderr=PIPE)
    if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
        print(proc.stdout.decode("utf-8"), "\n---\n", proc.stderr.decode("utf-8"))
        raise Exception("Error in gathering information ")


def disk_resize(name, zone, size_gb):
    proc = subprocess.run(
        [gcloud, "compute", "disks", "resize", name, "--size=" + str(size_gb), "--zone=" + zone, "-q"], stdout=PIPE,
        stderr=PIPE)
    if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
        print(proc.stdout.decode("utf-8"), "\n---\n", proc.stderr.decode("utf-8"))
        raise Exception("Error in gathering information ")


def delete_server(name, zone):
    proc = subprocess.run(["gcloud", "compute", "instances", "delete", name, "--zone", zone], stdin=PIPE, stdout=PIPE,
                          stderr=PIPE)
    if proc.stderr.decode("utf-8").find("ERROR") != -1 or proc.stdout.decode("utf-8").find("ERROR") != -1:
        print(proc.stdout.decode("utf-8"), "\n---\n", proc.stderr.decode("utf-8"))
        raise Exception("Error in deleting server " + name)


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


def main(zones_file_name="zones.txt"):
    zones = read_zones(zones_file_name)

    start_all_servers(zones)


if __name__ == "__main__":
    if (len(sys.argv) == 2):
        zones_file_name = sys.argv[1]
    else:
        zones_file_name = "zones.txt"
    main(zones_file_name)
    print("Done")
