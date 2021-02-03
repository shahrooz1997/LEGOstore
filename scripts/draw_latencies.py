#!/usr/bin/python3

import command
import subprocess
from subprocess import PIPE
import threading
import os
from time import sleep
from signal import signal, SIGINT
import urllib.request
import argparse
from numpy import *
import matplotlib.pyplot as plt

path = "data/CAS_NOF"


keys = ["222221"] #, "222222", "222223", "222224", "222225", "222226", "222227", "222228", "222229", "222230"]#, "222222", "222223"]
servers = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]

def read_operation_for_key(key, log_path):
    log_files_names = [os.path.join(log_path, f) for f in os.listdir(log_path) if os.path.isfile(os.path.join(log_path, f))]
    get_operations = []
    put_operations = []
    for file in log_files_names:
        log = open(file, 'r')
        lines = log.readlines()
        for line in lines:
            words = line.split()
            if (words[2][:-1] != key):
                continue

            if (words[1][:-1] == "get"):
                latency = (int(words[5]) - int(words[4][:-1])) / 1000
                get_operations.append([int(words[4][:-1]), latency, file])

            if (words[1][:-1] == "put"):
                latency = (int(words[5]) - int(words[4][:-1])) / 1000
                put_operations.append([int(words[4][:-1]), latency, file])

    get_operations.sort(key=lambda x: x[0])
    put_operations.sort(key=lambda x: x[0])

    return get_operations, put_operations

def mean_server_response_time(server):
    log_path = os.path.join(path, server)
    log_files_names = [os.path.join(log_path, f) for f in os.listdir(log_path) if
                       os.path.isfile(os.path.join(log_path, f))]
    for file in log_files_names:
        if file.find("server_") != -1 and file.find("_output.txt", file.find("server_") + 7):
            log_file = file
            break

    # log_file = os.path.join(log_path, server_file)

    data = []
    log = open(log_file, 'r')
    lines = log.readlines()
    flag = 0
    for i in range(len(lines) - 4):
        if(lines[i].find("[src/Data_Server.cpp][reconfig_query]73: started") != -1):
            flag = 5
        if (lines[i].find("[src/Server_driver.cpp][message_handler]") != -1 and \
                lines[i + 1].find(":0040: started") != -1):
            val = int(lines[i + 4][lines[i + 4].find("elapsed time = ") + 15:-7])
            if flag <= 0:
                data.append(val)
            if flag > 0 and val < 1000:
                flag -= 1
            # if(data[-1] > 1000):
            #     print(data[-1])

        # print(flag)

    print("mean_server_response_time without recon: " + str(sum(data) / len(data) / 1000) + "ms")


def plot_one_key_one_server(key, server):
    log_path = os.path.join(path, server)
    log_path = os.path.join(log_path, "logs")
    get_operations, put_operations = read_operation_for_key(key, log_path)

    plots = []
    plots.append(plt.figure())
    x = [(a[0] - get_operations[0][0]) / 1000 for a in get_operations]
    y = [a[1] for a in get_operations]
    plt.scatter(x, y, color="b")
    # plt.plot(y, '.-g')
    plt.title(server + ":get_operations")
    plt.xlabel('time(ms)')
    plt.ylabel('latency(ms)')
    plt.grid(True)

    plots.append(plt.figure())
    x = [(a[0] - put_operations[0][0]) / 1000 for a in put_operations]
    y = [a[1] for a in put_operations]
    plt.scatter(x, y, color="g")
    plt.title(server + ":put_operations")
    plt.xlabel('time(ms)')
    plt.ylabel('latency(ms)')
    plt.grid(True)

def print_one_key_one_server(key, server):
    log_path = os.path.join(path, server)
    log_path = os.path.join(log_path, "logs")
    get_operations, put_operations = read_operation_for_key(key, log_path)

    # for op in get_operations:
    #     print(op[0], op[1], op[2])

    for op in put_operations:
        print(op[0], op[1], op[2])

def plot_reconfiguration_latencies():
    log_path = os.path.join(path, "s7-cont")
    log_file = os.path.join(log_path, "controller_output.txt")

    data = []
    log = open(log_file, 'r')
    lines = log.readlines()
    for i in range(len(lines) - 8):
        if(lines[i].find("[src/Reconfig.cpp][reconfig_one_key]") != -1 and \
                lines[i + 1].find(":0307: started") != -1):
            key = lines[i + 1][lines[i + 1].find("for key ")+8:-1]
            data.append([key, int(int(lines[i + 8][lines[i + 8].find("elapsed time = ")+15:-7]) / 1000)])

    data.sort(key=lambda x: x[0])

    plt.figure()
    x = [a[0] for a in data]
    y = [a[1] for a in data]
    plt.bar(x, y, color="y")
    plt.title("Reconfiguration Latency")
    plt.xlabel('key id')
    plt.ylabel('latency(ms)')
    plt.grid(True)

    # for x in data:
    #     print(x[0], x[1])


def main():
    file1 = open('data/CAS_NOF/logs/logfile_403177472.txt', 'r')
    # file1 = open('data/CAS_NOF/logs/logfile_403177472.txt', 'r')
    lines = file1.readlines()

    plots = []
    i = 1;
    for key in keys:
        y = []
        for line in lines:
            words = line.split()
            if(words[1][:-1] != "get"):
                continue
            if(words[2][:-1] != key):
                continue
            latency = (int(words[5]) - int(words[4][:-1])) / 1000
            print(latency)
            y.append(latency)

        x = range(len(y))

        plots.append(plt.figure(i))
        plt.scatter(x, y)
        # plt.show()



        # plot1 = plt.figure(1)
        #
        # plt.plot(x1, y1)
        #
        # plot2 = plt.figure(2)
        #
        # plt.plot(x2, y2)
        i += 1

    plt.show()


if __name__ == "__main__":
    # main()

    plot_reconfiguration_latencies()

    # plot_one_key_one_server(keys[0], "s4")
    # plot_one_key_one_server(keys[0], "s5")
    plot_one_key_one_server(keys[0], "s6")

    # print_one_key_one_server(keys[0], "s6")

    # mean_server_response_time("s2")

    plt.show()