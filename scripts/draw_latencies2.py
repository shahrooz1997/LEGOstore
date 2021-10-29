#!/usr/bin/python3

# Required packages: numpy matplotlib


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
import json
from collections import OrderedDict
from pylab import *

# Todo: I know read_operation_for_key is not optimized at all... It's late night and I am too tired to think of a better solution! (:

# path = "data/CAS_NOF_1"
path = "data/CAS_NOF"
# path = "data/CAS_NOF_ex2"
# path = "data/CAS_NOF_ex0_2"
# path = "data/LAST_RES/CAS_NOF_O->LO"
# path = "data/CAS_NOF_C"
# path = "data/arrival_rate/HR/CAS_NOF"
# path = "data/object_number/RW/CAS_NOF"
# path = "data/Findlimits/CAS_NOF_600_Success"
# path = "data/ARIF/CAS_NOF_HIGHDUR"
# path = "data/ARIF/CAS_NOF_HIGHDUR_OPTIMIZEDTRUE"
# path = "data/ARIF/CAS_NOF_HIGHDUR_3"
# path = "data/LAST_RES/CAS_NOF_One_hour_exec_for_latency_verifi"
# path = "data/arrival_rate/HW/CAS_NOF"
path


keys = ["20", "21", "22", "23", "24", "25", "26", "27", "28", "29"] #["2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010"]#, "222222", "222223"]
servers = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]

def read_operation_for_key(key, log_path, clients=None):
    log_files_names = [os.path.join(log_path, f) for f in os.listdir(log_path) if os.path.isfile(os.path.join(log_path, f))]
    get_operations = []
    put_operations = []

    if clients == None:
        clients = []
        for file in log_files_names:
            log = open(file, 'r')
            lines = log.readlines()
            words = lines[0].split()
            clients.append(words[0][:-1])
            log.close()
    else:
        for c in clients:
            get_operations.append([])
            put_operations.append([])

    start_timepoint = float("inf")


    for c in clients:
        get_operations.append([])
        put_operations.append([])

    for file in log_files_names:
        log = open(file, 'r')
        lines = log.readlines()
        for client_index, client in enumerate(clients):
            for line in lines:
                # print(file, line)
                words = line.split()

                start_time = int(words[4][:-1])
                if start_timepoint > start_time:
                    start_timepoint = start_time

                if (words[2][:-1] != key):
                    continue

                if (words[0][:-1] != client):
                    continue

                if (words[1][:-1] == "get"):
                    latency = (int(words[5]) - int(words[4][:-1])) / 1000
                    get_operations[client_index].append([int(words[4][:-1]), latency, file])

                if (words[1][:-1] == "put"):
                    latency = (int(words[5]) - int(words[4][:-1])) / 1000
                    put_operations[client_index].append([int(words[4][:-1]), latency, file])

                if latency > 5000:
                    print(file, line)

            get_operations[client_index].sort(key=lambda x: x[0])
            put_operations[client_index].sort(key=lambda x: x[0])

        log.close()

    # get_operations.sort(key=lambda x: x[0])
    # put_operations.sort(key=lambda x: x[0])

    get_operations_t = []
    put_operations_t = []
    for c in clients:
        get_operations_t.append([])
        put_operations_t.append([])

    for client_index, client in enumerate(clients):
        for el in get_operations[client_index]:
            get_operations_t[client_index].append([el[0] - start_timepoint, el[1], el[2]])
        for el in put_operations[client_index]:
            put_operations_t[client_index].append([el[0] - start_timepoint, el[1], el[2]])

    get_operations = get_operations_t
    put_operations = put_operations_t

    return get_operations, put_operations, clients

def mean_server_response_time(server):
    log_path = os.path.join(path, server)
    log_files_names = [os.path.join(log_path, f) for f in os.listdir(log_path) if
                       os.path.isfile(os.path.join(log_path, f))]
    for file in log_files_names:
        if file.find("server_") != -1 and file.find("_output.txt", file.find("server_") + 7) != -1:
            log_file = file
            break

    # log_file = os.path.join(log_path, server_file)
    print(log_file)

    data = []
    log = open(log_file, 'r')
    lines = log.readlines()
    flag = 0
    for i in range(len(lines) - 4):
        if(lines[i].find("[src/Data_Server.cpp][reconfig_query]73: started") != -1):
            flag = 5
        if (lines[i].find("[src/Server_driver.cpp][message_handler]") != -1 and \
                lines[i + 1].find(": started") != -1):
            # print(lines[i + 4])
            val = int(lines[i + 4][lines[i + 4].find("elapsed time = ") + 15:-7])
            # if flag <= 0:
            data.append(val)
            # if flag > 0 and val < 1000:
            #     flag -= 1
            # if(data[-1] > 1000):
            #     print(data[-1])

        # print(flag)

    print("mean_server_response_time without recon: " + str(sum(data) / float(len(data)) / 1000.) + "ms")
    print("max_server_response_time without recon: " + str(max(data) / 1000.) + "ms")
    print("min_server_response_time without recon: " + str(min(data) / 1000.) + "ms")

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

def standardize_clients(clients):
    clients.sort()
    clients_str = []
    for c in clients:
        clients_str.append(str(c))
    clients = clients_str
    return clients

def plot_latencies(key):

    server = "s1"
    log_path = os.path.join(path, server)
    log_path = os.path.join(log_path, "logs")
    clients = [524289, 524288, 524290, 524291] #537395211
    clients = standardize_clients(clients)
    get_operations, put_operations, clients = read_operation_for_key(key, log_path)

    plots = []
    plt.rcParams.update({'font.size': 38})
    fig = plt.figure()
    ax = fig.add_axes([0.09, 0.57, .88, .35])
    ax2 = fig.add_axes([0.09, 0.1, .88, .35])
    area = 500

    # ax.plot([0, 60], [400, 400], "-", color="r", linewidth=4.0, label="Latency SLO")
    # ax2.plot([0, 60], [400, 400], "-", color="r", linewidth=4.0, label="Latency SLO")

    for client_index, client in enumerate(clients):
        x = np.array([(a[0]) / 1000 for a in get_operations[client_index]]) / 1000.
        y = [a[1] for a in get_operations[client_index]]
        ax.scatter(x, y, label="Client " + str(client_index + 1), s=.5 * area)

    server = "s2"
    log_path = os.path.join(path, server)
    log_path = os.path.join(log_path, "logs")
    clients = [470810667, 470810704, 470810674, 470810712, 470810640]
    clients = standardize_clients(clients)
    get_operations, put_operations, clients = read_operation_for_key(key, log_path)

    for client_index, client in enumerate(clients):
        # x = np.array([(a[0]) / 1000 for a in get_operations[client_index]]) / 1000.
        x = (np.array([(a[0]) / 1000 for a in get_operations[client_index]])) / 1000.
        y = [a[1] for a in get_operations[client_index]]
        ax2.scatter(x, y, label="Client " + str(client_index + 1 + 5), s=.5 * area)

    ax2.set_xlabel('Time (sec)')
    ax.set_ylabel('Latency (msec)')
    ax2.set_ylabel('Latency (msec)')
    ax.grid(True)
    ax2.grid(True)
    ax.set_ylim(0, 1050)  # outlier 2
    ax2.set_ylim(0, 1000)
    ax2.set_xlim(0, 60)
    ax.set_xlim(0, 60)
    major_ticks = np.arange(0, 1001, 250)
    minor_ticks = np.arange(0, 601, 25)
    ax.set_yticks(major_ticks)
    ax2.set_yticks(major_ticks)
    ax2.set_xticks(minor_ticks, minor=True)
    ax.set_xticks(minor_ticks, minor=True)
    ax.grid(which='minor', alpha=0.5)
    ax.grid(which='major', alpha=1)
    ax2.grid(which='minor', alpha=0.5)
    ax2.grid(which='major', alpha=1)
    ax2.spines['top'].set_visible(False)
    ax2.tick_params(labeltop=False)
    ax.spines['top'].set_visible(False)
    ax.tick_params(labeltop=False)
    ax.tick_params(labelbottom=False)
    ax.set_title("GET Operations from Sydney users")
    ax2.set_title("GET Operations from Singapore users")
    # ax.legend()
    # ax2.legend()







    # put
    server = "s1"
    log_path = os.path.join(path, server)
    log_path = os.path.join(log_path, "logs")
    clients = [537395250, 537395220, 537395284, 537395277, 537395204]
    clients = standardize_clients(clients)
    get_operations, put_operations, clients = read_operation_for_key(key, log_path)

    plots = []
    plt.rcParams.update({'font.size': 38})

    fig = plt.figure()
    ax = fig.add_axes([0.09, 0.57, .88, .35])
    ax2 = fig.add_axes([0.09, 0.1, .88, .35])
    area = 500

    ax.plot([0, 600], [400, 400], "-", color="r", linewidth=4.0, label="Latency SLO")
    ax2.plot([0, 600], [400, 400], "-", color="r", linewidth=4.0, label="Latency SLO")

    for client_index, client in enumerate(clients):
        x = np.array([(a[0]) / 1000 for a in put_operations[client_index]]) / 1000.
        y = [a[1] for a in put_operations[client_index]]
        ax.scatter(x, y, label="Client " + str(client_index + 1 + 10), s=.5 * area)

    server = "s2"
    log_path = os.path.join(path, server)
    log_path = os.path.join(log_path, "logs")
    clients = [470810670, 470810723, 470810693, 470810633, 470810666]
    clients = standardize_clients(clients)
    get_operations, put_operations, clients = read_operation_for_key(key, log_path)

    for client_index, client in enumerate(clients):
        x = (np.array([(a[0]) / 1000 for a in put_operations[client_index]])) / 1000.
        y = [a[1] for a in put_operations[client_index]]
        ax2.scatter(x, y, label="Client " + str(client_index + 1 + 15), s=.5 * area)


    ax2.set_xlabel('Time (sec)')
    ax.set_ylabel('Latency (msec)')
    ax2.set_ylabel('Latency (msec)')
    ax.grid(True)
    ax2.grid(True)
    ax.set_ylim(0, 1050)  # outlier 2
    ax2.set_ylim(0, 1000)
    ax2.set_xlim(0, 600)
    ax.set_xlim(0, 600)
    major_ticks = np.arange(0, 1001, 250)
    minor_ticks = np.arange(0, 601, 25)
    ax.set_yticks(major_ticks)
    ax2.set_yticks(major_ticks)
    ax2.set_xticks(minor_ticks, minor=True)
    ax.set_xticks(minor_ticks, minor=True)
    ax.grid(which='minor', alpha=0.5)
    ax.grid(which='major', alpha=1)
    ax2.grid(which='minor', alpha=0.5)
    ax2.grid(which='major', alpha=1)
    ax2.spines['top'].set_visible(False)
    ax2.tick_params(labeltop=False)
    ax.spines['top'].set_visible(False)
    ax.tick_params(labeltop=False)
    ax.tick_params(labelbottom=False)
    ax.set_title("PUT Operations from Oregon users")
    ax2.set_title("PUT Operations from LA users")
    # ax.legend()
    # ax2.legend()



def print_one_key_one_server(key, server):
    log_path = os.path.join(path, server)
    log_path = os.path.join(log_path, "logs")
    get_operations, put_operations = read_operation_for_key(key, log_path)

    for op in get_operations:
        if op[1] > 1000:
            print(op[0], op[1], op[2])

    for op in put_operations:
        if op[1] > 1000:
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
    width = 0.8
    x = [a[0] for a in data]
    y = [a[1] for a in data]
    plt.bar(x, y, color="y", width=width / 2.)
    plt.xticks(x, x)
    plt.title("Reconfiguration Latency")
    plt.xlabel('key id')
    plt.ylabel('latency(ms)')
    plt.grid(True)

    # for x in data:
    #     print(x[0], x[1])

def plot_per_datacenter_opration(config):
    confid = config[config.find(".json") - 1:config.find(".json")]
    config = os.path.join(path, config)
    # input_worklaod = "../config/auto_test/input_workload.json"
    # config1 = "../config/auto_test/optimizer_output_1.json"
    # config2 = "../config/auto_test/optimizer_output_2.json"

    data_json = json.load(open(config, "r"), object_pairs_hook=OrderedDict)["output"][0]["client_placement_info"]

    #latency
    data = []
    for i in range(9):
        data.append([i, data_json[str(i)]["get_latency_on_this_client"], data_json[str(i)]["put_latency_on_this_client"]])

    data.sort(key=lambda x: x[0])

    plt.figure()
    width = 0.8
    indices = [a[0] for a in data]
    indices_label = ["s" + str(a[0]) for a in data]
    y1 = [a[1] for a in data]
    y2 = [a[2] for a in data]

    plt.bar(indices, y1, width=width / 2.,
            color='b', label='get_latency')
    plt.bar([i + 0.5 * width for i in indices], y2,
            width=width / 2., color='g', alpha=0.5, label='put_latency')

    # indices = [float(a) + width / 2. for a in indices]
    plt.xticks([i + 0.25 * width for i in indices], indices_label)

    plt.grid(True)
    plt.title("Latencies per datacenter for conf " + confid)
    plt.xlabel('datacenter name')
    plt.ylabel('latency(ms)')
    plt.legend()

    # costs
    data = []
    for i in range(9):
        data.append(
            [i, data_json[str(i)]["get_cost_on_this_client"], data_json[str(i)]["put_cost_on_this_client"]])

    data.sort(key=lambda x: x[0])

    plt.figure()
    width = 0.8
    indices = [a[0] for a in data]
    indices_label = ["s" + str(a[0]) for a in data]
    y1 = [a[1] for a in data]
    y2 = [a[2] for a in data]

    plt.bar(indices, y1, width=width / 2.,
            color='b', label='get_cost')
    plt.bar([i + 0.5 * width for i in indices], y2,
            width=width / 2., color='g', alpha=0.5, label='put_cost')

    # indices = [float(a) + width / 2. for a in indices]
    plt.xticks([i + 0.25 * width for i in indices], indices_label)

    plt.grid(True)
    plt.title("Network cost per datacenter for conf " + confid)
    plt.legend()

def plot_total_cost_per_configuration(configs):
    data = []
    for i, config in enumerate(configs):
        config = os.path.join(path, config)
        overall_cost = json.load(open(config, "r"), object_pairs_hook=OrderedDict)["overall_cost"]
        data.append([i + 1, overall_cost])

    plt.figure()
    width = 0.8
    x = [a[0] for a in data]
    y = [a[1] for a in data]
    plt.bar(x, y, color="y", width=width / 2.)
    plt.xticks(x, x)
    plt.title("Configuration Total Cost")
    plt.xlabel('Configuration id')
    plt.ylabel('cost($/hour)')
    plt.grid(True)

def draw_latencies_based_on_arrival_rate_for_server(server):
    arrival_rates = list(range(20, 101, 20))

    put_avg = []
    get_avg = []

    put_tail_95 = []
    get_tail_95 = []

    put_tail_99 = []
    get_tail_99 = []

    put_tail_100 = []
    get_tail_100 = []

    for ar in arrival_rates:
        dir = path + "_" + str(ar)
        summary_file = os.path.join(dir, "summary.txt")
        f = open(summary_file, "r")
        lines = f.readlines()
        server_data_is_reached = False
        for line in lines:
            if line.find("latencies for") != -1 and server_data_is_reached:
                break
            if line.find("latencies for") != -1 and line.find(server) != -1:
                server_data_is_reached = True
                continue
            if server_data_is_reached:
                if line.find("put average latency") != -1:
                    put_avg.append(float(line[21:line.find("ms")]))
                elif line.find("get average latency") != -1:
                    get_avg.append(float(line[21:line.find("ms")]))
                elif line.find("put tail latency(95%)") != -1:
                    put_tail_95.append(float(line[23:line.find("ms")]))
                elif line.find("get tail latency(95%)") != -1:
                    get_tail_95.append(float(line[23:line.find("ms")]))
                elif line.find("put tail latency(99%)") != -1:
                    put_tail_99.append(float(line[23:line.find("ms")]))
                elif line.find("get tail latency(99%)") != -1:
                    get_tail_99.append(float(line[23:line.find("ms")]))
                elif line.find("put tail latency(100%)") != -1:
                    put_tail_100.append(float(line[24:line.find("ms")]))
                elif line.find("get tail latency(100%)") != -1:
                    get_tail_100.append(float(line[24:line.find("ms")]))

    # plt.figure()

    plt.rcParams.update({'font.size': 44})
    # fig, ax = plt.subplots()
    fig = plt.figure()
    # ax = fig.add_axes([0.07, 0.09, .68, .88])
    ax = fig.add_axes([0.1, 0.1, .88, .88])

    x = arrival_rates

    colors = ["r", "b", "g"]
    area = 500

    ax.scatter(x, put_tail_99, s=1.75 * area, c="darkslateblue", label='put 99%')
    ax.plot(x, put_tail_99, "--", color="darkslateblue", linewidth=5.0)
    # plt.plot(x, put_tail_95, color="c", linewidth=4.0)

    ax.scatter(x, put_avg, s=1 * area, linewidth=10, facecolors='none', edgecolors="b", label='put avg')
    ax.plot(x, put_avg, "--", color="b")

    ax.scatter(x, get_tail_99, s=1.75 * area, c="lime", label='get 99%')
    ax.plot(x, get_tail_99, "--", color="lime", linewidth=5.0)

    ax.scatter(x, get_avg, s=1 * area, linewidth=10, facecolors='none', edgecolors="r", label='get avg')
    ax.plot(x, get_avg, "--", color="r", linewidth=5.0)


    # plt.plot(x, get_tail_99, ":", color="lime", linewidth=15.0, label='get 99%')
    # plt.plot(x, get_tail_95, color="tab:pink", linewidth=4.0)
    # plt.plot(x, get_avg, "-.", color="r", linewidth=15.0, label='get avg')

    # legs = plt.legend(["put avg", "put tail 95", "put tail_99", "get_avg", "get_tail_95", "get_tail_99"])
    legs = plt.legend()
    for leg in legs.get_lines():
        leg.set_linewidth(7)
    # plt.plot(y, '.-g')
    # plt.title(server + "")
    plt.xlabel('Arrival rate (req/sec)')
    plt.ylabel('Latency (ms)')

    limits = ax.axis()
    ax.axis([limits[0], limits[1], 0, limits[3]])

    plt.grid(True)

def draw_latencies_based_on_object_number_for_server(server):
    object_number = [1, 5, 25, 125]

    put_avg = []
    get_avg = []

    put_tail_95 = []
    get_tail_95 = []

    put_tail_99 = []
    get_tail_99 = []

    put_tail_100 = []
    get_tail_100 = []

    for on in object_number:
        dir = path + "_" + str(on)
        summary_file = os.path.join(dir, "summary.txt")
        f = open(summary_file, "r")
        lines = f.readlines()
        server_data_is_reached = False
        for line in lines:
            if line.find("latencies for") != -1 and server_data_is_reached:
                break
            if line.find("latencies for") != -1 and line.find(server) != -1:
                server_data_is_reached = True
                continue
            if server_data_is_reached:
                if line.find("put average latency") != -1:
                    put_avg.append(float(line[21:line.find("ms")]))
                elif line.find("get average latency") != -1:
                    get_avg.append(float(line[21:line.find("ms")]))
                elif line.find("put tail latency(95%)") != -1:
                    put_tail_95.append(float(line[23:line.find("ms")]))
                elif line.find("get tail latency(95%)") != -1:
                    get_tail_95.append(float(line[23:line.find("ms")]))
                elif line.find("put tail latency(99%)") != -1:
                    put_tail_99.append(float(line[23:line.find("ms")]))
                elif line.find("get tail latency(99%)") != -1:
                    get_tail_99.append(float(line[23:line.find("ms")]))
                elif line.find("put tail latency(100%)") != -1:
                    put_tail_100.append(float(line[24:line.find("ms")]))
                elif line.find("get tail latency(100%)") != -1:
                    get_tail_100.append(float(line[24:line.find("ms")]))

    plt.figure()
    x = object_number
    plt.plot(x, put_avg, '-o', color="b", linewidth=4.0)
    plt.plot(x, put_tail_95, '-o', color="c", linewidth=4.0)
    plt.plot(x, put_tail_99, '-o', color="darkslateblue", linewidth=4.0)
    plt.plot(x, get_avg, '-o', color="r", linewidth=4.0)
    plt.plot(x, get_tail_95, '-o', color="tab:pink", linewidth=4.0)
    plt.plot(x, get_tail_99, '-o', color="lime", linewidth=4.0)
    legs = plt.legend(["put_avg", "put_tail_95", "put_tail_99", "get_avg", "get_tail_95", "get_tail_99"])
    for leg in legs.get_lines():
        leg.set_linewidth(4)
    # plt.plot(y, '.-g')
    plt.title(server + "")
    plt.xlabel('object_number')
    plt.ylabel('latency(ms)')
    plt.grid(True)

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

    # for i in range(10):
    plot_latencies("10")

    # plot_reconfiguration_latencies()

    # plot_one_key_one_server(keys[1], "s0")
    # plot_one_key_one_server(keys[1], "s7")
    # plot_one_key_one_server(keys[1], "s8")

    # for server in servers:
    #    plot_one_key_one_server(keys[0], server)
       # print_one_key_one_server(keys[0], server)

    # for key in keys:
    #     for server in servers:
    #         print_one_key_one_server(key, server)

    # mean_server_response_time("s2")
    # mean_server_response_time("s5")
    # mean_server_response_time("s7")
    # mean_server_response_time("s8")

    # plot_per_datacenter_opration("config/auto_test/optimizer_output_1.json")
    # plot_per_datacenter_opration("config/auto_test/optimizer_output_2.json")
    # plot_total_cost_per_configuration(["config/auto_test/optimizer_output_1.json", "config/auto_test/optimizer_output_2.json"])

    # for server in servers:
    #    draw_latencies_based_on_arrival_rate_for_server(server)
       #  draw_latencies_based_on_object_number_for_server(server)

    # draw_latencies_based_on_arrival_rate_for_server(servers[0])


    plt.show()