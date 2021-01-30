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
import matplotlib.pyplot as plt

keys = ["222221"] #, "222222", "222223", "222224", "222225", "222226", "222227", "222228", "222229", "222230"]#, "222222", "222223"]

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
# network_costs = []
# total_costs = []
# arrival_rates = list(range(450, 651, 10))
# with open("NSF_output/NSF_proposal_res.json") as fd:
#     _outputs = json.load(fd)["output"]
#     for ele in _outputs:
#         get_cost = float(ele["get_cost"])
#         put_cost = float(ele["put_cost"])
#         network_costs.append(get_cost + put_cost)
#         total_costs.append(float(ele["total_cost"]))
#
#     assert(len(network_costs) == len(total_costs))


# x = arrival_rates
#y = [network_costs[i]/total_costs[i] for i in range(len(total_costs))]
#y = network_costs
# y = total_costs

# plt.scatter(x, y)
# plt.show()