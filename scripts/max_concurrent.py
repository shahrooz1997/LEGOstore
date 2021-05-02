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
import json
from collections import OrderedDict
from pylab import *

# path = "data/CAS_NOF"
# path = "data/arrival_rate/HW/CAS_NOF_100"

# ops = []

def read_operation_for_key(ops, key, log_path, op="put"):
    log_files_names = [os.path.join(log_path, f) for f in os.listdir(log_path) if os.path.isfile(os.path.join(log_path, f))]
    for file in log_files_names:
        log = open(file, 'r')
        lines = log.readlines()
        for line in lines:
            # print(file, line)
            words = line.split()
            # if (words[2][:-1] != key):
            #     continue
            if (words[1][:-1] != op):
                continue
            ops.append(((int(words[4][:-1]) / 1000) % 1000000, (int(words[5]) / 1000) % 1000000))

        log.close()

    ops.sort(key=lambda x: x[0])

    return ops

def number_of_ops_concurrent_to(ops, index):
    start = ops[index][0]
    end = ops[index][1]

    counter = 1
    start_index = 0

    # for i in range(index - 1, -1, -1):


    for op in ops:
        if start <= op[1] and op[0] <= end:
            counter += 1
        if op[0] > end:
            break

    return counter

def max_concurrent(ops):
    max_count = 0
    for i in range(len(ops)):
        count = number_of_ops_concurrent_to(ops, i)
        if max_count < count:
            max_count = count

    return max_count

def main(ar, ops, path):
    read_operation_for_key(ops, "2001", os.path.join(path, "logs"))

    # print("Number of ops is", len(ops))
    print(str(ar) + " " + str(max_concurrent(ops)))

if __name__ == "__main__":
    path = "data/arrival_rate/RW/CAS_NOF_"

    for ar in range(20, 101, 20):
        ops = []
        main(ar, ops, path + str(ar))


    # main()

