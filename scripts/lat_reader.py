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

def get_latency(line):
    last_word = line.split()[-1][:-2]
    ret = int(round(float(last_word)))
    return ret

def get_latency2(line):
    last_word = line.split()[-1]
    last_word = last_word[:-1] if last_word[-1] == ',' else last_word
    ret = int(round(float(last_word)))
    return ret

def print_lats():
    path = "data/LAST_RES/CAS_NOF_10min_opt_get"
    path_f = "data/LAST_RES/CAS_F_10min_opt_get"
    path_predicted = "../config/auto_test"

    with open(os.path.join(path, "summary.txt")) as f:
        lines = f.readlines()

    put_avg_lat = []
    put_99_lat = []

    get_avg_lat = []
    get_99_lat = []

    for line in lines:
        if line.find("put average latency") != -1:
            put_avg_lat.append(get_latency(line))
        if line.find("put tail latency(99%)") != -1:
            put_99_lat.append(get_latency(line))

        if line.find("get average latency") != -1:
            get_avg_lat.append(get_latency(line))
        if line.find("get tail latency(99%)") != -1:
            get_99_lat.append(get_latency(line))


    # predicted latencies
    put_predicted = []
    get_predicted = []
    with open(os.path.join(path_predicted, "optimizer_output_1.json")) as f:
        lines = f.readlines()

    for line in lines:
        if line.find("put_latency_on_this_client") != -1:
            put_predicted.append(get_latency2(line))

        if line.find("get_latency_on_this_client") != -1:
            get_predicted.append(get_latency2(line))

    # With failure
    with open(os.path.join(path_f, "summary.txt")) as f:
        lines = f.readlines()

    put_avg_lat_f = []
    put_99_lat_f = []

    get_avg_lat_f = []
    get_99_lat_f = []

    for line in lines:
        if line.find("put average latency") != -1:
            put_avg_lat_f.append(get_latency(line))
        if line.find("put tail latency(99%)") != -1:
            put_99_lat_f.append(get_latency(line))

        if line.find("get average latency") != -1:
            get_avg_lat_f.append(get_latency(line))
        if line.find("get tail latency(99%)") != -1:
            get_99_lat_f.append(get_latency(line))

    # print("Predicted latency")
    for lat in put_predicted:
        print(lat, end=" ")
    print()

    for lat in put_avg_lat:
        print(lat, end=" ")
    print()

    # print("put_avg_lat")
    for lat in put_99_lat:
        print(lat, end=" ")
    print()

    for lat in put_avg_lat_f:
        print(lat, end=" ")
    print()

    # print("put_avg_lat")
    for lat in put_99_lat_f:
        print(lat, end=" ")
    print()

    print("\n\n\n")

    for lat in get_predicted:
        print(lat, end=" ")
    print()

    for lat in get_avg_lat:
        print(lat, end=" ")
    print()

    for lat in get_99_lat:
        print(lat, end=" ")
    print()

    for lat in get_avg_lat_f:
        print(lat, end=" ")
    print()

    for lat in get_99_lat_f:
        print(lat, end=" ")
    print()

    print()

def main():
    print_lats()

if __name__ == '__main__':
    main()