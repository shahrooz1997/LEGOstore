#!/usr/bin/env python3

from Client import Client
import socket
import json
import time
import copy
import glob

from protocol import Protocol
from data_center import Datacenter
from datetime import datetime
from multiprocessing import Process
import uuid
import threading
from workload import Workload

import logging


if __name__ == "__main__":
    properties = json.load(open('client_config.json'))
    client = Client(properties, properties["local_datacenter"])
    while 1:
        data = input()
        method, key, value = data.split(",")
        a = time.time()
        if method == "insert":
            print(client.insert(key, value))
        elif method == "put":
            print(client.put(key, value))
        elif method == "get":
            print(client.get(key))
        else:
            print("Invalid Call")
        b = time.time()
        c = b - a
        print(c)
