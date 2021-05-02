#!/usr/bin/python3

import subprocess
from subprocess import PIPE
import threading
import os, shutil
import sys
from time import sleep
from signal import signal, SIGINT
import urllib.request
import argparse
from collections import OrderedDict
import json
# sys.path.insert(0, abspath(join(dirname(__file__), '../../scripts')))
# from optimizer import cold_hot

def get_commands():
    commands = []
    f = open('commands.txt', 'r')
    lines = f.readlines()
    f.close()
    for line in lines:
        if line[-1] == '\n':
            commands.append(line[:-1])
        else:
            commands.append(line)

    return commands

def execute(commands, num_of_threads=6):
    def batch_exec_thread_helper(commands):
        for command in commands:
            # self.execute("cd optimizer/Experiments; ")
            command_name = command[command.find("-o"):command.find(".json -v")]
            if command_name.find("coldhot") != -1:
                command_name = command_name[command_name.find("coldhot"):]
            else:
                command_name = command_name[command_name.find("workloads"):]
            # print("cd optimizer/Experiments; " + command + " >" + command_name + "_output.txt 2>&1")
            print(command)
            os.system(command + " >" + command_name + "_output.txt 2>&1")

    # execute 6 processes of the optimizer
    indicies = []
    batch_size = int(len(commands) / num_of_threads)
    indicies.append((0, batch_size))
    for i in range(1, num_of_threads - 1):
        indicies.append((i * batch_size + 1, (i + 1) * batch_size))
    indicies.append(((num_of_threads - 1) * batch_size + 1, len(commands) - 1))

    threads = []
    for index in indicies:
        threads.append(threading.Thread(target=batch_exec_thread_helper, args=[commands[index[0]:index[1] + 1]]))
        threads[-1].start()

    for thread in threads:
        thread.join()

def main():
    commands = get_commands()
    execute(commands)

if __name__ == "__main__":
    main()