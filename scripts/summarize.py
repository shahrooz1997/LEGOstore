#!/usr/bin/python3

import os
import sys
import shutil

def get_files():
    log_path = "logs"
    log_files = [os.path.join(log_path, logfile) for logfile in os.listdir(log_path) if logfile[0:7] == "logfile" and
                   os.path.isfile(os.path.join(log_path, logfile))]
    return log_files

def latency(files):
    put_tail = 0
    get_tail = 0

    put_count = 0
    put_total = 0
    get_count = 0
    get_total = 0

    for f in files:
        file = open(f, "r")
        lines = file.readlines()
        for line in lines:
            if(line.find("put") != -1):
                words = line.split()
                temp = int(words[5]) - int(words[4][:-1])
                put_total += temp
                put_count += 1
                if temp > put_tail:
                    put_tail = temp
            if (line.find("get") != -1):
                words = line.split()
                temp = int(words[5]) - int(words[4][:-1])
                get_total += temp
                get_count += 1
                if temp > get_tail:
                    get_tail = temp

    return put_count, put_total, put_tail, get_count, get_total, get_tail

def main():
    log_files = get_files()
    put_count, put_total, put_tail, get_count, get_total, get_tail = latency(log_files)

    print("put: total = " + str(put_total) + ", count = " + str(put_count))
    print("get: total = " + str(get_total) + ", count = " + str(get_count))
    print("put tail latency = " + str(put_tail));
    print("get tail latency = " + str(get_tail));

if __name__ == "__main__":
    main()
