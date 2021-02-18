#!/usr/bin/python3

import os
import sys
import shutil

def get_files():
    log_path = "logs"
    log_files = [os.path.join(log_path, logfile) for logfile in os.listdir(log_path) if logfile[0:7] == "logfile" and
                   os.path.isfile(os.path.join(log_path, logfile))]
    log_files.sort()
    return log_files

def latency(files):

    put_count = 0
    put_total = 0
    get_count = 0
    get_total = 0

    puts = [] # [(file, i, latency)]
    gets = [] # [(file, i, latency)]

    for f in files:
        file = open(f, "r")
        lines = file.readlines()
        i = 1
        for line in lines:
            if line.find("put") != -1:
                words = line.split()
                temp = int(words[5]) - int(words[4][:-1])
                put_total += temp
                put_count += 1
                puts.append((f, i, temp))
            if line.find("get") != -1:
                words = line.split()
                temp = int(words[5]) - int(words[4][:-1])
                get_total += temp
                get_count += 1
                gets.append((f, i, temp))

            i += 1

        file.close()

    return put_count, put_total, get_count, get_total, puts, gets

def write_ops(puts, gets):

    print("|puts|")
    file_name = ""
    for put in puts:
        if file_name != put[0]:
            file_name = put[0]
            print(file_name)
        print(put[1], put[2])

    print("|gets|")
    file_name = ""
    for get in gets:
        if file_name != get[0]:
            file_name = get[0]
            print(file_name)
        print(get[1], get[2])


def main():
    log_files = get_files()
    put_count, put_total, get_count, get_total, puts, gets = latency(log_files)

    write_ops(puts, gets)

    print("put: total = " + str(put_total) + ", count = " + str(put_count))
    print("get: total = " + str(get_total) + ", count = " + str(get_count))

if __name__ == "__main__":
    main()
