#!/usr/bin/python3

import os
import sys
import shutil

# servers = ["s7", "s8"] #["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
servers = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]

def get_files(run_type):
    server_dirs = [os.path.join(run_type, s, "sum.txt") for s in os.listdir(run_type) if s[0] == "s" and
                   os.path.isdir(os.path.join(run_type, s)) and s.find('-') == -1]
    server_dirs.sort()
    return server_dirs

def get_file_on_server(run_type, server):
    file = os.path.join(run_type, server, "sum.txt")
    return file

def latency(files):
    put_tail_95 = 0
    get_tail_95 = 0

    put_tail_99 = 0
    get_tail_99 = 0

    put_tail_100 = 0
    get_tail_100 = 0

    put_count = 0
    put_total = 0
    get_count = 0
    get_total = 0

    puts = []  # [latency]
    gets = []  # [latency]

    for f in files:
        file = open(f, "r")
        put_flag = False
        get_flag = False
        lines = file.readlines()
        for line in lines:
            if line.find("|puts|") != -1:
                put_flag = True
                continue

            if line.find("|gets|") != -1:
                put_flag = False
                get_flag = True
                continue

            if (line.find("put: total") != -1):
                words = line.split()
                put_count += int(words[6])
                put_total += int(words[3][:-1])
                get_flag = False

            if (line.find("get: total") != -1):
                words = line.split()
                get_count += int(words[6])
                get_total += int(words[3][:-1])

            if put_flag:
                words = line.split()
                if len(words) == 2:
                    puts.append(int(words[1]))

            if get_flag:
                words = line.split()
                if len(words) == 2:
                    gets.append(int(words[1]))

        file.close()

    puts.sort()
    if len(puts) != 0:
        put_tail_95 = puts[int(.95 * len(puts))]
        put_tail_99 = puts[int(.99 * len(puts))]
        put_tail_100 = puts[int(1 * len(puts)) - 1]
    else:
        put_tail_95 = 0
        put_tail_99 = 0
        put_tail_100 = 0
    gets.sort()
    if len(gets) != 0:
        get_tail_95 = gets[int(.95 * len(gets))]
        get_tail_99 = gets[int(.99 * len(gets))]
        get_tail_100 = gets[int(1 * len(gets)) - 1]
    else:
        get_tail_95 = 0
        get_tail_99 = 0
        get_tail_100 = 0

    if put_count == 0:
        if get_count == 0:
            return put_count, 0, put_tail_95, put_tail_99, put_tail_100, get_count, \
                   0, get_tail_95, get_tail_99, get_tail_100
        else:
            return put_count, 0, put_tail_95, put_tail_99, put_tail_100, get_count, \
                   float(get_total) / get_count, get_tail_95, get_tail_99, get_tail_100
    else:
        if get_count == 0:
            return put_count, float(put_total) / put_count, put_tail_95, put_tail_99, put_tail_100, get_count, \
                   0, get_tail_95, get_tail_99, get_tail_100
        else:
            return put_count, float(put_total) / put_count, put_tail_95, put_tail_99, put_tail_100, get_count, \
                   float(get_total) / get_count, get_tail_95, get_tail_99, get_tail_100

def print_latency(put_num, put_avg, put_tail_95, put_tail_99, put_tail_100, get_num, get_avg, get_tail_95, get_tail_99, get_tail_100):
    print("put:", put_num)
    print("get:", get_num)
    print("put average latency:", "{:.3f}".format(put_avg) + "ms")
    print("get average latency:", "{:.3f}".format(get_avg) + "ms")
    print("put tail latency(95%):", str(put_tail_95) + "ms")
    print("get tail latency(95%):", str(get_tail_95) + "ms")
    print("put tail latency(99%):", str(put_tail_99) + "ms")
    print("get tail latency(99%):", str(get_tail_99) + "ms")
    print("put tail latency(100%):", str(put_tail_100) + "ms")
    print("get tail latency(100%):", str(get_tail_100) + "ms")

    # print("{:.3f}".format(put_avg))
    # print("{:.3f}".format(get_avg))
    # print(put_tail_95)
    # print(get_tail_95)
    # print(put_tail_99)
    # print(get_tail_99)
    # print(put_tail_100)
    # print(get_tail_100)

def main():
    # run_type = "CAS_NOF"
    # if len(sys.argv) == 2:
    #     run_type = sys.argv[1]
    # server_dirs = get_files(run_type)
    # put_num, put_avg, put_tail_95, put_tail_99, put_tail_100, get_num, get_avg, get_tail_95 , get_tail_99, get_tail_100 = latency(server_dirs)
    # print("put:", put_num)
    # print("get:", get_num)
    # print("for " + run_type + ", we have:")
    # print("put average latency:", "{:.3f}".format(put_avg) + "ms")
    # print("get average latency:", "{:.3f}".format(get_avg) + "ms")
    # print("put tail latency(95%):", str(put_tail_95) + "ms")
    # print("get tail latency(95%):", str(get_tail_95) + "ms")
    # print("put tail latency(99%):", str(put_tail_99) + "ms")
    # print("get tail latency(99%):", str(get_tail_99) + "ms")
    # print("put tail latency(100%):", str(put_tail_100) + "ms")
    # print("get tail latency(100%):", str(get_tail_100) + "ms")
    #
    # print("{:.3f}".format(put_avg))
    # print("{:.3f}".format(get_avg))
    # print(put_tail_95)
    # print(get_tail_95)
    # print(put_tail_99)
    # print(get_tail_99)
    # print(put_tail_100)
    # print(get_tail_100)

    run_type = "CAS_NOF"
    if len(sys.argv) == 2:
        run_type = sys.argv[1]
    for server in servers:
        file = get_file_on_server(run_type, server)
        put_num, put_avg, put_tail_95, put_tail_99, put_tail_100, get_num, get_avg, get_tail_95, get_tail_99, get_tail_100 = latency([file])
        print("latencies for " + os.path.join(run_type, server))
        print_latency(put_num, put_avg, put_tail_95, put_tail_99, put_tail_100, get_num, get_avg, get_tail_95, get_tail_99, get_tail_100)
        print()

    # total
    files = get_files(run_type)
    put_num, put_avg, put_tail_95, put_tail_99, put_tail_100, get_num, get_avg, get_tail_95 , get_tail_99, get_tail_100 = latency(files)
    print("latencies for " + run_type + " all the servers together")
    print_latency(put_num, put_avg, put_tail_95, put_tail_99, put_tail_100, get_num, get_avg, get_tail_95, get_tail_99,
                  get_tail_100)

if __name__ == "__main__":
    main()

    run_type = "CAS_NOF"
    if len(sys.argv) == 2:
        run_type = sys.argv[1]
    os.environ["run_type"] = run_type
    os.system("./copy_logs.sh")


