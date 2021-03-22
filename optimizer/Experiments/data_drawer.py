#!/usr/bin/python3

import json
import os, sys, time
from collections import OrderedDict
import matplotlib.pyplot as plt
import numpy as np
from workload_def import *

directory = "workloads"
# directory = "/home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/data/ALL_optimizer_Mar19_0817"

def get_workloads():
    workloads = []
    for client_dist in client_dists:
        for object_size in object_sizes:
            for arrival_rate in arrival_rates:
                for read_ratio in read_ratios:
                    # for lat in SLO_latencies:
                    lat = 1000
                    FILE_NAME = client_dist + "_" + object_size + "_" + str(arrival_rate) + "_" + \
                                read_ratio + "_" + str(lat) + ".json"
                    workloads.append(FILE_NAME)

    # workloads = ["cheap_skewed_10KB_500_HR_1000.json", "uniform_10KB_500_HR_1000.json"]

    return workloads

def get_results():
    all_results = []
    confs = []
    for f_index, f in enumerate(availability_targets):
        files_path = os.path.join(directory, "f=" + str(f))
        all_results.append(OrderedDict())
        confs.append(OrderedDict())
        for exec in executions[f_index]:
            for workload in workloads:
                res_file = os.path.join(files_path, "res_" + exec + "_" + workload)
                data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                for grp_index, grp in enumerate(data):
                    key = exec + "_" + workload  # list(client_dists.keys())[grp_index] + "_" + object_size_default + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default
                    value = "{:.4e}".format(grp["get_cost"]) + " " + "{:.4e}".format(grp["put_cost"]) + " " + \
                            "{:.4e}".format(grp["vm_cost"]) + " " + "{:.4e}".format(grp["storage_cost"])
                    all_results[f_index][key] = value
                    value = str(grp["m"]) + "|" + (str(grp["k"]) if grp["protocol"] != "abd" else str(0)) + "|"
                    for s in grp["selected_dcs"]:
                        value += str(s) + ","
                    value = value[0:-1]
                    value += "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(len(grp["client_placement_info"]["0"]["Q2"]))
                    if grp["protocol"] != "abd":
                        value += "|" + str(len(grp["client_placement_info"]["0"]["Q3"])) + "|" + str(len(grp["client_placement_info"]["0"]["Q4"]))
                    value += "|" + "{:.4f}".format(grp["total_cost"]) + "|" + "{:.0f}".format(grp["put_latency"]) + "|" + "{:.0f}".format(grp["get_latency"])
                    confs[f_index][key] = value

    return all_results, confs

def list_reformat(l):
    ret = ""
    for e in l:
        ret += str(e) + ","

    return ret[0:-1]

def quorum_sizes(grp):
    ret = ""
    ret += str(len(grp["client_placement_info"]["0"]["Q1"])) + ","
    ret += str(len(grp["client_placement_info"]["0"]["Q2"])) + ","
    if "Q3" in grp["client_placement_info"]["0"]:
        ret += str(len(grp["client_placement_info"]["0"]["Q3"])) + ","
        ret += str(len(grp["client_placement_info"]["0"]["Q4"])) + ","
    else:
        ret += "-,-,"

    return ret[0:-1]

configuration_count = {}
configuration_cost = {}
def count_the_configuration_of(grp):
    configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(grp["selected_dcs"])
    if configuration in configuration_count:
        configuration_count[configuration] += 1
    else:
        configuration_count[configuration] = 1
        configuration_cost[configuration] = [grp["client_placement_info"]["0"]["get_cost_on_this_client"], grp["client_placement_info"]["0"]["put_cost_on_this_client"]]

def print_configurations(configurations, configuration_cost):
    for config in configurations:
        strs = config.split("|")
        print(strs[0], strs[1], strs[2], configurations[config], "{:.4e}".format(configuration_cost[config][0]), \
              "{:.4e}".format(configuration_cost[config][1]))

def extractor_type2():
    all_results, confs = get_results()

    for f_index, f in enumerate(availability_targets):
        keys = list(all_results[f_index].keys())
        # keys = [k for k in keys if k[0:8] != "baseline"]
        # print(keys)
        for exec in executions[0]:
            print(exec)
            for workload in workloads:
                print(workload, all_results[f_index][exec + "_" + workload])

def extractor_type3():
    all_results, confs = get_results()

    for f_index, f in enumerate(availability_targets):
        keys = list(all_results[f_index].keys())
        # keys = [k for k in keys if k[0:8] != "baseline"]
        # print(keys)
        for workload in workloads:
            print(workload[:workload.find(".json")])
            print(" get_cost put_cost vm_cost storage_cost")
            for exec in executions[0]:
                print(exec, all_results[f_index][exec + "_" + workload])
            print("\n")

def print_workload(workload, availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    workload = workload if workload.find(".json") != -1 else workload + ".json"

    labels = []
    get_cost = []
    put_cost = []
    vm_cost = []
    storage_cost = []

    print(workload[:workload.find(".json")] + ":")
    print(" get_cost put_cost vm_cost storage_cost total")
    for exec in executions[f_index]:
        # if exec == "optimized":
        #     continue
        labels.append(exec if exec.find("baseline") == -1 else exec[9:])
        values = results[f_index][exec + "_" + workload].split()
        # print(values)
        get_cost.append(float(values[0]))
        put_cost.append(float(values[1]))
        vm_cost.append(float(values[2]))
        storage_cost.append(float(values[3]))
        # print(exec + ":", confs[f_index][exec + "_" + workload])
    # print()

    for i, l in enumerate(labels):
        print(l, get_cost[i], put_cost[i], vm_cost[i], storage_cost[i], get_cost[i] + put_cost[i] + vm_cost[i] + storage_cost[i])
    print()

def plot_workload(workload, availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    workload = workload if workload.find(".json") != -1 else workload + ".json"

    labels = []
    get_cost = []
    put_cost = []
    vm_cost = []
    storage_cost = []

    # optimized = []
    # values = results[f_index]["optimized" + "_" + workload].split()
    # optimized.append(float(values[0]))
    # optimized.append(float(values[1]))
    # optimized.append(float(values[2]))
    # optimized.append(float(values[3]))
    # optimized_cost = sum(optimized)
    print(workload[:workload.find(".json")] + ":")
    for exec in executions[f_index]:
        # if exec == "optimized":
        #     continue
        labels.append(exec if exec.find("baseline") == -1 else exec[9:])
        values = results[f_index][exec + "_" + workload].split()
        # print(values)
        get_cost.append(float(values[0]))
        put_cost.append(float(values[1]))
        vm_cost.append(float(values[2]))
        storage_cost.append(float(values[3]))
        print(exec + ":", confs[f_index][exec + "_" + workload])
    print()

    # print(get_cost)
    # print(put_cost)
    # print(vm_cost)
    # print(storage_cost)
    width = 0.5  # the width of the bars: can also be len(x) sequence
    fig, ax = plt.subplots()

    # ax.bar(labels, women_means, width, yerr=women_std, bottom=men_means, label='Women')

    ax.bar(labels, storage_cost, width, bottom=np.array(get_cost) + np.array(put_cost) + np.array(vm_cost),
           label='storage_cost', color='tab:red')
    ax.bar(labels, vm_cost, width, bottom=np.array(get_cost) + np.array(put_cost), label='vm_cost', color='tab:green')
    ax.bar(labels, put_cost, width, bottom=get_cost, label='put_cost', color='tab:orange')
    ax.bar(labels, get_cost, width, label='get_cost', color='tab:blue')

    ax.set_ylabel('Cost($/hour)')
    ax.set_title(workload[:workload.find(".json")])
    ax.legend()
    plt.grid(True)



    # print(workload[:workload.find(".json")])
    # print(" get_cost put_cost vm_cost storage_cost")
    # for exec in executions:
    #     print(exec, results[f_index][exec + "_" + workload])
    # print("\n")

def plot_normalized(workload, availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    workload = workload if workload.find(".json") != -1 else workload + ".json"
    print(workload + ":")

    labels = []
    get_cost = []
    put_cost = []
    vm_cost = []
    storage_cost = []
    norm_total_cost = []

    optimized = []
    values = results[f_index]["optimized" + "_" + workload].split()
    optimized.append(float(values[0]))
    optimized.append(float(values[1]))
    optimized.append(float(values[2]))
    optimized.append(float(values[3]))
    optimized_cost = sum(optimized)
    print("optimized" + ":", confs[f_index]["optimized" + "_" + workload])

    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        labels.append(exec if exec.find("baseline") == -1 else exec[9:])
        values = results[f_index][exec + "_" + workload].split()
        # print(values)
        norm_total_cost.append((float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])) / optimized_cost * 100)
        # get_cost.append(float(values[0]))
        # put_cost.append(float(values[1]))
        # vm_cost.append(float(values[2]))
        # storage_cost.append(float(values[3]))
        print(exec + ":", confs[f_index][exec + "_" + workload])
    print()

    # print(get_cost)
    # print(put_cost)
    # print(vm_cost)
    # print(storage_cost)
    width = 0.5  # the width of the bars: can also be len(x) sequence
    fig, ax = plt.subplots()

    # ax.bar(labels, women_means, width, yerr=women_std, bottom=men_means, label='Women')

    # ax.bar(labels, storage_cost, width, bottom=np.array(get_cost) + np.array(put_cost) + np.array(vm_cost),
    #        label='storage_cost', color='tab:red')
    # ax.bar(labels, vm_cost, width, bottom=np.array(get_cost) + np.array(put_cost), label='vm_cost', color='tab:green')
    # ax.bar(labels, put_cost, width, bottom=get_cost, label='put_cost', color='tab:orange')
    ax.bar(labels, norm_total_cost, width, label='normalized_total_cost', color='tab:blue')

    ax.set_ylabel('Cost($/hour)')
    ax.set_title(workload[:workload.find(".json")])
    ax.legend()
    plt.grid(True)
    limits = ax.axis()
    ax.axis([limits[0], limits[1], 100.0, limits[3]])

    # print(workload[:workload.find(".json")])
    # print(" get_cost put_cost vm_cost storage_cost")
    # for exec in executions:
    #     print(exec, results[f_index][exec + "_" + workload])
    # print("\n")

def counting(availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)

    cas_num = 0
    abd_num = 0

    for workload in workloads:
        workload = workload if workload.find(".json") != -1 else workload + ".json"
        conf = confs[f_index]["optimized" + "_" + workload].split("|")
        if int(conf[1]) > 0:
            cas_num += 1
        else:
            abd_num += 1

    print("#ABD:", abd_num, "#CAS:", cas_num)

def workload_satisfies(workload, attrs):
    for a in attrs:
        if workload.find(a) == -1:
            return False
    return True

def plot_all():
    for workload in workloads:
        # print("A")
        # interesting cases
        attrs = ["1KB"] #, "HW"] #workload.find(list(client_dists.keys())[2])
        # attrs = ["10KB", "HR"]
        if workload_satisfies(workload, attrs):
            # plot_workload(workload, 1)
            plot_normalized(workload, 2)

if __name__ == "__main__":
    global workloads
    workloads = get_workloads()

    # counting(2)

    # interesting_workloads = ["dist_LO_1KB_500_RW_1000.json", "dist_L_1KB_50_RW_1000"]
    # for wl in interesting_workloads:
    #     print_workload(wl, 2)
    #     plot_normalized(wl, 2)

    # extractor_type2()
    # extractor_type3()

    # plot_workload(list(client_dists.keys())[2] + "_" + "10KB" + "_" + str(arrival_rates[1]) + "_" + "RW" + "_" + "1000" + ".json")
    # plot_workload(list(client_dists.keys())[2] + "_" + "1KB" + "_" + str(arrival_rates[1]) + "_" + "RW" + "_" + "1000" + ".json")

    plot_all()
    os.system("subl drawer_output.txt")
    sys.stdout.flush()
    plt.show()


