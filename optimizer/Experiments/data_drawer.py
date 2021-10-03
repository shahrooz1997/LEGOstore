#!/usr/bin/python3

import json
import os, sys, time
from collections import OrderedDict
import matplotlib.pyplot as plt
import numpy as np
from workload_def import *

# directory = "workloads"
# directory = "RESULTS/workloads_200ms"
# directory = "RESULTS/workloads_500ms"
directory = "RESULTS/workloads_1000ms"
# directory = "workloads_Mar24_2152"
# directory = "/home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/data/ALL_optimizer_Mar19_0817"

def open_sublime():
    os.system("subl drawer_output.txt")

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
    # print(grp)
    if grp["protocol"] != "abd":
        configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(grp["selected_dcs"])
    else:
        configuration = str(grp["m"]) + "|" + "0" + "|" + list_reformat(grp["selected_dcs"])

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

def print_configurations_count():
    for f_index, f in enumerate(availability_targets):
        files_path = os.path.join(directory, "f=" + str(f))
        min_total_cost = 10000000
        max_total_cost = 0
        for exec in ["optimized"]: # ["baseline_abd", "baseline_cas"]:
            configuration_count = {}
            configuration_cost = {}
            for workload in workloads:
                res_file = os.path.join(files_path, "res_" + exec + "_" + workload)
                data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                for grp_index, grp in enumerate(data):
                    if grp["protocol"] != "abd":
                        configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(grp["selected_dcs"]) + \
                                        "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                        len(grp["client_placement_info"]["0"]["Q2"])) + "|" + str(len(grp["client_placement_info"]["0"]["Q3"])) + "|" + str(
                                        len(grp["client_placement_info"]["0"]["Q4"]))
                    else:
                        configuration = str(grp["m"]) + "|" + "0" + "|" + list_reformat(grp["selected_dcs"]) + "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                        len(grp["client_placement_info"]["0"]["Q2"]))

                    total_cost = float("{:.4f}".format(grp["total_cost"]))
                    if min_total_cost > total_cost:
                        min_total_cost = total_cost
                    if max_total_cost < total_cost:
                        max_total_cost = total_cost

                    # print(configuration)

                    if configuration in configuration_count:
                        configuration_count[configuration] += 1
                    else:
                        configuration_count[configuration] = 1
                        configuration_cost[configuration] = [
                            grp["client_placement_info"]["0"]["get_cost_on_this_client"],
                            grp["client_placement_info"]["0"]["put_cost_on_this_client"]]

            print("For f=" + str(f) + " and " + exec + ":")
            print("for " + str(len(workloads)) + ", we have " + str(len(configuration_count.keys())) + " different confs")
            print("min_total_cost", min_total_cost, "max_total_cost", max_total_cost, "range:", max_total_cost - min_total_cost)

            configurations = OrderedDict()
            for config in configuration_count:
                strs = config.split("|")
                key = strs[0] + "|" + strs[1]
                if key in configurations:
                    configurations[key] += configuration_count[config]
                else:
                    configurations[key] = configuration_count[config]

            configurations = OrderedDict(sorted(configurations.items(), key=lambda x: x[1], reverse=True))
            print(configurations)

def get_workloads():
    workloads = []
    for client_dist in client_dists:
        for object_size in object_sizes:
            for storage_size in storage_sizes:
                for arrival_rate in arrival_rates:
                    for read_ratio in read_ratios:
                        # for lat in SLO_latencies:
                        lat = 1000

                        num_objects = storage_sizes[storage_size] / object_sizes[object_size]
                        FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(arrival_rate) + "_" + \
                                    read_ratio + "_" + str(lat) + ".json"
                        workloads.append(FILE_NAME)

    # for client_dist in client_dists:
    #     for object_size in object_sizes:
    #         for arrival_rate in arrival_rates:
    #             for read_ratio in read_ratios:
    #                 # for lat in SLO_latencies:
    #                 lat = 500
    #                 FILE_NAME = client_dist + "_" + object_size + "_" + str(arrival_rate) + "_" + \
    #                             read_ratio + "_" + str(lat) + ".json"
    #                 workloads.append(FILE_NAME)

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
                key = exec + "_" + workload  # list(client_dists.keys())[grp_index] + "_" + object_size_default + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default
                res_file = os.path.join(files_path, "res_" + exec + "_" + workload)
                if not os.path.exists(res_file):
                    value = "INVALID"
                    all_results[f_index][key] = value
                    confs[f_index][key] = value
                    continue
                data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                for grp_index, grp in enumerate(data):
                    if grp["m"] != "INVALID":
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
                        value += "|" + "{:.4e}".format(grp["get_cost"]) + "|" + "{:.4e}".format(grp["put_cost"]) + "|" + \
                                "{:.4e}".format(grp["vm_cost"]) + "|" + "{:.4e}".format(grp["storage_cost"]) + "|" + "{:.4f}".format(grp["total_cost"]) + "|" + "{:.0f}".format(grp["put_latency"]) + "|" + "{:.0f}".format(grp["get_latency"])
                        confs[f_index][key] = value
                    else:
                        value = "INVALID"
                        all_results[f_index][key] = value
                        confs[f_index][key] = value
    return all_results, confs

def get_results2():
    all_results = []
    confs = []
    num_of_objects = []
    for f_index, f in enumerate(availability_targets):
        files_path = os.path.join(directory, "f=" + str(f))
        all_results.append(OrderedDict())
        confs.append(OrderedDict())
        num_of_objects.append(OrderedDict())
        for exec in executions[f_index]:
            for workload in workloads:
                key = exec + "_" + workload  # list(client_dists.keys())[grp_index] + "_" + object_size_default + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default
                main_file = os.path.join(files_path, workload)
                res_file = os.path.join(files_path, "res_" + exec + "_" + workload)
                if not os.path.exists(res_file):
                    value = "INVALID"
                    all_results[f_index][key] = value
                    confs[f_index][key] = value
                    continue
                data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                for grp_index, grp in enumerate(data):
                    if grp["m"] != "INVALID":
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
                        value += "|" + "{:.4e}".format(grp["get_cost"]) + "|" + "{:.4e}".format(grp["put_cost"]) + "|" + \
                                "{:.4e}".format(grp["vm_cost"]) + "|" + "{:.4e}".format(grp["storage_cost"]) + "|" + "{:.4f}".format(grp["total_cost"]) + "|" + "{:.0f}".format(grp["put_latency"]) + "|" + "{:.0f}".format(grp["get_latency"])
                        confs[f_index][key] = value
                    else:
                        value = "INVALID"
                        all_results[f_index][key] = value
                        confs[f_index][key] = value

                if exec == "optimized":
                    data = json.load(open(main_file, "r"), object_pairs_hook=OrderedDict)["input_groups"]
                    for grp_index, grp in enumerate(data):
                        num_of_objects[f_index][workload] = [int(grp["num_objects"]), grp["object_size"]]

    return all_results, confs, num_of_objects

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
        if exec != "optimized":
            continue
        if results[f_index][exec + "_" + workload] == "INVALID":
            print("WARN: no data found for " + exec + "_" + workload)
            continue
        labels.append(exec if exec.find("baseline") == -1 else exec[9:])
        values = results[f_index][exec + "_" + workload].split()
        # print(values)
        get_cost.append(float(values[0]))
        put_cost.append(float(values[1]))
        vm_cost.append(float(values[2]))
        storage_cost.append(float(values[3]))
        print(exec + ":", confs[f_index][exec + "_" + workload])
    # print()

    # for i, l in enumerate(labels):
    #     print(l, get_cost[i], put_cost[i], vm_cost[i], storage_cost[i], get_cost[i] + put_cost[i] + vm_cost[i] + storage_cost[i])
    # print()

def print_workload2(availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)

    labels = []
    get_cost = []
    put_cost = []
    vm_cost = []
    storage_cost = []

    dist1 = "dist_O"
    dist2 = "dist_LO"

    dist1_workloads = []
    dist2_workloads = []

    for workload in workloads:
        if workload.find(dist1) != -1:
            dist1_workloads.append(workload)
        if workload.find(dist2) != -1:
            dist2_workloads.append(workload)

    for i, workload in enumerate(dist1_workloads):
        exec = "optimized"
        print(exec + "_" + workload + ":", confs[f_index][exec + "_" + workload])
        print(exec + "_" + dist2_workloads[i] + ":", confs[f_index][exec + "_" + dist2_workloads[i]])
        print()

    # print(workload[:workload.find(".json")] + ":")
    # print(" get_cost put_cost vm_cost storage_cost total")
    # for exec in executions[f_index]:
    #     if exec != "optimized":
    #         continue
    #     if results[f_index][exec + "_" + workload] == "INVALID":
    #         print("WARN: no data found for " + exec + "_" + workload)
    #         continue
    #     labels.append(exec if exec.find("baseline") == -1 else exec[9:])
    #     values = results[f_index][exec + "_" + workload].split()
    #     # print(values)
    #     get_cost.append(float(values[0]))
    #     put_cost.append(float(values[1]))
    #     vm_cost.append(float(values[2]))
    #     storage_cost.append(float(values[3]))
    #     print(exec + ":", confs[f_index][exec + "_" + workload])
    # print()

    # for i, l in enumerate(labels):
    #     print(l, get_cost[i], put_cost[i], vm_cost[i], storage_cost[i], get_cost[i] + put_cost[i] + vm_cost[i] + storage_cost[i])
    # print()

def find_good_opt_get(availability_target):
    def good_opt_get(conf):
        words = conf.split('|')
        if words[1] == '0':
            return int(words[3]) >= int(words[4])
        else:
            return int(words[3]) >= int(words[6])

    results, confs = get_results()
    f_index = availability_targets.index(availability_target)

    labels = []
    get_cost = []
    put_cost = []
    vm_cost = []
    storage_cost = []

    for workload in workloads:
        exec = "optimized"
        if results[f_index][exec + "_" + workload] == "INVALID":
            continue
        # elif confs[f_index][exec + "_" + workload][2] == "0":
        if good_opt_get(confs[f_index][exec + "_" + workload]):
            print(workload, confs[f_index][exec + "_" + workload])

    return

    dist1 = "dist_O"
    dist2 = "dist_LO"

    dist1_workloads = []
    dist2_workloads = []

    for workload in workloads:
        if workload.find(dist1) != -1:
            dist1_workloads.append(workload)
        if workload.find(dist2) != -1:
            dist2_workloads.append(workload)

    for i, workload in enumerate(dist1_workloads):
        exec = "optimized"
        print(exec + "_" + workload + ":", confs[f_index][exec + "_" + workload])
        print(exec + "_" + dist2_workloads[i] + ":", confs[f_index][exec + "_" + dist2_workloads[i]])
        print()

    # print(workload[:workload.find(".json")] + ":")
    # print(" get_cost put_cost vm_cost storage_cost total")
    # for exec in executions[f_index]:
    #     if exec != "optimized":
    #         continue
    #     if results[f_index][exec + "_" + workload] == "INVALID":
    #         print("WARN: no data found for " + exec + "_" + workload)
    #         continue
    #     labels.append(exec if exec.find("baseline") == -1 else exec[9:])
    #     values = results[f_index][exec + "_" + workload].split()
    #     # print(values)
    #     get_cost.append(float(values[0]))
    #     put_cost.append(float(values[1]))
    #     vm_cost.append(float(values[2]))
    #     storage_cost.append(float(values[3]))
    #     print(exec + ":", confs[f_index][exec + "_" + workload])
    # print()

    # for i, l in enumerate(labels):
    #     print(l, get_cost[i], put_cost[i], vm_cost[i], storage_cost[i], get_cost[i] + put_cost[i] + vm_cost[i] + storage_cost[i])
    # print()

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
        if results[f_index][exec + "_" + workload] == "INVALID":
            print("WARN: no data found for " + "optimized" + "_" + workload)
            continue
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
    if results[f_index]["optimized" + "_" + workload] == "INVALID":
        print("WARN: no data found for " + "optimized" + "_" + workload)
        return
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
        if results[f_index][exec + "_" + workload] == "INVALID":
            print("WARN: no data found for " + exec + "_" + workload)
            return
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

    ax.set_ylabel('Normalized to the optimizer output(%)')
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

def get_exec_name(exec):
    if exec == "baseline_fixed_ABD":
        return "ABD Fixed"
    elif exec == "baseline_fixed_CAS":
        return "CAS Fixed"
    elif exec == "baseline_abd_nearest":
        return "ABD Nearest"
    elif exec == "baseline_cas_nearest":
        return "CAS Nearest"
    elif exec == "baseline_abd":
        return "ABD Only Optimal"
    elif exec == "baseline_cas":
        return "CAS Only Optimal"

plot_cumulative_max_x = 0
def plot_cumulative(workloads, availability_target):
    def plot_one_exec(fig, ax, exec, workloads, norm_total_cost):
        global plot_cumulative_max_x
        granularity = 100.
        counter = [0 for _ in range(int(1000 * granularity))]
        number_workload = 0
        for workload in workloads:
            # if workload == "dist_SS_1KB_500_HW_500.json":
            #     print(norm_total_cost[workload][exec])
            if norm_total_cost[workload][exec] == -1:
                continue
            counter[int(norm_total_cost[workload][exec] * granularity)] += 1
            number_workload += 1

        for j in range(len(counter) - 1):
            counter[j + 1] += counter[j]

        # if exec == "baseline_cas_nearest":
        #     for j in range(len(counter)):
        #         # if counter[j] != 0:
        #             print(counter[j])

        max_x = 0
        for i, c in enumerate(counter):
            if c == number_workload:
                max_x = (i / granularity + 1) / 100.
                break

        colors = OrderedDict([
            ("optimized", "g"),
            ("baseline_fixed_ABD", "saddlebrown"),
            ("baseline_fixed_CAS", "r"),
            ("baseline_abd_nearest", "magenta"),
            ("baseline_cas_nearest", "black"),
            ("baseline_abd", "b"),
            ("baseline_cas", "g")
        ])

        line_types = OrderedDict([
            ("optimized", "g"),
            ("baseline_fixed_ABD", ":"),
            ("baseline_fixed_CAS", ":"),
            ("baseline_abd_nearest", "--"),
            ("baseline_cas_nearest", "--"),
            ("baseline_abd", "-"),
            ("baseline_cas", "-")
        ])

        # fig, ax = plt.subplots()
        ax.plot(np.array(range(int(1000 * granularity))) / granularity / 100., np.array(counter), label=get_exec_name(exec), linewidth=10, color=colors[exec], linestyle=line_types[exec]) #linestyle=':'
        ax.set_ylabel('Cumulative Count of Workloads')
        ax.set_xlabel('Normalized Cost of Baseline.')
        # ax.set_title(exec)
        # ax.legend()
        plt.grid(True)
        limits = ax.axis()
        # print(limits)

        print("number of possible workloads for " + exec + " is: " + str(max(counter)) + " (impossible: " + str(len(workloads) - max(counter)) + ")")

        if plot_cumulative_max_x < max_x:
            plot_cumulative_max_x = max_x
        ax.axis([1.0, plot_cumulative_max_x, 0.0, len(workloads)])

    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    labels = []
    norm_total_cost = OrderedDict()

    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        labels.append(exec if exec.find("baseline") == -1 else exec[9:])


    # print("SHAHH")
    # shah_counter = 0
    # for workload in workloads:
    #     if results[f_index]["baseline_cas" + "_" + workload] == "INVALID":
    #         continue
    #
    #     # print(confs[f_index]["baseline_cas" + "_" + workload][2])
    #     if confs[f_index]["baseline_cas" + "_" + workload][2] == '1':
    #         print(workload)
    #         shah_counter += 1
    # print(shah_counter)
    # print("SHAHH")
    # return

    for workload in workloads:
        optimized = []
        if results[f_index]["optimized" + "_" + workload] == "INVALID":
            print("WARN: no data found for " + "optimized" + "_" + workload)
            return
        values = results[f_index]["optimized" + "_" + workload].split()
        optimized.append(float(values[0]))
        optimized.append(float(values[1]))
        optimized.append(float(values[2]))
        optimized.append(float(values[3]))
        optimized_cost = sum(optimized)
        norm_total_cost[workload] = OrderedDict()

        # values = results[f_index]["baseline_abd" + "_" + workload].split()
        # s_val_abd = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])
        # values = results[f_index]["baseline_cas" + "_" + workload].split()
        # s_val_cas = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])
        # if s_val_abd < s_val_cas:
        #     print("AAAA", workload, s_val_abd, s_val_cas)


        for exec in executions[f_index]:
            if exec == "optimized":
                continue
            if results[f_index][exec + "_" + workload] == "INVALID":
                print("WARN: no data found for " + exec + "_" + workload)
                norm_total_cost[workload][exec] = -1
                continue
            values = results[f_index][exec + "_" + workload].split()
            # print(values)
            norm_total_cost[workload][exec] = (float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])) / optimized_cost * 100
            # get_cost.append(float(values[0]))
            # put_cost.append(float(values[1]))
            # vm_cost.append(float(values[2]))
            # storage_cost.append(float(values[3]))
            # print(exec + ":", confs[f_index][exec + "_" + workload])
    # print(len(workloads))

    # for workload in workloads:
    #     if norm_total_cost[workload]["baseline_cas"] != 100.0:
    #         print(workload)
        # print(norm_total_cost[workload]["baseline_cas"])
    plt.rcParams.update({'font.size': 48})
    fig, ax = plt.subplots()
    plot_cumulative_max_x = 0
    for exec in executions[f_index]:
        if exec == "optimized": #or exec == "baseline_fixed_ABD" or exec == "baseline_fixed_CAS":
            continue
        plot_one_exec(fig, ax, exec, workloads, norm_total_cost)
    ax.legend(loc="lower right")

    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        max_cost = 0
        geo_mean_cost = 1.
        for workload in workloads:
            if norm_total_cost[workload][exec] == -1:
                continue
            if max_cost < norm_total_cost[workload][exec]:
                max_cost = norm_total_cost[workload][exec]
            # print(norm_total_cost[workload][exec])
            geo_mean_cost *= norm_total_cost[workload][exec]**(1/float(len(workloads)))
        print("for " + exec + ": max is", max_cost, "and geo_mean is", geo_mean_cost)
        # break

def plot_cumulative2(workloads, availability_target):
    def plot_one_exec(fig, ax, exec, workloads, norm_total_cost):
        step_len = 10
        counter = [0 for _ in range(int(1000))]
        cost_savings = []
        for workload in workloads:
            cost_saving = int(norm_total_cost[workload][exec]) - 100
            cost_savings.append(cost_saving)
            counter_index = int(cost_saving / step_len)
            counter[counter_index] += 1

        step_number = 0
        i = len(counter) - 1
        while i != -1:
            if counter[i] != 0:
                step_number = i + 1
                break
            i -= 1

        ys = counter[0:step_number]
        xs = range(0, step_number * step_len, step_len)

        data = [[30, 25, 50, 20],
                [40, 23, 51, 17],
                [35, 22, 45, 19]]
        X = np.arange(4)
        print(X)
        ax.hist(cost_savings, xs)
        # ax.hist(X + 0.00, data[0], color='b', width=0.25)
        # ax.bar(X + 0.25, data[1], color='g', width=0.25)
        # ax.bar(X + 0.50, data[2], color='r', width=0.25)

        # ys = []
        # step_len = 5
        # min_bound = 0
        # max_bound = min_bound + step_len
        # the_y = 0
        # for i, c in enumerate(counter):
        #     the_y += 1
        #     if c >= max_bound:
        #         ys.append(the_y)
        #         max_bound += step_len
        #         the_y = 0

        # fig, ax = plt.subplots()
        # ax.plot(np.array(range(int(1000 * granularity))) / granularity, np.array(counter) / len(workloads) * 100., label=exec)
        ax.set_ylabel('Number of workloads')
        ax.set_xlabel('Saving-cost normalized to the optimizer output(%)')
        ax.set_title(exec)
        # ax.legend()
        plt.grid(True)
        # limits = ax.axis()
        # print(limits)

        # if plot_cumulative_max_x < max_x:
        #     plot_cumulative_max_x = max_x
        # ax.axis([0.0, plot_cumulative_max_x, 0.0, limits[3]])

    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    labels = []
    norm_total_cost = OrderedDict()

    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        labels.append(exec if exec.find("baseline") == -1 else exec[9:])

    for workload in workloads:
        optimized = []
        if results[f_index]["optimized" + "_" + workload] == "INVALID":
            print("WARN: no data found for " + "optimized" + "_" + workload)
            return
        values = results[f_index]["optimized" + "_" + workload].split()
        optimized.append(float(values[0]))
        optimized.append(float(values[1]))
        optimized.append(float(values[2]))
        optimized.append(float(values[3]))
        optimized_cost = sum(optimized)
        norm_total_cost[workload] = OrderedDict()

        # values = results[f_index]["baseline_abd" + "_" + workload].split()
        # s_val_abd = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])
        # values = results[f_index]["baseline_cas" + "_" + workload].split()
        # s_val_cas = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])
        # if s_val_abd < s_val_cas:
        #     print("AAAA", workload, s_val_abd, s_val_cas)


        for exec in executions[f_index]:
            if exec == "optimized":
                continue
            if results[f_index][exec + "_" + workload] == "INVALID":
                print("WARN: no data found for " + exec + "_" + workload)
                return
            values = results[f_index][exec + "_" + workload].split()
            # print(values)
            norm_total_cost[workload][exec] = (float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])) / optimized_cost * 100
            # get_cost.append(float(values[0]))
            # put_cost.append(float(values[1]))
            # vm_cost.append(float(values[2]))
            # storage_cost.append(float(values[3]))
            # print(exec + ":", confs[f_index][exec + "_" + workload])
    # print(len(workloads))

    # for workload in workloads:
    #     if norm_total_cost[workload]["baseline_cas"] != 100.0:
    #         print(workload)
        # print(norm_total_cost[workload]["baseline_cas"])

    plt.rcParams.update({'font.size': 24})
    # fig, ax = plt.subplots()
    # fig = plt.figure(figsize=(10, 5))
    # ax = fig.add_axes([0.1, 0.1, .8, .8])
    # ax = fig.add_subplot()
    plot_cumulative_max_x = 0
    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        fig = plt.figure(figsize=(10, 5))
        ax = fig.add_subplot()
        plot_one_exec(fig, ax, exec, workloads, norm_total_cost)
        ax.legend()

    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        max_cost = 0
        geo_mean_cost = 1.
        for workload in workloads:
            if max_cost < norm_total_cost[workload][exec]:
                max_cost = norm_total_cost[workload][exec]
            # print(norm_total_cost[workload][exec])
            geo_mean_cost *= norm_total_cost[workload][exec]**(1/float(len(workloads)))
        print("for " + exec + ": max is", max_cost, "and geo_mean is", geo_mean_cost)
        # break

bins = [0, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256]
def plot_cumulative3(workloads, availability_target):
    def plot_one_exec(fig, ax, ax2, exec, workloads, norm_total_cost, bar_place):
        global plot_cumulative_max_x
        granularity = 100.
        counter = [0 for _ in range(int(1000 * granularity))]
        number_workload = 0
        for workload in workloads:
            # if workload == "dist_SS_1KB_500_HW_500.json":
            #     print(norm_total_cost[workload][exec])
            if norm_total_cost[workload][exec] == -1:
                continue
            counter[int((norm_total_cost[workload][exec] - 100) * granularity)] += 1
            number_workload += 1

        ys = []
        for i, bin in enumerate(bins[:-1]):
            lower_bound = int(bin * granularity)
            upper_bound = int(bins[i + 1] * granularity)
            ys.append(0)
            for j in range(lower_bound, upper_bound):
                ys[-1] += counter[j]

        # print(ys)

        labels = []
        for i, bin in enumerate(bins[:-1]):
            label = "[" + str(bin) + "," + str(bins[i + 1]) + (")" if i is not len(bins) - 2 else "]")
            labels.append(label)

        # print(labels)
        # x = np.arange(0.0, len(labels))  # the label locations
        # print(x)
        x = [0.0 for _ in range(len(labels))]
        x = np.array(x)
        x[0] = 0.0
        x[1] = x[0] + 1.1
        x[2] = x[1] + .92
        x[3] = x[2] + .9
        x[4] = x[3] + .9
        x[5] = x[4] + .9
        x[6] = x[5] + 1.1
        x[7] = x[6] + 1.2
        x[8] = x[7] + 1.4
        x[9] = x[8] + 1.6
        # x[10] = x[9] + 1.6
        # print(x)

        width = 0.12  # the width of the bars
        if bar_place < 3:
            rects1 = ax.bar(x - width / 2 - (2 - bar_place) * width, ys, width, label=exec)
            rects2 = ax2.bar(x - width / 2 - (2 - bar_place) * width, ys, width, label=exec)
        else:
            rects1 = ax.bar(x + width / 2 + (bar_place - 3) * width, ys, width, label=exec)
            rects2 = ax2.bar(x + width / 2 + (bar_place - 3) * width, ys, width, label=exec)


        # men_means = [20, 34, 30, 35, 27]
        # women_means = [25, 32, 34, 20, 25]
        #
        # rects1 = ax.bar(x - width / 2, men_means, width, label='Men')
        # rects2 = ax.bar(x + width / 2, women_means, width, label='Women')

        # Add some text for labels, title and custom x-axis tick labels, etc.
        # ax.set_ylabel('Scores')
        # ax.set_title('Scores by group and gender')
        ax2.set_xticks(x)
        ax.set_xticks(x)
        ax2.set_xticklabels(labels)
        # ax.legend()
        ax.set_ylim(520, 580)  # outlier 2
        ax.set_yticks([525, 575])
        ax2.set_ylim(0, 280)  # most of the data

        d = .015  # how big to make the diagonal lines in axes coordinates
        kwargs = dict(transform=ax.transAxes, color='k', clip_on=False)
        ax.plot((-d, +d), (5.69 * -d, 5.69 * +d), **kwargs)
        ax.plot((1 - d, 1 + d), (5.69 * -d, 5.69 * +d), **kwargs)

        kwargs.update(transform=ax2.transAxes)
        ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)
        ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)

        # kwargs = dict(transform=ax2.transAxes, color='k', clip_on=False)
        # ax.plot((-d, +d), (-d, +d), **kwargs)
        # ax.plot((1 - d, 1 + d), (-d, +d), **kwargs)

        # kwargs.update(transform=ax3.transAxes)
        # ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)
        # ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)

        ax2.spines['top'].set_visible(False)
        ax2.tick_params(labeltop=False)

        ax.spines['top'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.tick_params(labeltop=False)
        ax.tick_params(labelbottom=False)

        # for rect, label in zip(rects, labels):
        #     height = rect.get_height()
        #     ax.text(rect.get_x() + rect.get_width() / 2, height + 5, height,
        #             ha='center', va='bottom')

        # ax.bar_label(the_bar, padding=3)
        # ax.bar_label(rects2, padding=3)


        # fig, ax = plt.subplots()
        # ax.plot(np.array(range(int(1000 * granularity))) / granularity - 100., np.array(counter), label=exec, linewidth=4) #linestyle=':'
        ax2.set_ylabel('Number of workloads')
        ax2.set_xlabel('Cost-saving interval normalized to the cost of the optimizer output(%)')
        # ax.set_title(exec)
        # ax.legend()
        ax.grid(True, axis="y")
        ax2.grid(True, axis="y")
        limits = ax.axis()
        # print(limits)

        print("number of possible workloads for " + exec + " is: " + str(sum(counter)) + " (impossible: " + str(len(workloads) - sum(counter)) + ")")

        # if plot_cumulative_max_x < max_x:
        #     plot_cumulative_max_x = max_x
        # ax.axis([0.0, plot_cumulative_max_x, 0.0, len(workloads)])

    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    labels = []
    norm_total_cost = OrderedDict()

    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        labels.append(exec if exec.find("baseline") == -1 else exec[9:])

    for workload in workloads:
        optimized = []
        if results[f_index]["optimized" + "_" + workload] == "INVALID":
            print("WARN: no data found for " + "optimized" + "_" + workload)
            return
        values = results[f_index]["optimized" + "_" + workload].split()
        optimized.append(float(values[0]))
        optimized.append(float(values[1]))
        optimized.append(float(values[2]))
        optimized.append(float(values[3]))
        optimized_cost = sum(optimized)
        norm_total_cost[workload] = OrderedDict()

        # values = results[f_index]["baseline_abd" + "_" + workload].split()
        # s_val_abd = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])
        # values = results[f_index]["baseline_cas" + "_" + workload].split()
        # s_val_cas = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])
        # if s_val_abd < s_val_cas:
        #     print("AAAA", workload, s_val_abd, s_val_cas)


        for exec in executions[f_index]:
            if exec == "optimized":
                continue
            if results[f_index][exec + "_" + workload] == "INVALID":
                print("WARN: no data found for " + exec + "_" + workload)
                norm_total_cost[workload][exec] = -1
                continue
            values = results[f_index][exec + "_" + workload].split()
            # print(values)
            norm_total_cost[workload][exec] = (float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])) / optimized_cost * 100
            # get_cost.append(float(values[0]))
            # put_cost.append(float(values[1]))
            # vm_cost.append(float(values[2]))
            # storage_cost.append(float(values[3]))
            # print(exec + ":", confs[f_index][exec + "_" + workload])
    # print(len(workloads))

    # for workload in workloads:
    #     if norm_total_cost[workload]["baseline_cas"] != 100.0:
    #         print(workload)
        # print(norm_total_cost[workload]["baseline_cas"])
    plt.rcParams.update({'font.size': 38})
    # fig, ax = plt.subplots()
    # fig, (ax, ax2) = plt.subplots(2, 1)

    fig = plt.figure()
    ax = fig.add_axes([0.07, 0.86, .88, .13])
    ax2 = fig.add_axes([0.07, 0.1, .88, .74])

    bar_place = 0
    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        plot_one_exec(fig, ax, ax2, exec, workloads, norm_total_cost, bar_place)
        bar_place += 1
    ax2.legend(loc="upper left", bbox_to_anchor=(0.1, 0.98))

    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        max_cost = 0
        geo_mean_cost = 1.
        for workload in workloads:
            if norm_total_cost[workload][exec] == -1:
                continue
            if max_cost < norm_total_cost[workload][exec]:
                max_cost = norm_total_cost[workload][exec]
            # print(norm_total_cost[workload][exec])
            geo_mean_cost *= norm_total_cost[workload][exec]**(1/float(len(workloads)))
        print("for " + exec + ": max is", max_cost, "and geo_mean is", geo_mean_cost)
        # break

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
            print(workload)

    print("#ABD:", abd_num, "#CAS:", cas_num)

def when_read_ratio_changes(availability_target, should_print=True):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    exec = "optimized"

    placement_for_HW = []
    placement_for_RW = []
    placement_for_HR = []
    the_workloads = []

    for workload in workloads:
        if workload.find("_HW_") != -1:
            placement_for_HW.append([confs[f_index][exec + "_" + workload][0], confs[f_index][exec + "_" + workload][2]])
        elif workload.find("_RW_") != -1:
            placement_for_RW.append([confs[f_index][exec + "_" + workload][0], confs[f_index][exec + "_" + workload][2]])
        elif workload.find("_HR_") != -1:
            placement_for_HR.append([confs[f_index][exec + "_" + workload][0], confs[f_index][exec + "_" + workload][2]])
            the_workloads.append(workload)

    if should_print:
        # HW -> RW
        for i, p in enumerate(placement_for_HW):
            if p[0] != placement_for_RW[i][0] or p[1] != placement_for_RW[i][1]:
                print("HW -> RW", the_workloads[i] + ":", p, "and", placement_for_RW[i])
                workload1 = the_workloads[i].replace("HR", "HW")
                workload2 = the_workloads[i].replace("HR", "RW")
                print(workload1, confs[f_index][exec + "_" + workload1])
                print(workload2, confs[f_index][exec + "_" + workload2])
                print()

        # HW -> HR
        for i, p in enumerate(placement_for_HW):
            if p[0] != placement_for_HR[i][0] or p[1] != placement_for_HR[i][1]:
                print("HW -> HR", the_workloads[i] + ":", p, "and", placement_for_HR[i])
                workload1 = the_workloads[i].replace("HR", "HW")
                workload2 = the_workloads[i] #.replace("HR", "RW")
                print(workload1, confs[f_index][exec + "_" + workload1])
                print(workload2, confs[f_index][exec + "_" + workload2])
                print()

        # RW -> HR
        for i, p in enumerate(placement_for_RW):
            if p[0] != placement_for_HR[i][0] or p[1] != placement_for_HR[i][1]:
                print("RW -> HR", the_workloads[i] + ":", p, "and", placement_for_HR[i])
                workload1 = the_workloads[i].replace("HR", "RW")
                workload2 = the_workloads[i] #.replace("HR", "RW")
                print(workload1, confs[f_index][exec + "_" + workload1])
                print(workload2, confs[f_index][exec + "_" + workload2])
                print()

    open_sublime()

    return placement_for_HW, placement_for_RW, placement_for_HR, the_workloads

def when_arrival_rate_changes(availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    exec = "optimized"

    placement_for_50 = []
    placement_for_200 = []
    placement_for_500 = []
    the_workloads_50 = []
    the_workloads_200 = []
    the_workloads_500 = []

    for workload in workloads:
        if workload.find("_50_") != -1:
            placement_for_50.append([confs[f_index][exec + "_" + workload][0], confs[f_index][exec + "_" + workload][2]])
            the_workloads_50.append(workload)
        elif workload.find("_200_") != -1:
            placement_for_200.append([confs[f_index][exec + "_" + workload][0], confs[f_index][exec + "_" + workload][2]])
            the_workloads_200.append(workload)
        elif workload.find("_500_") != -1:
            placement_for_500.append([confs[f_index][exec + "_" + workload][0], confs[f_index][exec + "_" + workload][2]])
            the_workloads_500.append(workload)

    # 50 -> 200
    for i, p in enumerate(placement_for_50):
        if p[0] != placement_for_200[i][0] or p[1] != placement_for_200[i][1]:
            print("50 -> 200", the_workloads_50[i] + ":", p, "and", placement_for_200[i])
            workload1 = the_workloads_50[i]
            workload2 = the_workloads_200[i]
            print(workload1, confs[f_index][exec + "_" + workload1])
            print(workload2, confs[f_index][exec + "_" + workload2])
            print()
            # workload1 = the_workloads[i].replace("_500_", "_50_")
            # workload2 = the_workloads[i].replace("_500_", "_200_")
            # print(workload1, confs[f_index][exec + "_" + workload1])
            # print(workload2, confs[f_index][exec + "_" + workload2])
            # print()

    # 50 -> 500
    for i, p in enumerate(placement_for_50):
        if p[0] != placement_for_500[i][0] or p[1] != placement_for_500[i][1]:
            print("50 -> 500", the_workloads_50[i] + ":", p, "and", placement_for_500[i])
            workload1 = the_workloads_50[i]
            workload2 = the_workloads_500[i]
            print(workload1, confs[f_index][exec + "_" + workload1])
            print(workload2, confs[f_index][exec + "_" + workload2])
            print()

    # 200 -> 500
    for i, p in enumerate(placement_for_200):
        if p[0] != placement_for_500[i][0] or p[1] != placement_for_500[i][1]:
            print("200 -> 500", the_workloads_200[i] + ":", p, "and", placement_for_500[i])
            workload1 = the_workloads_200[i]
            workload2 = the_workloads_500[i]
            print(workload1, confs[f_index][exec + "_" + workload1])
            print(workload2, confs[f_index][exec + "_" + workload2])
            print()

    open_sublime()

def when_baseline_is_better(availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    norm_total_cost = OrderedDict()

    for workload in workloads:
        optimized = []
        if results[f_index]["optimized" + "_" + workload] == "INVALID":
            print("WARN: no data found for " + "optimized" + "_" + workload)
            return
        values = results[f_index]["optimized" + "_" + workload].split()
        optimized.append(float(values[0]))
        optimized.append(float(values[1]))
        optimized.append(float(values[2]))
        optimized.append(float(values[3]))
        optimized_cost = sum(optimized)
        norm_total_cost[workload] = OrderedDict()
        for exec in executions[f_index]:
            if exec == "optimized":
                continue
            if results[f_index][exec + "_" + workload] == "INVALID":
                print("WARN: no data found for " + exec + "_" + workload)
                return
            values = results[f_index][exec + "_" + workload].split()
            norm_total_cost[workload][exec] = (float(values[0]) + float(values[1]) + float(values[2]) + float(
                values[3])) / optimized_cost * 100

    for workload in workloads:
        for exec in executions[f_index]:
            if exec == "optimized":
                continue
            if results[f_index][exec + "_" + workload] == "INVALID":
                print("WARN: no data found for " + exec + "_" + workload)
                return
            if norm_total_cost[workload][exec] < 100.0:
                print(workload, exec)
            # values = results[f_index][exec + "_" + workload].split()
            # norm_total_cost[workload][exec] = (float(values[0]) + float(values[1]) + float(values[2]) + float(
            #     values[3])) / optimized_cost * 100

def get_lat(conf_str):
    def find_sep_occurrence(occur):
        val = -1
        for i in range(0, occur):
            val = conf_str.find("|", val + 1)
        return val
    if conf_str[2] == "0": #ABD
        write_latency = conf_str[find_sep_occurrence(10)+1:find_sep_occurrence(11)]
        read_latency = conf_str[find_sep_occurrence(11)+1:]
        selected_dcs = conf_str[find_sep_occurrence(2)+1:find_sep_occurrence(3)]
        # print("AAAAA", write_latency, read_latency)
    else:
        write_latency = conf_str[find_sep_occurrence(12) + 1:find_sep_occurrence(13)]
        read_latency = conf_str[find_sep_occurrence(13) + 1:]
        selected_dcs = conf_str[find_sep_occurrence(2) + 1:find_sep_occurrence(3)]
        # print("BBBBB", write_latency, read_latency)

    selected_dcs = selected_dcs.split(",")
    return int(write_latency), int(read_latency), selected_dcs

def when_EC_lat_better(availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)

    exec = "optimized"
    # exec = "baseline_cas"

    min_lat_diff = 1000000000

    # Viveck's view
    for workload in workloads:
        ec_put_lat, ec_get_lat, ec_selected_dcs = get_lat(confs[f_index]["baseline_cas_nearest" + "_" + workload])
        k = int(confs[f_index]["baseline_cas_nearest" + "_" + workload][2])
        rep_put_lat, rep_get_lat, rep_selected_dcs = get_lat(confs[f_index]["baseline_abd_nearest" + "_" + workload])
        if set(rep_selected_dcs).issubset(set(ec_selected_dcs)) and k > 1:
            if min_lat_diff >= (ec_get_lat - rep_get_lat):
                min_lat_diff = (ec_get_lat - rep_get_lat)
                picked_workload = workload
                picked_rep_selected_dcs = rep_selected_dcs
                picked_ec_selected_dcs = ec_selected_dcs
                picked_rep_get_lat = rep_get_lat
                picked_ec_get_lat = ec_get_lat

            # if ec_get_lat <= rep_get_lat:
            #     print(workload)
            #     print(rep_selected_dcs, ec_selected_dcs)
            #     print(rep_get_lat, ec_get_lat)
            #     print()

    print(min_lat_diff, picked_workload, picked_rep_selected_dcs, picked_ec_selected_dcs, picked_rep_get_lat, picked_ec_get_lat)
    print("baseline_abd_nearest", confs[f_index]["baseline_abd_nearest" + "_" + picked_workload])
    print("baseline_cas_nearest", confs[f_index]["baseline_cas_nearest" + "_" + picked_workload])

    # Cost-basis view
    # for workload in workloads:
    #     ec_put_lat, ec_get_lat, ec_selected_dcs = get_lat(confs[f_index]["baseline_cas" + "_" + workload])
    #     k = int(confs[f_index]["baseline_cas" + "_" + workload][2])
    #     rep_put_lat, rep_get_lat, rep_selected_dcs = get_lat(confs[f_index]["baseline_abd" + "_" + workload])
    #     if set(rep_selected_dcs).issubset(set(ec_selected_dcs)) and k > 1:
    #         if ec_get_lat <= rep_get_lat:
    #             print(workload)
    #             print(rep_selected_dcs, ec_selected_dcs, k)
    #             print(rep_get_lat, ec_get_lat)
    #             print()

    open_sublime()

def nearest_experiment(workloads, availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    # workload = workload if workload.find(".json") != -1 else workload + ".json"

    labels = []
    get_cost = []
    put_cost = []
    vm_cost = []
    storage_cost = []

    max_profit_cas = 0.
    max_profit_abd = 0.
    best_workload = ""

    profits = []

    for workload in workloads:
        if workload.find("HW") != -1:
            continue
        if workload.find("dist_S") == -1:
            continue
        exec = "baseline_abd_nearest"
        if results[f_index][exec + "_" + workload] == "INVALID":
            continue
        values = results[f_index][exec + "_" + workload].split()
        nabd_total_cost = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])

        exec = "baseline_cas_nearest"
        if results[f_index][exec + "_" + workload] == "INVALID":
            continue
        values = results[f_index][exec + "_" + workload].split()
        ncas_total_cost = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])

        exec = "optimized"
        if results[f_index][exec + "_" + workload] == "INVALID":
            continue
        values = results[f_index][exec + "_" + workload].split()
        opt_total_cost = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])

        if opt_total_cost < nabd_total_cost and opt_total_cost < ncas_total_cost:
            # print(workload, ":", opt_total_cost, nabd_total_cost, ncas_total_cost)
            profits.append(ncas_total_cost / opt_total_cost)
            # if max_profit_cas + max_profit_abd < ncas_total_cost / opt_total_cost + nabd_total_cost / opt_total_cost:
            #     max_profit_cas = ncas_total_cost / opt_total_cost
            #     max_profit_abd = nabd_total_cost / opt_total_cost
            #     best_workload = workload

            if max_profit_cas < ncas_total_cost / opt_total_cost:
                max_profit_cas = ncas_total_cost / opt_total_cost
                max_profit_abd = nabd_total_cost / opt_total_cost
                best_workload = workload

    for profit in profits:
        if "{:.4f}".format(profit) == "{:.4f}".format(max_profit_cas):
            print("AAAA")

    print("best:", best_workload, max_profit_cas, max_profit_abd)

def mis_prediction_robustness(availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)
    files_path = os.path.join(directory, "f=" + str(availability_targets[f_index]))

    lat = 1000

    # client dist.
    workload_counter = 0
    conf_change_counter = 0
    for object_size in object_sizes:
        for storage_size in storage_sizes:
            for arrival_rate in arrival_rates:
                for read_ratio in read_ratios:
                    workload_counter += 1
                    old_configuration = ""
                    for client_dist in client_dists:
                        # for lat in SLO_latencies:

                        num_objects = storage_sizes[storage_size] / object_sizes[object_size]
                        FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(arrival_rate) + "_" + \
                                    read_ratio + "_" + str(lat) + ".json"
                        workload = FILE_NAME

                        res_file = os.path.join(files_path, "res_" + "optimized" + "_" + workload)
                        data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                        for grp_index, grp in enumerate(data):
                            if grp["protocol"] != "abd":
                                configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(
                                    grp["selected_dcs"]) + \
                                                "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q3"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q4"]))
                            else:
                                configuration = str(grp["m"]) + "|" + "0" + "|" + list_reformat(
                                    grp["selected_dcs"]) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"]))

                        if old_configuration != "" and old_configuration != configuration:
                            conf_change_counter += 1
                            break

                        old_configuration = configuration
    print("client dist:", "{:.2f}".format(float(conf_change_counter) / float(workload_counter)) )

    # object_size
    workload_counter = 0
    conf_change_counter = 0
    for client_dist in client_dists:
        for storage_size in storage_sizes:
            for arrival_rate in arrival_rates:
                for read_ratio in read_ratios:
                    workload_counter += 1
                    old_configuration = ""
                    for object_size in object_sizes:
                        # for lat in SLO_latencies:

                        num_objects = storage_sizes[storage_size] / object_sizes[object_size]
                        FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(
                            arrival_rate) + "_" + \
                                    read_ratio + "_" + str(lat) + ".json"
                        workload = FILE_NAME

                        res_file = os.path.join(files_path, "res_" + "optimized" + "_" + workload)
                        data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                        for grp_index, grp in enumerate(data):
                            if grp["protocol"] != "abd":
                                configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(
                                    grp["selected_dcs"]) + \
                                                "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q3"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q4"]))
                            else:
                                configuration = str(grp["m"]) + "|" + "0" + "|" + list_reformat(
                                    grp["selected_dcs"]) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"]))

                        if old_configuration != "" and old_configuration != configuration:
                            conf_change_counter += 1
                            break

                        old_configuration = configuration
    print("object size:", "{:.2f}".format(float(conf_change_counter) / float(workload_counter)))

    # storage_size
    workload_counter = 0
    conf_change_counter = 0
    for client_dist in client_dists:
        for object_size in object_sizes:
            for arrival_rate in arrival_rates:
                for read_ratio in read_ratios:
                    workload_counter += 1
                    old_configuration = ""
                    for storage_size in storage_sizes:
                        # for lat in SLO_latencies:

                        num_objects = storage_sizes[storage_size] / object_sizes[object_size]
                        FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(
                            arrival_rate) + "_" + \
                                    read_ratio + "_" + str(lat) + ".json"
                        workload = FILE_NAME

                        res_file = os.path.join(files_path, "res_" + "optimized" + "_" + workload)
                        data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                        for grp_index, grp in enumerate(data):
                            if grp["protocol"] != "abd":
                                configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(
                                    grp["selected_dcs"]) + \
                                                "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q3"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q4"]))
                            else:
                                configuration = str(grp["m"]) + "|" + "0" + "|" + list_reformat(
                                    grp["selected_dcs"]) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"]))

                        if old_configuration != "" and old_configuration != configuration:
                            conf_change_counter += 1
                            break

                        old_configuration = configuration
    print("storage size:", "{:.2f}".format(float(conf_change_counter) / float(workload_counter)))

    # arrival_rate
    workload_counter = 0
    conf_change_counter = 0
    for client_dist in client_dists:
        for object_size in object_sizes:
            for storage_size in storage_sizes:
                for read_ratio in read_ratios:
                    workload_counter += 1
                    old_configuration = ""
                    for arrival_rate in arrival_rates:
                        # for lat in SLO_latencies:

                        num_objects = storage_sizes[storage_size] / object_sizes[object_size]
                        FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(
                            arrival_rate) + "_" + \
                                    read_ratio + "_" + str(lat) + ".json"
                        workload = FILE_NAME

                        res_file = os.path.join(files_path, "res_" + "optimized" + "_" + workload)
                        data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                        for grp_index, grp in enumerate(data):
                            if grp["protocol"] != "abd":
                                configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(
                                    grp["selected_dcs"]) + \
                                                "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q3"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q4"]))
                            else:
                                configuration = str(grp["m"]) + "|" + "0" + "|" + list_reformat(
                                    grp["selected_dcs"]) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"]))

                        if old_configuration != "" and old_configuration != configuration:
                            conf_change_counter += 1
                            break

                        old_configuration = configuration
    print("arrival rate:", "{:.2f}".format(float(conf_change_counter) / float(workload_counter)))

    # read_ratio
    workload_counter = 0
    conf_change_counter = 0
    for client_dist in client_dists:
        for object_size in object_sizes:
            for storage_size in storage_sizes:
                for arrival_rate in arrival_rates:
                    workload_counter += 1
                    old_configuration = ""
                    for read_ratio in read_ratios:
                        # for lat in SLO_latencies:

                        num_objects = storage_sizes[storage_size] / object_sizes[object_size]
                        FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(
                            arrival_rate) + "_" + \
                                    read_ratio + "_" + str(lat) + ".json"
                        workload = FILE_NAME

                        res_file = os.path.join(files_path, "res_" + "optimized" + "_" + workload)
                        data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                        for grp_index, grp in enumerate(data):
                            if grp["protocol"] != "abd":
                                configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(
                                    grp["selected_dcs"]) + \
                                                "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q3"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q4"]))
                            else:
                                configuration = str(grp["m"]) + "|" + "0" + "|" + list_reformat(
                                    grp["selected_dcs"]) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                                    len(grp["client_placement_info"]["0"]["Q2"]))

                        if old_configuration != "" and old_configuration != configuration:
                            conf_change_counter += 1
                            break

                        old_configuration = configuration
    print("read ratio:", "{:.2f}".format(float(conf_change_counter) / float(workload_counter)))

def get_conf(res_file):
    data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
    for grp_index, grp in enumerate(data):
        if grp["protocol"] != "abd":
            configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(
                grp["selected_dcs"]) + \
                            "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                len(grp["client_placement_info"]["0"]["Q2"])) + "|" + str(
                len(grp["client_placement_info"]["0"]["Q3"])) + "|" + str(
                len(grp["client_placement_info"]["0"]["Q4"]))
        else:
            configuration = str(grp["m"]) + "|" + "0" + "|" + list_reformat(
                grp["selected_dcs"]) + "|" + str(
                len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
                len(grp["client_placement_info"]["0"]["Q2"]))
            
    return configuration

def get_res_files(characteristic):
    for client_dist in client_dists:
        for object_size in object_sizes:
            for storage_size in storage_sizes:
                for arrival_rate in arrival_rates:
                    for read_ratio in read_ratios:
                        old_configuration = ""

                        # for lat in SLO_latencies:

                        num_objects = storage_sizes[storage_size] / object_sizes[object_size]
                        FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(arrival_rate) + "_" + \
                                    read_ratio + "_" + str(lat) + ".json"
                        workload = FILE_NAME

                        res_file = os.path.join(files_path, "res_" + "optimized" + "_" + workload)

def change_arrival_rate(file_name, new_arrival_rate):
    index_start = file_name.rfind("_", 0, file_name.rfind("_", 0, file_name.rfind("_")))
    index_end = file_name.rfind("_", 0, file_name.rfind("_"))

    ret = file_name[0:index_start + 1] + str(new_arrival_rate) + file_name[index_end:]
    return ret

def mis_prediction():
    # results, confs = get_results()
    # f_index = availability_targets.index(availability_target)
    # files_path = os.path.join(directory, "f=" + str(availability_targets[f_index]))

    lat = 1000

    # object_size
    workload_counter = 0
    conf_change_low_counter = 0
    conf_change_high_counter = 0
    low_path = "RESULTS/prediction/workloads_OS_Low/f=1"
    main_path = "RESULTS/workloads_1000ms/f=1"
    high_path = "RESULTS/prediction/workloads_OS_High/f=1"

    for object_size in object_sizes:
        for workload in workloads:
            if workload_satisfies(workload, [object_size]):
                workload_counter += 1
                file = "res_" + "optimized" + "_" + workload
                low_res_file = os.path.join(low_path, file)
                main_res_file = os.path.join(main_path, file)
                high_res_file = os.path.join(high_path, file)

                if get_conf(low_res_file) != get_conf(main_res_file):
                    conf_change_low_counter += 1

                if get_conf(high_res_file) != get_conf(main_res_file):
                    conf_change_high_counter += 1

        print("object size: " + object_size + "(-10%)", "{:.2f}".format(float(conf_change_low_counter) / float(workload_counter)))
        print("object size: " + object_size + "(+10%)", "{:.2f}".format(float(conf_change_high_counter) / float(workload_counter)))

    # storage_size
    workload_counter = 0
    conf_change_low_counter = 0
    conf_change_high_counter = 0
    low_path = "RESULTS/prediction/workloads_SS_Low/f=1"
    main_path = "RESULTS/workloads_1000ms/f=1"
    high_path = "RESULTS/prediction/workloads_SS_High/f=1"

    for storage_size in storage_sizes:
        for workload in workloads:
            if workload_satisfies(workload, [storage_size]):
                workload_counter += 1
                file = "res_" + "optimized" + "_" + workload
                low_res_file = os.path.join(low_path, file)
                main_res_file = os.path.join(main_path, file)
                high_res_file = os.path.join(high_path, file)

                if get_conf(low_res_file) != get_conf(main_res_file):
                    conf_change_low_counter += 1

                if get_conf(high_res_file) != get_conf(main_res_file):
                    conf_change_high_counter += 1

        print("storage size: " + storage_size + "(-10%)", "{:.2f}".format(float(conf_change_low_counter) / float(workload_counter)))
        print("storage size: " + storage_size + "(+10%)", "{:.2f}".format(float(conf_change_high_counter) / float(workload_counter)))


    # read_ratio
    workload_counter = 0
    conf_change_low_counter = 0
    conf_change_high_counter = 0
    low_path = "RESULTS/prediction/workloads_RR_Low/f=1"
    main_path = "RESULTS/workloads_1000ms/f=1"
    high_path = "RESULTS/prediction/workloads_RR_High/f=1"

    for read_ratio in read_ratios:
        for workload in workloads:
            if workload_satisfies(workload, [read_ratio]):
                workload_counter += 1
                file = "res_" + "optimized" + "_" + workload
                low_res_file = os.path.join(low_path, file)
                main_res_file = os.path.join(main_path, file)
                high_res_file = os.path.join(high_path, file)

                if get_conf(low_res_file) != get_conf(main_res_file):
                    conf_change_low_counter += 1

                if get_conf(high_res_file) != get_conf(main_res_file):
                    conf_change_high_counter += 1

        print("read ratio: " + read_ratio + "(-10%)", "{:.2f}".format(float(conf_change_low_counter) / float(workload_counter)))
        print("read ratio: " + read_ratio + "(+10%)", "{:.2f}".format(float(conf_change_high_counter) / float(workload_counter)))

    # arrival_rate
    workload_counter = 0
    conf_change_low_counter = 0
    conf_change_high_counter = 0
    low_path = "RESULTS/prediction/workloads_AR_Low/f=1"
    main_path = "RESULTS/workloads_1000ms/f=1"
    high_path = "RESULTS/prediction/workloads_AR_High/f=1"

    for arrival_rate in arrival_rates:
        for workload in workloads:
            if workload_satisfies(workload, ["_" + str(arrival_rate) + "_"]):
                workload_counter += 1
                file = "res_" + "optimized" + "_" + workload
                file = change_arrival_rate(file, int(arrival_rate * 0.9))
                low_res_file = os.path.join(low_path, file)

                file = "res_" + "optimized" + "_" + workload
                main_res_file = os.path.join(main_path, file)

                file = "res_" + "optimized" + "_" + workload
                file = change_arrival_rate(file, int(arrival_rate * 1.1))
                high_res_file = os.path.join(high_path, file)

                if get_conf(low_res_file) != get_conf(main_res_file):
                    conf_change_low_counter += 1

                if get_conf(high_res_file) != get_conf(main_res_file):
                    conf_change_high_counter += 1

        print("arrival rate: " + str(arrival_rate) + "(-10%)", "{:.2f}".format(float(conf_change_low_counter) / float(workload_counter)))
        print("arrival rate: " + str(arrival_rate) + "(+10%)", "{:.2f}".format(float(conf_change_high_counter) / float(workload_counter)))

def workload_satisfies(workload, attrs):
    for a in attrs:
        if workload.find(a) == -1:
            return False
    return True

def write_latencies(availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)

    myworkloads = []

    for client_dist in client_dists:
        object_size = next(reversed(object_sizes))
        storage_size = next(reversed(storage_sizes))
        arrival_rate = arrival_rates[-1]
        read_ratio = list(read_ratios.keys())
        read_ratio = read_ratio[int(len(read_ratio)/2)]
        lat = SLO_latencies[0]
        num_objects = storage_sizes[storage_size] / object_sizes[object_size]
        FILE_NAME = client_dist + "_" + object_size + "_" + storage_size + "_" + str(arrival_rate) + "_" + \
                    read_ratio + "_" + str(lat) + ".json"
        myworkloads.append(FILE_NAME)

    for workload in myworkloads:
        ec_put_lat, ec_get_lat, ec_selected_dcs = get_lat(confs[f_index]["baseline_cas_nearest" + "_" + workload])
        k = int(confs[f_index]["baseline_cas_nearest" + "_" + workload][2])
        rep_put_lat, rep_get_lat, rep_selected_dcs = get_lat(confs[f_index]["baseline_abd_nearest" + "_" + workload])
        print(distribution_full_name[workload[:workload.find("_", workload.find("_") + 1)]] + "," + str(rep_put_lat) + "," + str(rep_get_lat) + "," + str(ec_put_lat) + "," + str(ec_get_lat))

def plot_storage_cost_cold_ratio(availability_target):
    def get_storage_cost(costs):
        sc = costs[costs.rfind(" "):]
        return float(sc)

    results, confs, num_of_objects = get_results2()
    f_index = availability_targets.index(availability_target)

    max_number_of_object = 104857600
    total_storage_price = 0.438 / (730*3600)

    labels = []
    get_cost = []
    put_cost = []
    vm_cost = []
    storage_cost = []

    total_cost_subopt = OrderedDict()
    total_cost_opt = OrderedDict()

    cost_without_cold_store = OrderedDict()

    # print(workload[:workload.find(".json")] + ":")
    # print(" get_cost put_cost vm_cost storage_cost total")
    for workload in workloads:
        for exec in executions[f_index]:
            if exec != "optimized":
                continue
            if results[f_index][exec + "_" + workload] == "INVALID":
                print("WARN: no data found for " + exec + "_" + workload)
                continue
            labels.append(exec if exec.find("baseline") == -1 else exec[9:])
            values = results[f_index][exec + "_" + workload].split()
            # print(values)
            # get_cost.append(float(values[0]))
            # put_cost.append(float(values[1]))
            # vm_cost.append(float(values[2]))
            # storage_cost.append(float(values[3]))
            # print(exec + ":", confs[f_index][exec + "_" + workload])

            # total_cost_subopt[workload] = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3])

            cost_without_cold_store = float(values[0]) + float(values[1]) + float(values[2]) + float(values[3]) / num_of_objects[f_index][workload][0]

            for m in range(1, max_number_of_object + 1, 10000000):
                cost_of_cold_store_subopt = float(m) * float(values[3]) / num_of_objects[f_index][workload][0]
                total_cost_subopt[str(m) + "_" + workload] = float(values[0]) + float(values[1]) + float(values[2]) + cost_of_cold_store_subopt

                # cost_of_cold_store = (num_of_objects[f_index][workload][0] - 1) * (total_storage_price) * (num_of_objects[f_index][workload][1] / 7.)
                cost_of_cold_store = (m) * (total_storage_price) * (num_of_objects[f_index][workload][1] / 7.)
                total_cost_opt[str(m) + "_" + workload] = cost_of_cold_store + cost_without_cold_store

    total_cost_subopt_overall = OrderedDict()
    total_cost_opt_overall = OrderedDict()
    for m in range(1, max_number_of_object + 1, 10000000):
        total_cost_subopt_overall[m] = 0
        total_cost_opt_overall[m] = 0
        for workload in workloads:
            total_cost_subopt_overall[m] += total_cost_subopt[str(m) + "_" + workload]
            total_cost_opt_overall[m] += total_cost_opt[str(m) + "_" + workload]

    colors = ["r", "b", "g"]

    plt.rcParams.update({'font.size': 38})
    area = 100
    # fig = plt.figure()
    # ax = fig.add_axes([0.09, 0.2, .70, .78])
    fig, ax = plt.subplots()

    x = range(1, max_number_of_object + 1, 10000000)
    real_x = np.array(range(1, max_number_of_object + 1, 10000000)) * len(workloads)

    y = []
    for each_x in x:
        y.append(total_cost_subopt_overall[each_x])
    ax.plot(real_x, y, linewidth=5, c=colors[1], label="Sub optimal")

    y = []
    for each_x in x:
        y.append(total_cost_opt_overall[each_x])
    ax.plot(real_x, y, linewidth=5, c=colors[2], label="Optimal")


    major_x_ticks = list(range(0, max_number_of_object* len(workloads), 12500000 * 200))
    major_x_ticks.append(major_x_ticks[-1] + 12500000 * 200)
    ax.set_xticks(major_x_ticks)
    labels = []
    for tt in major_x_ticks:
        labels.append(str(int(tt/100000000)))
    ax.set_xticklabels(labels)

    # labels = cold_ratios



    # plt.rcParams.update({'font.size': 38})
    # area = 100
    # # fig = plt.figure()
    # # ax = fig.add_axes([0.09, 0.2, .70, .78])
    # fig, ax = plt.subplots()
    # x = labels # np.arange(0.0, len(labels))  # the label locations
    # print(x)
    # # ax.set_xticks(x)
    # # ax.set_xticklabels(labels)
    # # ax.axvline(x=7.5, color='k', lw=5)
    # # ax.axvline(x=15.5, color='k', lw=5)
    #
    # # ax.scatter(x, cold_storage_cost, s=1.75*area, c=colors[1])
    # ax.plot(x, total_storage_cost, "--", linewidth=4, c="darkgoldenrod", label="Hot + cold")
    # # ax.scatter(x, rwks[1], s=1.75 * area, c=colors[1])
    # # ax.plot(x, rwks[1], "--", c=colors[1], label="HR - 100KB")
    #
    # # ax.scatter(x, cold_storage_cost, s=1.75*area, c=colors[1])
    # ax.plot(x, total_storage_cost_opt, "-.", linewidth=4, c="darkgreen", label="Hot + optimized cold")
    # # ax.scatter(x, rwks[1], s=1.75 * area, c=colors[1])
    # # ax.plot(x, rwks[1], "--", c=colors[1], label="HR - 100KB")
    #
    # # ax.scatter(x, hot_storage_cost, s=3*area, c=colors[0])
    # ax.plot(x, hot_storage_cost, linewidth=4, c=colors[0], label="Hot objects")
    # # print(m_k_pair)
    # # ax.scatter(x, hwks[1], s=3 * area, c=colors[0])
    # # ax.plot(x, hwks[1], "--", c=colors[0], label="HW - 100KB")
    #
    # # ax.scatter(x, ms, s=3 * area, c=colors[0])
    # # ax.plot(x, ms, "-.", c=colors[0], label="HW")
    #
    # # ax.scatter(x, cold_storage_cost, s=1.75*area, c=colors[1])
    # ax.plot(x, cold_storage_cost, linewidth=4, c=colors[1], label="Cold objects")
    # # ax.scatter(x, rwks[1], s=1.75 * area, c=colors[1])
    # # ax.plot(x, rwks[1], "--", c=colors[1], label="HR - 100KB")
    #
    # # ax.scatter(x, opt_cold_storage_cost, s=.5*area, c=colors[2])
    # ax.plot(x, opt_cold_storage_cost, linewidth=4, c=colors[2], label="Optimized cold objects")
    # # ax.scatter(x, hrks[1], s=.5 * area, c=colors[2])
    # # ax.plot(x, hrks[1], "--", c=colors[2], label="HR - 100KB")

    ax.legend()

    # major_ticks = np.arange(0, 10, 1)
    # minor_ticks = np.arange(0, 1001, 25)

    # ax.set_yticks(major_ticks)
    # ax.set_yticks(minor_ticks, minor=True)

    # ax.grid(which='minor', alpha=0.4)
    # ax.grid(which='major', alpha=0.8)
    ax.grid()
    ax.spines['top'].set_visible(False)
    ax.tick_params(labeltop=False)

    limits = ax.axis()
    ax.axis([0, limits[1], 0, limits[3]])

    # ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left')
    ax.set_ylabel('Total cost ($/hour)')
    ax.set_xlabel('Number of cold objects (100 Million)')

def plot_all(availability_target):
    for workload in workloads:
        # print("A")
        # interesting cases
        attrs = ["dist_ST", "HR"] #, "HW"] #workload.find(list(client_dists.keys())[2])
        # attrs = ["10KB", "HR"]
        if workload_satisfies(workload, attrs):
            plot_workload(workload, availability_target)
            # plot_normalized(workload, availability_target)

    os.system("subl drawer_output.txt")

if __name__ == "__main__":
    global workloads
    workloads = get_workloads()

    # print_configurations_count()

    # counting(2)

    # interesting_workloads = ["dist_LO_1KB_500_RW_1000.json", "dist_L_1KB_50_RW_1000"]
    # for wl in interesting_workloads:
    #     print_workload(wl, 2)
    #     plot_normalized(wl, 2)

    # extractor_type2()
    # extractor_type3()

    # plot_workload(list(client_dists.keys())[2] + "_" + "10KB" + "_" + str(arrival_rates[1]) + "_" + "RW" + "_" + "1000" + ".json")
    # plot_workload(list(client_dists.keys())[2] + "_" + "1KB" + "_" + str(arrival_rates[1]) + "_" + "RW" + "_" + "1000" + ".json")

    # counting(1)
    # when_read_ratio_changes(1)
    # when_arrival_rate_changes(1)

    # when_baseline_is_better(1)

    # when_EC_lat_better(2)

    # write_latencies(2)

    # plot_all(1)
    plot_cumulative(workloads, 1)
    # plot_cumulative2(workloads, 1)
    # plot_cumulative3(workloads, 1)

    # nearest_experiment(workloads, 1)
    # plot_workload("dist_SS_1KB_100GB_500_HR_1000.json", 1)
    # plot_workload("dist_ST_1KB_100GB_500_HR_1000.json", 1)

    # mis_prediction_robustness(1)
    # mis_prediction()

    # print_workload2(1)
    # find_good_opt_get(1)

    # plot_storage_cost_cold_ratio(1)

    # os.system("subl drawer_output.txt")
    sys.stdout.flush()
    plt.show()


