#!/usr/bin/python3

import json
import os, sys, time
from collections import OrderedDict
import matplotlib.pyplot as plt
import numpy as np
from workload_def import *
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap

# directory = "workloads"
directory = "RESULTS/workloads_cost_k"
# directory = "RESULTS/workloads_lat_sense_1KB"
# directory = "RESULTS/workloads_object_size_sense"
# directory = "RESULTS/workloads_object_size_sense3"
# directory = "RESULTS/workloads_arrival_rate_sense2"
# directory = "RESULTS/workloads_arrival_rate_sense"
# directory = "RESULTS/workloads_500ms"
# directory = "RESULTS/workloads_1000ms"
# directory = "workloads_Mar24_2152"
# directory = "/home/shahrooz/Desktop/PSU/Research/LEGOstore/scripts/data/ALL_optimizer_Mar19_0817"

m = 9

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
                        for lat in SLO_latencies:
                        # lat = 1000

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

def get_results_k():
    all_results = []
    confs = []
    # m = 9

    for f_index, f in enumerate(availability_targets):
        files_path = os.path.join(directory, "f=" + str(f))
        all_results.append(OrderedDict())
        confs.append(OrderedDict())
        for k in range(1, m - 1, 1):
            exec = str(k)
        # for exec in executions[f_index]:
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
                max_x = i / granularity + 1
                break

        # fig, ax = plt.subplots()
        ax.plot(np.array(range(int(1000 * granularity))) / granularity - 100., np.array(counter), label=exec, linewidth=4) #linestyle=':'
        ax.set_ylabel('CDF across workloads')
        ax.set_xlabel('Cost-saving normalized to the cost of the optimizer output(%)')
        # ax.set_title(exec)
        # ax.legend()
        plt.grid(True)
        limits = ax.axis()
        # print(limits)

        print("number of possible workloads for " + exec + " is: " + str(max(counter)) + " (impossible: " + str(len(workloads) - max(counter)) + ")")

        if plot_cumulative_max_x < max_x:
            plot_cumulative_max_x = max_x
        ax.axis([0.0, plot_cumulative_max_x, 0.0, len(workloads)])

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
    plt.rcParams.update({'font.size': 48})
    fig, ax = plt.subplots()
    plot_cumulative_max_x = 0
    for exec in executions[f_index]:
        if exec == "optimized":
            continue
        plot_one_exec(fig, ax, exec, workloads, norm_total_cost)
    ax.legend()

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

def plot_scatter_slo_latency(workloads, availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)

    exec = "optimized"

    # data = OrderedDict()
    labels = []

    impossilbes_x = []
    impossilbes_y = []

    abd_x = []
    abd_y = []

    cas1_x = []
    cas1_y = []

    ec_x = []
    ec_y = []

    i = 0
    for cd in client_dists:
        # data[cd] = []  # 0: impossible, 1: Rep, 2: EC
        labels.append(distribution_full_name[cd])
        for workload in workloads:
            if workload.find("HW") == -1:
                continue
            if workload.find(cd) != -1:
                lat = int(workload[workload.rfind("_")+1:workload.find(".json")])
                if results[f_index][exec + "_" + workload] == "INVALID":
                    impossilbes_x.append(i)
                    impossilbes_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "0":
                    abd_x.append(i)
                    abd_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "1":
                    cas1_x.append(i)
                    cas1_y.append(lat)
                else:
                    ec_x.append(i)
                    ec_y.append(lat)
        i += 1

    for cd in client_dists:
        # data[cd] = []  # 0: impossible, 1: Rep, 2: EC
        labels.append(distribution_full_name[cd])
        for workload in workloads:
            if workload.find("RW") == -1:
                continue
            if workload.find(cd) != -1:
                lat = int(workload[workload.rfind("_") + 1:workload.find(".json")])
                if results[f_index][exec + "_" + workload] == "INVALID":
                    impossilbes_x.append(i)
                    impossilbes_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "0":
                    abd_x.append(i)
                    abd_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "1":
                    cas1_x.append(i)
                    cas1_y.append(lat)
                else:
                    ec_x.append(i)
                    ec_y.append(lat)
        i += 1

    for cd in client_dists:
        # data[cd] = []  # 0: impossible, 1: Rep, 2: EC
        labels.append(distribution_full_name[cd])
        for workload in workloads:
            if workload.find("HR") == -1:
                continue
            if workload.find(cd) != -1:
                lat = int(workload[workload.rfind("_") + 1:workload.find(".json")])
                if results[f_index][exec + "_" + workload] == "INVALID":
                    impossilbes_x.append(i)
                    impossilbes_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "0":
                    abd_x.append(i)
                    abd_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "1":
                    cas1_x.append(i)
                    cas1_y.append(lat)
                else:
                    ec_x.append(i)
                    ec_y.append(lat)
        i += 1

    plt.rcParams.update({'font.size': 38})
    area = 100
    fig = plt.figure()
    ax = fig.add_axes([0.09, 0.4, .90, .58])
    # fig, ax = plt.subplots()

    x = np.arange(0.0, len(labels))  # the label locations
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation='vertical')
    ax.axvline(x=7.5, color='k', lw=5)
    ax.axvline(x=15.5, color='k', lw=5)
    colors = ["r", "b", "c", "g"]


    ax.scatter(impossilbes_x, impossilbes_y, s=area, c=colors[0], label="Infeasible")
    ax.scatter(abd_x, abd_y, s=area, c=colors[1], label="ABD")
    ax.scatter(cas1_x, cas1_y, s=area, c=colors[2], label="CAS K=1")
    ax.scatter(ec_x, ec_y, s=area, c=colors[3], label="CAS K>1")

    major_ticks = np.arange(0, 1001, 100)
    minor_ticks = np.arange(0, 1001, 25)

    ax.set_yticks(major_ticks)
    ax.set_yticks(minor_ticks, minor=True)

    ax.grid(which='minor', alpha=0.4)
    ax.grid(which='major', alpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.tick_params(labeltop=False)

    # for cd in client_dists:
    #     for i, state in enumerate(data[cd]):
    #         if state == 0:
    #             ax.scatter([labels.index(cd)], SLO_latencies[i], c=colors[0])
    #         elif state == 1:
    #             ax.scatter([labels.index(cd)], SLO_latencies[i], c=colors[1])
    #         else:
    #             ax.scatter([labels.index(cd)], SLO_latencies[i], c=colors[2])

    # for state in [0, 1, 2]:
    #     for i, lat in enumerate(SLO_latencies):
    #         ys =


    ax.grid(True)
    # ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left')
    ax.set_ylabel('SLO latency (ms)')
    ax.set_xlabel('Client Distribution')

def plot_scatter_slo_latency2(workloads, availability_target):
    results, confs = get_results()
    f_index = availability_targets.index(availability_target)

    exec = "optimized"

    # data = OrderedDict()
    labels = []

    impossilbes_x = []
    impossilbes_y = []

    abd_x = []
    abd_y = []

    cas1_x = []
    cas1_y = []

    ec_x = []
    ec_y = []

    i = 0
    for cd in client_dists:
        # data[cd] = []  # 0: impossible, 1: Rep, 2: EC
        labels.append(distribution_full_name[cd])
        for workload in workloads:
            if workload.find("HW") == -1:
                continue
            if workload.find(cd) != -1:
                lat = int(workload[workload.rfind("_")+1:workload.find(".json")])
                if results[f_index][exec + "_" + workload] == "INVALID":
                    impossilbes_x.append(i)
                    impossilbes_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "0":
                    abd_x.append(i)
                    abd_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "1":
                    cas1_x.append(i)
                    cas1_y.append(lat)
                else:
                    ec_x.append(i)
                    ec_y.append(lat)
        i += 1

    for cd in client_dists:
        # data[cd] = []  # 0: impossible, 1: Rep, 2: EC
        labels.append(distribution_full_name[cd])
        for workload in workloads:
            if workload.find("RW") == -1:
                continue
            if workload.find(cd) != -1:
                lat = int(workload[workload.rfind("_") + 1:workload.find(".json")])
                if results[f_index][exec + "_" + workload] == "INVALID":
                    impossilbes_x.append(i)
                    impossilbes_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "0":
                    abd_x.append(i)
                    abd_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "1":
                    cas1_x.append(i)
                    cas1_y.append(lat)
                else:
                    ec_x.append(i)
                    ec_y.append(lat)
        i += 1

    for cd in client_dists:
        # data[cd] = []  # 0: impossible, 1: Rep, 2: EC
        labels.append(distribution_full_name[cd])
        for workload in workloads:
            if workload.find("HR") == -1:
                continue
            if workload.find(cd) != -1:
                lat = int(workload[workload.rfind("_") + 1:workload.find(".json")])
                if results[f_index][exec + "_" + workload] == "INVALID":
                    impossilbes_x.append(i)
                    impossilbes_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "0":
                    abd_x.append(i)
                    abd_y.append(lat)
                elif confs[f_index][exec + "_" + workload][2] == "1":
                    cas1_x.append(i)
                    cas1_y.append(lat)
                else:
                    ec_x.append(i)
                    ec_y.append(lat)
        i += 1

    print(i, len(SLO_latencies))
    a = [[0] * i for _ in range(len(SLO_latencies))]

    for t, val_x in enumerate(impossilbes_x):
        a[SLO_latencies.index(impossilbes_y[t])][val_x] = 0

    for t, val_x in enumerate(abd_x):
        a[SLO_latencies.index(abd_y[t])][val_x] = 1

    for t, val_x in enumerate(cas1_x):
        a[SLO_latencies.index(cas1_y[t])][val_x] = 2

    for t, val_x in enumerate(ec_x):
        a[SLO_latencies.index(ec_y[t])][val_x] = 3

    # a = np.random.random((16, 16))
    # print(a)
    b = [[0] * i for _ in range(2)]
    for r in a:
        b.append(r)
    a = b

    print(a)
    # return

    plt.rcParams.update({'font.size': 38})
    area = 100
    fig = plt.figure()
    ax = fig.add_axes([0.09, 0.4, 1.1, .58])
    # ax = fig.add_axes([0.09, 0.4, .90, .58])

    # ax.imshow(a, cmap='hot', interpolation='nearest')
    # ax = sns.heatmap(a, linewidth=0.5)

    #(0.84, 0.15, 0.16, 1.0) : tab:red

    myColors = ((0.70, 0.13, 0.13, 1.0), (0.12, 0.47, 0.71, 1.0), (0.28, 0.82, 0.8, 1.0), (0.2, 0.80, 0.2, 1.0))
    cmap = LinearSegmentedColormap.from_list('Custom', myColors, len(myColors))

    ax = sns.heatmap(a, cmap=cmap, linewidths=.5, linecolor='lightgray')

    # Manually specify colorbar labelling after it's been generated
    colorbar = ax.collections[0].colorbar
    colorbar.set_ticks([0.375, 1.125, 1.875, 2.625])
    colorbar.set_ticklabels(['Infeasible', 'ABD', 'CAS K=1', 'CAS K>1'])

    plt.gca().invert_yaxis()

    x = np.arange(0.0, len(labels))  # the label locations
    ax.set_xticks(x + 0.5)
    ax.set_xticklabels(labels, rotation='vertical')
    ax.axvline(x=7.5 + 0.5, color='k', lw=5)
    ax.axvline(x=15.5 + 0.5, color='k', lw=5)
    # plt.show()

    major_ticks = np.arange(0, 1001, 25)
    # minor_ticks = np.arange(0, 1001, 25)
    # for i, val in enumerate(major_ticks):
    #     if i % 4 != 2:
    #         major_ticks[i] = ""

    y = np.arange(0.0, len(SLO_latencies) + 2)
    ax.set_yticks(y + 0.5)
    ax.set_yticklabels(major_ticks)
    plt.locator_params(axis='y', nbins=10)



    # ax.set_yticks(major_ticks)
    # ax.set_yticks(minor_ticks, minor=True)

    # ax.grid(which='minor', alpha=0.4)
    # ax.grid(which='major', alpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.tick_params(labeltop=False)

    ax.set_ylabel('Latency SLO (msec)')
    ax.set_xlabel('User Location')

    return

    plt.rcParams.update({'font.size': 38})
    area = 100
    fig = plt.figure()
    ax = fig.add_axes([0.09, 0.4, .90, .58])
    # fig, ax = plt.subplots()

    x = np.arange(0.0, len(labels))  # the label locations
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation='vertical')
    ax.axvline(x=7.5, color='k', lw=5)
    ax.axvline(x=15.5, color='k', lw=5)
    colors = ["r", "b", "c", "g"]


    ax.scatter(impossilbes_x, impossilbes_y, s=area, c=colors[0], label="Infeasible")
    ax.scatter(abd_x, abd_y, s=area, c=colors[1], label="ABD")
    ax.scatter(cas1_x, cas1_y, s=area, c=colors[2], label="CAS K=1")
    ax.scatter(ec_x, ec_y, s=area, c=colors[3], label="CAS K>1")

    major_ticks = np.arange(0, 1001, 100)
    minor_ticks = np.arange(0, 1001, 25)

    ax.set_yticks(major_ticks)
    ax.set_yticks(minor_ticks, minor=True)

    ax.grid(which='minor', alpha=0.4)
    ax.grid(which='major', alpha=0.8)
    ax.spines['top'].set_visible(False)
    ax.tick_params(labeltop=False)

    # for cd in client_dists:
    #     for i, state in enumerate(data[cd]):
    #         if state == 0:
    #             ax.scatter([labels.index(cd)], SLO_latencies[i], c=colors[0])
    #         elif state == 1:
    #             ax.scatter([labels.index(cd)], SLO_latencies[i], c=colors[1])
    #         else:
    #             ax.scatter([labels.index(cd)], SLO_latencies[i], c=colors[2])

    # for state in [0, 1, 2]:
    #     for i, lat in enumerate(SLO_latencies):
    #         ys =


    ax.grid(True)
    # ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left')
    ax.set_ylabel('SLO latency (ms)')
    ax.set_xlabel('Client Distribution')

def plot_sense_to_object_size(workloads, availability_target):
    def get_object_size(workload):
        size = workload[workload.find("_") + 1:workload.find("_", workload.find("_") + 1)]
        if size.find("B") != -1:
            return size
        size = workload[workload.find("_", workload.find("_") + 1) + 1:workload.find("_", workload.find("_", workload.find("_") + 1) + 1)]
        return size

    results, confs = get_results()
    f_index = availability_targets.index(availability_target)

    exec = "baseline_cas"

    arrival_rate = 200

    # data = OrderedDict()
    labels = list(object_sizes.keys())

    get_object_size(workloads[0])

    hwks = [-1 for _ in range(len(object_sizes))]
    ms = [-1 for _ in range(len(object_sizes))]
    rwks = [-1 for _ in range(len(object_sizes))]
    hrks = [-1 for _ in range(len(object_sizes))]

    colors = ["r", "b", "g"]

    i = 1
    for cd in client_dists:
        if cd != "dist_ST":
            continue
        print("Figure " + str(i) + ": " + cd)
        i += 1
        # for workload in workloads:
        #     if workload.find(cd) == -1 or workload.find("_" + str(arrival_rate) + "_") == -1:
        #         continue
        #     if workload.find("HW") == -1:
        #         continue
        #
        #     object_size = get_object_size(workload)
        #     # print(object_size)
        #     hwks[list(object_sizes.keys()).index(object_size)] = int(confs[f_index][exec + "_" + workload][2])
        #     ms[list(object_sizes.keys()).index(object_size)] = int(confs[f_index][exec + "_" + workload][0])

        for workload in workloads:
            if workload.find(cd) == -1 or workload.find("_" + str(arrival_rate) + "_") == -1:
                continue
            if workload.find("RW") == -1:
                continue

            object_size = get_object_size(workload)
            # print(object_size)
            rwks[list(object_sizes.keys()).index(object_size)] = int(confs[f_index][exec + "_" + workload][2])

        # for workload in workloads:
        #     if workload.find(cd) == -1 or workload.find("_" + str(arrival_rate) + "_") == -1:
        #         continue
        #     if workload.find("HR") == -1:
        #         continue
        #
        #     object_size = get_object_size(workload)
        #     # print(object_size)
        #     hrks[list(object_sizes.keys()).index(object_size)] = int(confs[f_index][exec + "_" + workload][2])

        plt.rcParams.update({'font.size': 38*1.6})
        # area = 100
        area = 500
        fig = plt.figure()
        ax = fig.add_axes([0.08, 0.14, .85, .80])
        # fig = plt.figure()
        # ax = fig.add_axes([0.09, 0.2, .70, .78])
        # fig, ax = plt.subplots()
        x = np.arange(0.0, len(labels))  # the label locations
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        # ax.axvline(x=7.5, color='k', lw=5)
        # ax.axvline(x=15.5, color='k', lw=5)

        # ax.scatter(x, hwks, s=3*area, c=colors[0])
        # ax.plot(x, hwks, c=colors[0], label="HW")
        # ax.scatter(x, ms, s=3 * area, c=colors[0])
        # ax.plot(x, ms, "-.", c=colors[0], label="HW")

        ax.scatter(x, rwks, s=1.75*area, linewidth=10.0, c=colors[1])
        ax.plot(x, rwks, c=colors[1], linewidth=5.0)

        # ax.scatter(x, hrks, s=.5*area, c=colors[2])
        # ax.plot(x, hrks, c=colors[2], label="HR")

        # ax.legend()

        major_ticks = np.arange(0, 10, 1)
        # minor_ticks = np.arange(0, 1001, 25)

        ax.set_yticks(major_ticks)
        # ax.set_yticks(minor_ticks, minor=True)

        ax.grid(which='minor', alpha=0.4)
        ax.grid(which='major', alpha=0.8)
        ax.spines['top'].set_visible(False)
        ax.tick_params(labeltop=False)

        # ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left')
        params = {'mathtext.default': 'regular'}
        plt.rcParams.update(params)
        ax.set_ylabel('$K_{Opt}$')
        ax.set_xlabel('Object size')

def plot_sense_to_arrival_rate(workloads, availability_target):
    def get_arrival_rate(workload):
        workload = workload[0:workload.rfind("_")]
        workload = workload[0:workload.rfind("_")]
        ar = workload[workload.rfind("_") + 1:]
        return int(ar)

    results, confs = get_results()
    f_index = availability_targets.index(availability_target)

    exec = "baseline_cas"

    object_size = list(object_sizes.keys())[0]

    m_k_pair = []

    # data = OrderedDict()
    labels = arrival_rates[0:21:4]

    hwks = [[-1 for _ in range(len(labels))] for i in range(2)]
    ms = [[-1 for _ in range(len(labels))] for i in range(2)]
    rwks = [[-1 for _ in range(len(labels))] for i in range(2)]
    hrks = [[-1 for _ in range(len(labels))] for i in range(2)]

    colors = ["r", "b", "g"]

    i = 1
    for cd in client_dists:
        if cd != "dist_ST":
            continue
        print("Figure " + str(i) + ": " + cd)
        i += 1
        for object_size_index, object_size in enumerate(list(object_sizes.keys())[0:-1]):
            # print("SSSS")
            # for workload in workloads:
            #     if workload.find(cd) == -1 or workload.find("_" + str(object_size) + "_") == -1:
            #         continue
            #     if workload.find("HW") == -1:
            #         continue
            #
            #     ar = get_arrival_rate(workload)
            #     if not ar in labels:
            #         continue
            #     # print(object_size)
            #     hwks[object_size_index][labels.index(ar)] = int(confs[f_index][exec + "_" + workload][2])
            #     ms[object_size_index][labels.index(ar)] = int(confs[f_index][exec + "_" + workload][0])
            #     # m_k_pair.append([int(confs[f_index][exec + "_" + workload][0]), int(confs[f_index][exec + "_" + workload][2])])

            for workload in workloads:
                if workload.find(cd) == -1 or workload.find("_" + str(object_size) + "_") == -1:
                    continue
                if workload.find("RW") == -1:
                    continue

                ar = get_arrival_rate(workload)
                if not ar in labels:
                    continue
                # print(object_size)
                rwks[object_size_index][labels.index(ar)] = int(confs[f_index][exec + "_" + workload][2])

            # for workload in workloads:
            #     if workload.find(cd) == -1 or workload.find("_" + str(object_size) + "_") == -1:
            #         continue
            #     if workload.find("HR") == -1:
            #         continue
            #
            #     ar = get_arrival_rate(workload)
            #     if not ar in labels:
            #         continue
            #     # print(object_size)
            #     hrks[object_size_index][labels.index(ar)] = int(confs[f_index][exec + "_" + workload][2])

            plt.rcParams.update({'font.size': 38*1.6})
            # area = 100
            area = 500
            fig = plt.figure()
            ax = fig.add_axes([0.08, 0.14, .85, .80])
            # fig, ax = plt.subplots()
            x = labels # np.arange(0.0, len(labels))  # the label locations
            # ax.set_xticks(x)
            # ax.set_xticklabels(labels)
            # ax.axvline(x=7.5, color='k', lw=5)
            # ax.axvline(x=15.5, color='k', lw=5)

            # ax.scatter(x, hwks[0], s=3*area, c=colors[0])
            # ax.plot(x, hwks[0], c=colors[0], label="HW")
            # print(m_k_pair)
            # ax.scatter(x, hwks[1], s=3 * area, c=colors[0])
            # ax.plot(x, hwks[1], "--", c=colors[0], label="HW - 100KB")

            # ax.scatter(x, ms, s=3 * area, c=colors[0])
            # ax.plot(x, ms, "-.", c=colors[0], label="HW")

            ax.scatter(x, rwks[0], s=1.75*area, linewidth=10, c=colors[1])
            ax.plot(x, rwks[0], linewidth=5.0, c=colors[1])
            # ax.scatter(x, rwks[1], s=1.75 * area, c=colors[1])
            # ax.plot(x, rwks[1], "--", c=colors[1], label="HR - 100KB")

            # ax.scatter(x, hrks[0], s=.5*area, c=colors[2])
            # ax.plot(x, hrks[0], c=colors[2], label="HR")
            # ax.scatter(x, hrks[1], s=.5 * area, c=colors[2])
            # ax.plot(x, hrks[1], "--", c=colors[2], label="HR - 100KB")

            # ax.legend()

            major_ticks = np.arange(50, 650, 100)
            ax.set_xticks(major_ticks)

            major_ticks = np.arange(0, 10, 1)
            # minor_ticks = np.arange(0, 1001, 25)

            ax.set_yticks(major_ticks)
            # ax.set_yticks(minor_ticks, minor=True)

            ax.grid(which='minor', alpha=0.4)
            ax.grid(which='major', alpha=0.8)
            ax.spines['top'].set_visible(False)
            ax.tick_params(labeltop=False)

            limits = ax.axis()
            ax.axis([limits[0], 600, limits[2], limits[3]])

            # ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left')
            params = {'mathtext.default': 'regular'}
            plt.rcParams.update(params)
            ax.set_ylabel('$K_{Opt}$')
            ax.set_xlabel('Arrival rate (req/sec)')

            # minor_ticks = np.arange(0, 600, 25)
            # ax.set_xticks(major_ticks)
            # ax.set_xticks(minor_ticks, minor=True)
            # ax.grid(which='minor', alpha=0.5)
            ax.grid(which='major', alpha=1)


def cost_const_k(workload, availability_target):
    results, confs = get_results_k()
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
    for k in range(1, m - 1, 1):
        exec = str(k)
    # for exec in executions[f_index]:
        # if exec == "optimized":
        #     continue
        if results[f_index][exec + "_" + workload] == "INVALID":
            print("WARN: no data found for " + exec + "_" + workload)
            continue
        labels.append(exec)
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
    ax.set_xlabel('K')
    ax.set_title(workload[:workload.find(".json")] + ", N is constant 9")
    ax.legend()
    plt.grid(True)

def cost_k(workload, availability_target):
    results, confs = get_results_k()
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
    for k in range(1, m - 1, 1):
        exec = str(k)
    # for exec in executions[f_index]:
        # if exec == "optimized":
        #     continue
        if results[f_index][exec + "_" + workload] == "INVALID":
            print("WARN: no data found for " + exec + "_" + workload)
            continue
        labels.append(exec)
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
    # plt.rcParams.update({'font.size': 38})
    width = 0.5  # the width of the bars: can also be len(x) sequence
    # fig, ax = plt.subplots()
    # fig = plt.figure()
    # ax = fig.add_axes([0.07, 0.09, .68, .88])

    plt.rcParams.update({'font.size': 38 * 1.6})
    fig = plt.figure()
    ax = fig.add_axes([0.1, 0.14, .54, .80])

    # ax.bar(labels, women_means, width, yerr=women_std, bottom=men_means, label='Women')

    ax.bar(labels, storage_cost, width, bottom=np.array(get_cost) + np.array(put_cost) + np.array(vm_cost),
           label='Storage cost', color='tab:red')
    ax.bar(labels, vm_cost, width, bottom=np.array(get_cost) + np.array(put_cost), label='VM cost', color='tab:green', fill=True, hatch='/')
    ax.bar(labels, put_cost, width, bottom=get_cost, label='PUT cost', color='tab:orange', fill=True, hatch='|')
    ax.bar(labels, get_cost, width, label='GET cost', color='tab:blue', fill=True, hatch='X')

    opt_index = 1
    ax.text(opt_index - 0.3, storage_cost[opt_index] + vm_cost[opt_index] + put_cost[opt_index] + get_cost[opt_index] + 0.01, "$K_{Opt}$")


    ax.set_ylabel('Cost ($/hour)')
    ax.set_xlabel('K')
    # ax.set_title(workload[:workload.find(".json")] + ", N k+2f")
    # ax.legend()

    # minor_ticks = np.arange(0, 600, 25)
    # ax.set_xticks(major_ticks)
    # ax.set_xticks(minor=True)

    ax.legend(bbox_to_anchor=(1, 1), loc='upper left')
    ax.grid(which='minor', alpha=0.5, axis='y')
    plt.grid(True, axis='y')


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

def plot_all(availability_target):
    for workload in workloads:
        # print("A")
        # interesting cases
        attrs = ["uniform", "1KB"] #attrs = ["dist_ST", "1KB"]# ["dist_SS", "1KB", "1TB", "_500_"] #, "HW"] #workload.find(list(client_dists.keys())[2])
        # attrs = ["10KB", "HR"]
        if workload_satisfies(workload, attrs):
            # plot_workload(workload, availability_target)
            # plot_normalized(workload, availability_target)

            # cost_const_k(workload, availability_target)
            cost_k(workload, availability_target)

    # os.system("subl drawer_output.txt")

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
    # plot_cumulative(workloads, 2)
    # plot_cumulative2(workloads, 1)
    # plot_cumulative3(workloads, 1)


    # plot_scatter_slo_latency(workloads, 1)
    # plot_scatter_slo_latency2(workloads, 2)
    # plot_sense_to_object_size(workloads, 1)
    # plot_sense_to_arrival_rate(workloads, 1)

    plot_all(1)

    # os.system("subl drawer_output.txt")
    sys.stdout.flush()
    plt.show()


