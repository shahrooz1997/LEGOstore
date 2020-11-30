#!/usr/bin/python3

import json
import os, sys, time

availability_targets = [2]

client_dists = {
    "uniform": [1/9 for _ in range(9)],
    #"skewed_single": [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025],
    #"skewed_double": [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]
    "cheap_skewed": [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9],
    "expensive_skewed": [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.9, 0.025, 0.025],
    "Japan_skewed": [0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.0, 0.05, 0.025]
}

read_ratios = {"HW": 0.1, "RW": 0.5, "HR": 0.9}

metrics = ["arrival_rate", "object_size", "object_count"]


def list_reformat(l):
    ret = ""
    for e in l:
        ret += str(e) + ","

    return ret[0:len(ret) - 1]

def quorum_sizes(grp):
    ret = ""
    ret += str(len(grp["client_placement_info"]["0"]["Q1"])) + ","
    ret += str(len(grp["client_placement_info"]["0"]["Q2"])) + ","
    if "Q3" in grp["client_placement_info"]["0"]:
        ret += str(len(grp["client_placement_info"]["0"]["Q3"])) + ","
        ret += str(len(grp["client_placement_info"]["0"]["Q4"])) + ","

    return ret[0:len(ret) - 1]

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

if __name__ == "__main__":


    # file_name = "/home/shahrooz/Desktop/PSU/Research/data/Sense/f=2/uniform/res_uniform_HR_arrival_rate.json" # = 'res_{}_{}_{}.json'
    # result_path = "../../data/Sense/"

    if len(sys.argv) == 2:
        result_path = "Sense/"
    else:
        result_path = "../../data/Sense/"

    for f in availability_targets:
        for m in metrics:
            for cd in client_dists:
                for rr in read_ratios:
                    if len(sys.argv) == 2:
                        file_name = result_path + "f={0}/{1}/res_baseline_{1}_{2}_{3}.json".format(f, cd, rr, m)
                    else:
                        file_name = result_path + "f={0}/{1}/res_{1}_{2}_{3}.json".format(f, cd, rr, m)
                    # print(file_name)

                    data = json.load(open(file_name, "r"))["output"]
                    for grp in data:
                        if "k" not in grp:
                            grp["k"] = 0

                        count_the_configuration_of(grp)

                        print("{}_{}_{}".format(cd, rr, m), end=" ")
                        print(grp["m"], grp["k"], list_reformat(grp["selected_dcs"]), "{:.2f}".format(grp["get_latency"]), \
                              "{:.2f}".format(grp["put_latency"]), "{:.4e}".format(grp["get_cost"]),"{:.4e}".format(grp["put_cost"]), \
                              "{:.4e}".format(grp["vm_cost"]), "{:.4e}".format(grp["storage_cost"]), quorum_sizes(grp))
                    print()
                print()

        print_configurations(configuration_count, configuration_cost)
        configuration_count = {}
        configuration_cost = {}
    # for d in cli_distrib:
    #     for rw in rw_ratio:
    #         for p in properties:
    #             print("plotting ", FILE_FORMAT.format(d, rw, p), "...")
    #             total_costs = []
    #             network_costs = []
    #             _data = json.load(open(FILE_FORMAT.format(d, rw, p), "r"))["output"]
    #             for ele in _data:
    #                 get_cost = float(ele["get_cost"])
    #                 put_cost = float(ele["put_cost"])
    #                 network_costs.append(get_cost + put_cost)
    #                 total_costs.append(ele["total_cost"])
    #
    #             if p == "arrival_rate":
    #                 granularity = 50
    #                 start = 200
    #                 end = 1000
    #             elif p == "object_count":
    #                 granularity = 10000
    #                 start = 1
    #                 end = 1000000
    #             else:
    #                 granularity = 1000
    #                 start = 1
    #                 end = 100000
    #
    #             x_axis = list(range(start, end, granularity))
    #             print(len(x_axis), len(total_costs))
    #             plt.scatter(x_axis, total_costs)
    #             plt.ylabel(p)
    #             print("saving ", FILE_FORMAT.format(d, rw, p), "...")
    #             plt.savefig("graphs/" +
    #                         FILE_FORMAT.format(d, rw, p) + ".png")
    #             plt.clf()
    #             print("cleared figure and move to next...")

