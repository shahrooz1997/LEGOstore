#!/usr/bin/python3

import json
import os, sys, time
from collections import OrderedDict

availability_targets = [1, 2]
client_dists = OrderedDict([
    ("uniform", [1/9 for _ in range(9)]),
    #("skewed_single", [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025]),
    #("skewed_double", [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]),
    ("cheap_skewed", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
    ("expensive_skewed", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.9, 0.025, 0.025])
    # ("Japan_skewed", [0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.0, 0.05, 0.025])
])
object_sizes = OrderedDict([
    ("10B", 10 / 2**30),
    ("1KB", 1*2**10 / 2**30),
    ("10KB", 10*2**10 / 2**30),
    ("100KB", 100*2**10 / 2**30)
])
arrival_rates = [200, 350, 500]
read_ratios = OrderedDict([("HW", 0.1), ("RW", 0.5), ("HR", 0.9)])
SLO_reads = [750, 1000]

results = ["optimized", "baseline_REP", "baseline_ABD"]



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

# Default values
# availability_target_default  = 2
client_dist_default          = "uniform" #[1/9 for _ in range(9)]
object_size_default          = "10KB"#1*2**10 / 2**30 # in GB
# metadata_size_default        = 1 / 2**30 # in GB
# num_objects_default          = 1000000
arrival_rate_default         = "200"
read_ratio_default           = "RW"
# write_ratio_default          = float("{:.2f}".format(1 - read_ratio_default))
SLO_read_default             = "1000"
# SLO_write_default            = 1000
# duration_default             = 3600
# time_to_decode_default       = 0.00028

def extractor_type1():
    directory = "."
    files_path = directory
    for res in results:
        print(res)

        res_file = os.path.join(files_path, res + ".json")
        data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
        for grp_index, grp in enumerate(data):
            if "k" not in grp:
                grp["k"] = 0
            # print(list(client_dists.keys())[
            #           grp_index] + "_" + object_size_default + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default,
            #       end=" ")
            # print("{}_{}_{}".format(cd, rr, m), end=" ")
            print(grp["m"], grp["k"], list_reformat(grp["selected_dcs"]), "{:.2f}".format(grp["get_latency"]), \
                  "{:.2f}".format(grp["put_latency"]), "{:.4e}".format(grp["get_cost"]),
                  "{:.4e}".format(grp["put_cost"]), \
                  "{:.4e}".format(grp["vm_cost"]), "{:.4e}".format(grp["storage_cost"]), quorum_sizes(grp))
        print()


def extractor_type2():
    directory = "."

    files_path = directory
    all_results = OrderedDict()
    for res in results:
        # print(res)

        res_file = os.path.join(files_path, res + ".json")
        data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]

        for grp_index, grp in enumerate(data):
            # key = list(client_dists.keys())[grp_index] + "_" + object_size_default + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default
            value = "{:.4e}".format(grp["get_cost"]) + " " + "{:.4e}".format(grp["put_cost"]) + " " + \
                    "{:.4e}".format(grp["vm_cost"]) + " " + "{:.4e}".format(grp["storage_cost"])
            print(res, value)
            # all_results[res + key] = value

    # print(all_results)

        # keys = list(all_results.keys())
        # keys = [k for k in keys if k[0:8] != "baseline"]
        # print(keys)
        #
        # for key in keys:
        #     print(key)
        #     print(" get_cost put_cost vm_cost storage_cost")
        #     for res in results:
        #         if (res == ""):
        #             res = "optimized"
        #         else:
        #             res = res[0:-1]
        #         print(res, all_results[res + "_" + key if res != "optimized" else key])
        #     print()



if __name__ == "__main__":

    extractor_type2()


