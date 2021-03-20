#!/usr/bin/python3

import json
import os, sys, time
from collections import OrderedDict
from workload_def import *

# availability_targets = [sys.argv[1]]

# availability_targets = [1]
# client_dists = OrderedDict([
#     ("uniform", [1/9 for _ in range(9)]),
#     #("skewed_single", [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025]),
#     #("skewed_double", [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]),
#     ("cheap_skewed", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
#     ("expensive_skewed", [0.025, 0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.025])
#     # ("Japan_skewed", [0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.0, 0.05, 0.025])
# ])
# object_sizes = [10 / 2**30, 1*2**10 / 2**30, 10*2**10 / 2**30, 100*2**10 / 2**30]
# arrival_rates = [200, 350, 500]
# read_ratios = OrderedDict([("HW", 0.1), ("RW", 0.5), ("HR", 0.9)])
# SLO_latencies = [500, 750, 1000]
#
# # Default values
# availability_target_default  = 2
# client_dist_default          = [1/9 for _ in range(9)]
# object_size_default          = 1*2**10 / 2**30 # in GB
# metadata_size_default        = 11 / 2**30 # in GB
# num_objects_default          = 1000000
# arrival_rate_default         = 350
# read_ratio_default           = 0.5
# write_ratio_default          = float("{:.2f}".format(1 - read_ratio_default))
# SLO_read_default             = 1000000
# SLO_write_default            = 1000000
# duration_default             = 3600
# time_to_decode_default       = 0.00028

# object_size_name = {10 / 2**30: "1B", 1*2**10 / 2**30: "1KB", 10*2**10 / 2**30: "10KB", 100*2**10 / 2**30: "100KB"}

# executions = ["optimized", "baseline_fixed_ABD", "baseline_fixed_CAS", "baseline_replication_based", "baseline_ec_based"]
# executions = ["baseline_fixed_ABD", "baseline_fixed_CAS", "baseline_replication_based", "baseline_ec_based"]

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

def extractor_type1():
    directory = "workloads"

    for f_index, f in enumerate(availability_targets):
        files_path = os.path.join(directory, "f=" + str(f))
        all_results = OrderedDict()
        for exec in executions[f_index]:
            for workload in workloads:
                res_file = os.path.join(files_path, "res_" + exec + "_" + workload)
                data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                for grp_index, grp in enumerate(data):
                    key = exec + "_" + workload  # list(client_dists.keys())[grp_index] + "_" + object_size_default + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default
                    value = "{:.4e}".format(grp["get_cost"]) + " " + "{:.4e}".format(grp["put_cost"]) + " " + \
                            "{:.4e}".format(grp["vm_cost"]) + " " + "{:.4e}".format(grp["storage_cost"])
                    all_results[key] = value

        keys = list(all_results.keys())
        # keys = [k for k in keys if k[0:8] != "baseline"]
        # print(keys)
        for exec in executions[f_index]:
            print(exec)
            for workload in workloads:
                print(workload, all_results[exec + "_" + workload])









    # directory = "workloads"
    # for f in availability_targets:
    #     files_path = os.path.join(directory, "f=" + str(f))
    #     baseline_started = False
    #     for res in results:
    #         if (res != "" and not baseline_started):
    #             print("BASELINES")
    #             baseline_started = True
    #
    #         if (res != ""):
    #             print(res[0:-1])
    #
    #         # client_dists testcases
    #         print("client_dists")
    #         res_file = os.path.join(files_path, "res_" + res + "client_dists.json")
    #         data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
    #         for grp_index, grp in enumerate(data):
    #             if "k" not in grp:
    #                 grp["k"] = 0
    #             # count_the_configuration_of(grp)
    #             print(list(client_dists.keys())[
    #                       grp_index] + "_" + object_size_default + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default,
    #                   end=" ")
    #             # print("{}_{}_{}".format(cd, rr, m), end=" ")
    #             print(grp["m"], grp["k"], list_reformat(grp["selected_dcs"]), "{:.2f}".format(grp["get_latency"]), \
    #                   "{:.2f}".format(grp["put_latency"]), "{:.4e}".format(grp["get_cost"]),
    #                   "{:.4e}".format(grp["put_cost"]), \
    #                   "{:.4e}".format(grp["vm_cost"]), "{:.4e}".format(grp["storage_cost"]), quorum_sizes(grp))
    #         print()
    #
    #         # object_sizes testcases
    #         print("object_sizes")
    #         res_file = os.path.join(files_path, "res_" + res + "object_sizes.json")
    #         data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
    #         for grp_index, grp in enumerate(data):
    #             if "k" not in grp:
    #                 grp["k"] = 0
    #             # count_the_configuration_of(grp)
    #             print(client_dist_default + "_" + list(object_sizes.keys())[
    #                 grp_index] + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default,
    #                   end=" ")
    #             # print("{}_{}_{}".format(cd, rr, m), end=" ")
    #             print(grp["m"], grp["k"], list_reformat(grp["selected_dcs"]), "{:.2f}".format(grp["get_latency"]), \
    #                   "{:.2f}".format(grp["put_latency"]), "{:.4e}".format(grp["get_cost"]),
    #                   "{:.4e}".format(grp["put_cost"]), \
    #                   "{:.4e}".format(grp["vm_cost"]), "{:.4e}".format(grp["storage_cost"]), quorum_sizes(grp))
    #         print()
    #
    #         # arrival_rates testcases
    #         print("arrival_rates")
    #         res_file = os.path.join(files_path, "res_" + res + "arrival_rates.json")
    #         data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
    #         for grp_index, grp in enumerate(data):
    #             if "k" not in grp:
    #                 grp["k"] = 0
    #             # count_the_configuration_of(grp)
    #             print(client_dist_default + "_" + object_size_default + "_" + str(
    #                 arrival_rates[grp_index]) + "_" + read_ratio_default + "_" + SLO_read_default,
    #                   end=" ")
    #             # print("{}_{}_{}".format(cd, rr, m), end=" ")
    #             print(grp["m"], grp["k"], list_reformat(grp["selected_dcs"]), "{:.2f}".format(grp["get_latency"]), \
    #                   "{:.2f}".format(grp["put_latency"]), "{:.4e}".format(grp["get_cost"]),
    #                   "{:.4e}".format(grp["put_cost"]), \
    #                   "{:.4e}".format(grp["vm_cost"]), "{:.4e}".format(grp["storage_cost"]), quorum_sizes(grp))
    #         print()
    #
    #         # read_ratios testcases
    #         print("read_ratios")
    #         res_file = os.path.join(files_path, "res_" + res + "read_ratios.json")
    #         data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
    #         for grp_index, grp in enumerate(data):
    #             if "k" not in grp:
    #                 grp["k"] = 0
    #             # count_the_configuration_of(grp)
    #             print(client_dist_default + "_" + object_size_default + "_" + arrival_rate_default + "_" +
    #                   list(read_ratios.keys())[grp_index] + "_" + SLO_read_default,
    #                   end=" ")
    #             # print("{}_{}_{}".format(cd, rr, m), end=" ")
    #             print(grp["m"], grp["k"], list_reformat(grp["selected_dcs"]), "{:.2f}".format(grp["get_latency"]), \
    #                   "{:.2f}".format(grp["put_latency"]), "{:.4e}".format(grp["get_cost"]),
    #                   "{:.4e}".format(grp["put_cost"]), \
    #                   "{:.4e}".format(grp["vm_cost"]), "{:.4e}".format(grp["storage_cost"]), quorum_sizes(grp))
    #         print()
    #
    #         # SLO_reads testcases
    #         print("SLO_latencies")
    #         res_file = os.path.join(files_path, "res_" + res + "latencies.json")
    #         data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
    #         for grp_index, grp in enumerate(data):
    #             if "k" not in grp:
    #                 grp["k"] = 0
    #             # count_the_configuration_of(grp)
    #             print(client_dist_default + "_" + object_size_default + "_" + arrival_rate_default + "_" +
    #                   read_ratio_default + "_" + str(SLO_reads[grp_index]),
    #                   end=" ")
    #             # print("{}_{}_{}".format(cd, rr, m), end=" ")
    #             print(grp["m"], grp["k"], list_reformat(grp["selected_dcs"]), "{:.2f}".format(grp["get_latency"]), \
    #                   "{:.2f}".format(grp["put_latency"]), "{:.4e}".format(grp["get_cost"]),
    #                   "{:.4e}".format(grp["put_cost"]), \
    #                   "{:.4e}".format(grp["vm_cost"]), "{:.4e}".format(grp["storage_cost"]), quorum_sizes(grp))
    #         print()

def extractor_type2():
    directory = "workloads"

    for f_index, f in enumerate(availability_targets):
        files_path = os.path.join(directory, "f=" + str(f))
        all_results = OrderedDict()
        for exec in executions[f_index]:
            for workload in workloads:
                res_file = os.path.join(files_path, "res_" + exec + "_" + workload)
                data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                for grp_index, grp in enumerate(data):
                    key = exec + "_" + workload # list(client_dists.keys())[grp_index] + "_" + object_size_default + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default
                    value = "{:.4e}".format(grp["get_cost"]) + " " + "{:.4e}".format(grp["put_cost"]) + " " + \
                            "{:.4e}".format(grp["vm_cost"]) + " " + "{:.4e}".format(grp["storage_cost"])
                    all_results[key] = value

        keys = list(all_results.keys())
        # keys = [k for k in keys if k[0:8] != "baseline"]
        # print(keys)
        for exec in executions[f_index]:
            print(exec)
            for workload in workloads:
                print(workload, all_results[exec + "_" + workload])

def extractor_type3():
    directory = "workloads"

    for f_index, f in enumerate(availability_targets):
        files_path = os.path.join(directory, "f=" + str(f))
        all_results = OrderedDict()
        for exec in executions[f_index]:
            for workload in workloads:
                res_file = os.path.join(files_path, "res_" + exec + "_" + workload)
                data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
                for grp_index, grp in enumerate(data):
                    key = exec + "_" + workload # list(client_dists.keys())[grp_index] + "_" + object_size_default + "_" + arrival_rate_default + "_" + read_ratio_default + "_" + SLO_read_default
                    value = "{:.4e}".format(grp["get_cost"]) + " " + "{:.4e}".format(grp["put_cost"]) + " " + \
                            "{:.4e}".format(grp["vm_cost"]) + " " + "{:.4e}".format(grp["storage_cost"])
                    all_results[key] = value

        keys = list(all_results.keys())
        # keys = [k for k in keys if k[0:8] != "baseline"]
        # print(keys)
        for workload in workloads:
            print(workload[:workload.find(".json")])
            print(" get_cost put_cost vm_cost storage_cost")
            for exec in executions[f_index]:
                print(exec, all_results[exec + "_" + workload])
            print("\n")




if __name__ == "__main__":
    global workloads
    workloads = get_workloads()

    # extractor_type2()
    extractor_type3()


