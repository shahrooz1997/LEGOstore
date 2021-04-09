#!/usr/bin/python3

import os
import json
from collections import OrderedDict

confs = ['8|6', '9|7', '7|5', '6|4', '4|2', '3|0', '5|3']

workloads = ["RESULTS/workloads_1000ms/f=1/dist_O_10KB_1TB_500_HW_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_T_10KB_1TB_500_HW_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_LA_10KB_1TB_500_HW_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_ST_10KB_1TB_500_HW_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_SS_10KB_1TB_500_HW_1000.json",
"RESULTS/workloads_1000ms/f=1/uniform_10KB_1TB_500_HW_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_LO_10KB_1TB_500_HW_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_SY_10KB_1TB_500_HW_1000.json",

"RESULTS/workloads_1000ms/f=1/dist_O_10KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_T_10KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_LA_10KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_ST_10KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_SS_10KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/uniform_10KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_LO_10KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_SY_10KB_1TB_500_HR_1000.json",

"RESULTS/workloads_1000ms/f=1/dist_O_1KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_T_1KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_LA_1KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_ST_1KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_SS_1KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/uniform_1KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_LO_1KB_1TB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_SY_1KB_1TB_500_HR_1000.json",

"RESULTS/workloads_1000ms/f=1/dist_O_10KB_100GB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_T_10KB_100GB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_LA_10KB_100GB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_ST_10KB_100GB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_SS_10KB_100GB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/uniform_10KB_100GB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_LO_10KB_100GB_500_HR_1000.json",
"RESULTS/workloads_1000ms/f=1/dist_SY_10KB_100GB_500_HR_1000.json",
]

def print_configurations_count():
    # for f_index, f in enumerate(availability_targets):
    files_path = "cost_range/"
    min_total_cost = 10000000
    min_conf = ""
    max_total_cost = 0
    max_conf = ""
    # for exec in ["optimized"]: # ["baseline_abd", "baseline_cas"]:
    configuration_count = {}
    configuration_cost = {}
    for i, conf in enumerate(confs):
        res_file = os.path.join(files_path, str(i) + ".json")
        data = json.load(open(res_file, "r"), object_pairs_hook=OrderedDict)["output"]
        for grp_index, grp in enumerate(data):
            # if grp["protocol"] != "abd":
            #     configuration = str(grp["m"]) + "|" + str(grp["k"]) + "|" + list_reformat(grp["selected_dcs"]) + \
            #                     "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
            #                     len(grp["client_placement_info"]["0"]["Q2"])) + "|" + str(len(grp["client_placement_info"]["0"]["Q3"])) + "|" + str(
            #                     len(grp["client_placement_info"]["0"]["Q4"]))
            # else:
            #     configuration = str(grp["m"]) + "|" + "0" + "|" + list_reformat(grp["selected_dcs"]) + "|" + str(len(grp["client_placement_info"]["0"]["Q1"])) + "|" + str(
            #     len(grp["client_placement_info"]["0"]["Q2"]))

            total_cost = float("{:.4f}".format(grp["total_cost"]))
            if min_total_cost > total_cost:
                min_total_cost = total_cost
                min_conf = conf
            if max_total_cost < total_cost:
                max_total_cost = total_cost
                max_conf = conf

            # print(configuration)

            # if configuration in configuration_count:
            #     configuration_count[configuration] += 1
            # else:
            #     configuration_count[configuration] = 1
            #     configuration_cost[configuration] = [
            #         grp["client_placement_info"]["0"]["get_cost_on_this_client"],
            #         grp["client_placement_info"]["0"]["put_cost_on_this_client"]]

    # print("For f=" + str(f) + " and " + exec + ":")
    # print("for " + str(len(workloads)) + ", we have " + str(len(configuration_count.keys())) + " different confs")
    print("min", min_conf, min_total_cost)
    print("max", max_conf, max_total_cost)
    print("range:", max_total_cost - min_total_cost)

    # configurations = OrderedDict()
    # for config in configuration_count:
    #     strs = config.split("|")
    #     key = strs[0] + "|" + strs[1]
    #     if key in configurations:
    #         configurations[key] += configuration_count[config]
    #     else:
    #         configurations[key] = configuration_count[config]
    #
    # configurations = OrderedDict(sorted(configurations.items(), key=lambda x: x[1], reverse=True))
    # print(configurations)

def main():
    for workload in workloads:
        print(workload)
        for i, conf in enumerate(confs):
            m = int(conf[0])
            k = int(conf[2])
            command = "python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + workload + " -o cost_range/" + str(i) + ".json -v -H min_cost -b -t "
            if k == 0:
                command += "abd -m " + str(m)
            else:
                command += "cas -m " + str(m) + " -k " + str(k)
            # print(command)
            os.system(command)

        print_configurations_count()




if __name__ == '__main__':
    main()
