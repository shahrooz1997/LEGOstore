#!/usr/bin/python3

import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import json
import os, sys, time, shutil

availability_targets = [sys.argv[1]]

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

if __name__ == "__main__":

    # os.system('mkdir -p graphs')
    result_path = "../../data/Sense/"

    for f in availability_targets:
        y = []
        x = []
        x_ticks = []
        x_count = 0;
        for cd in client_dists:
            file_path = result_path + "f={0}/{1}/".format(f, cd)
            # shutil.rmtree(file_path + "graphs", ignore_errors=True)
            # os.mkdir(file_path + "graphs")
            for m in metrics:
                for rr in read_ratios:
                    file_name_baseline = "res_baseline_{1}_{2}_{3}.json".format(f, cd, rr, m)
                    file_name = "res_{1}_{2}_{3}.json".format(f, cd, rr, m)

                    print("plotting ", file_path + file_name, "...")
                    total_costs = []
                    network_costs = []
                    _data = json.load(open(file_path + file_name, "r"))["output"]
                    for ele in _data:
                        # get_cost = float(ele["get_cost"])
                        # put_cost = float(ele["put_cost"])
                        # network_costs.append(get_cost + put_cost)
                        total_costs.append(ele["total_cost"])

                    total_costs_baseline = []
                    _data = json.load(open(file_path + file_name_baseline, "r"))["output"]
                    for ele in _data:
                        # get_cost = float(ele["get_cost"])
                        # put_cost = float(ele["put_cost"])
                        # network_costs.append(get_cost + put_cost)
                        total_costs_baseline.append(ele["total_cost"])

                    y.append(sum(total_costs) / sum(total_costs_baseline) * 100)
                    x_ticks.append("{1}_{2}_{3}".format(f, cd, rr, m))
                    x.append(x_count)
                    x_count += 1


                    # x_axis = list(range(start, end, granularity))
                    # print(len(x_axis), len(total_costs))
                    # plt.scatter(x_axis, total_costs)
                    # plt.ylabel(m)
                    # print("saving ", file_path + file_name, "...")
                    # plt.savefig(file_path + "graphs/" +
                    #             file_name + ".png")
                    # plt.clf()
                    # print("cleared figure and move to next...")

        file_path_norm = result_path + "f={0}/".format(f)
        shutil.rmtree(file_path_norm + "graphs", ignore_errors=True)
        os.mkdir(file_path_norm + "graphs")
        fig = plt.figure(figsize=(20, 14))

        plt.xticks(x, x_ticks, rotation=90)
        plt.ylabel("Normalized percentage")
        for index, _ in enumerate(x):
            plt.text(index-.4, y[index], "{:.2f}".format(y[index]))

        plt.bar(x, y)
        # plt.show()
        # plt.savefig(file_path_norm + "graphs/" + "Normalized costs" + ".png")
        fig.tight_layout()
        fig.savefig(file_path_norm + "graphs/" + "Normalized costs" + ".png")
        plt.clf()

    # FILE_FORMAT = 'res_{}_{}_{}.json'
    # cli_distrib = ['uniform']
    # rw_ratio = ['HR', 'HW', 'RW']
    # properties = ['arrival_rate', 'object_count', 'object_size']
    #
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


















