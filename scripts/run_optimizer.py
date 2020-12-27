#!/usr/bin/python3

import os
import json

class Input_groups:
    class Single_group:
        def __init__(self, **kwargs):
            self.availability_target = None
            self.client_dist = []
            self.object_size = None
            self.metadata_size = None
            self.num_objects = None
            self.arrival_rate = None
            self.read_ratio = None
            self.write_ratio = None
            self.SLO_read = None
            self.SLO_write = None
            self.duration = None
            self.time_to_decode = None

    def __init__(self, confid):
        self.input_groups = []
        self.confid = confid

    def add_group(self, json_dict):
        group = Input_groups.Single_group()
        for key, val in json_dict.items():
            group.__dict__[key] = val
        del group.keys
        self.input_groups.append(group.__dict__)

    def dump(self):
        FILE_NAME = "optimizer_input_" + str(self.confid) + ".json"
        json.dump({"input_groups": self.input_groups}, open("./config/auto_test/" + FILE_NAME, "w"), indent=4)


def convert_workload_to_input():
    with open("./config/auto_test/input_workload.json", "r") as f:
        workload = json.load(f)
    group_configs = workload["workload_config"]
    global confs
    confs = []
    for gc in group_configs:
        input_groups = Input_groups(gc["id"])
        for g in gc["grp_workload"]:
            input_groups.add_group(g)
        confs.append(input_groups)
        input_groups.dump()
    
def run_optimizer(file_name):
    result_file_name = "res_" + file_name
    print("running", result_file_name, "...")
    os.system("python3 ./optimizer/placement.py -f ./optimizer/tests/inputtests/dc_gcp.json -i " + os.path.join(
        "./config/auto_test", file_name) + " -o " + os.path.join("./config/auto_test", result_file_name) +
              " -H min_cost -v")
    # print("python3 ./optimizer/placement.py -f ./optimizer/tests/inputtests/dc_gcp.json -i " + os.path.join(
    #     "./config/auto_test", file_name) + " -o " + os.path.join("./config/auto_test", result_file_name) +
    #       " -H min_cost -v")
    print("finishehd", result_file_name)
        
def main():
    convert_workload_to_input()
    for conf in confs:
        run_optimizer("optimizer_input_" + str(conf.confid) + ".json")

if __name__ == "__main__":
    main()

