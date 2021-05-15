#!/usr/bin/python3

import json
import sys
import matplotlib.pyplot as plt
import os
import time


# Default values
availability_target_default  = 2
client_dist_default          = [1/9 for _ in range(9)]
object_size_default          = 1*2**10 / 2**30 # in GB
metadata_size_default        = 100 / 2**30 # in GB
num_objects_default          = 10000000
arrival_rate_default         = 350
read_ratio_default           = 0.5
write_ratio_default          = float("{:.2f}".format(1 - read_ratio_default))
SLO_read_default             = 1000000
SLO_write_default            = 1000000
duration_default             = 3600
time_to_decode_default       = 0.00028

class Single_group:
    def __init__(self,
        availability_target,
        client_dist,
        object_size,
        metadata_size,
        num_objects,
        arrival_rate,
        read_ratio,
        write_ratio,
        SLO_read,
        SLO_write,
        duration,
        time_to_decode):


        self.availability_target  = availability_target
        self.client_dist          = client_dist
        self.object_size          = object_size
        self.metadata_size        = metadata_size
        self.num_objects          = num_objects
        self.arrival_rate         = arrival_rate
        self.read_ratio           = read_ratio
        self.write_ratio          = write_ratio
        self.SLO_read             = SLO_read
        self.SLO_write            = SLO_write
        self.duration             = duration
        self.time_to_decode       = time_to_decode

if __name__ == "__main__":

    # need path to create output file
    # if len(sys.argv) < 2:
    #     print("USAGE: python3 generate_input.py <path>")
    #     exit(1)

    # directory = sys.argv[1]
    directory = "workloads/"
    os.system("rm -rf " + directory + "*")
    # os.system("cp workload_def.py " + directory)
    # for f_index, f in enumerate(availability_targets):
    #     os.mkdir(directory + "f=" + str(f))
    #     files_path = directory + "f=" + str(f) + "/"
    files_path_cold = directory + "cold/"
    files_path_hot = directory + "hot/"
    os.mkdir(files_path_cold)
    os.mkdir(files_path_hot)

    # attrs
    f = 1
    client_dist = [1/9 for _ in range(9)]
    object_size = 1*2**10 / 2**30
    storage_size = 1024
    arrival_rate = 200
    read_ratio = 0.5
    lat = 1000

    num_objects = storage_size / object_size

    for cold_ratio in range(0, 101, 1):
        cold_ratio_p = cold_ratio / 100.
        input_groups = []
        key_group = Single_group(f, client_dist, object_size, metadata_size_default,
                                 int(num_objects * float("{:.2f}".format(1 - cold_ratio_p))), arrival_rate, read_ratio,
                                 float("{:.2f}".format(1 - read_ratio)), lat, lat,
                                 duration_default, time_to_decode_default)
        input_groups.append(key_group.__dict__)
        FILE_NAME = "workload_" + str(cold_ratio) + ".json"
        json.dump({"input_groups": input_groups}, open(files_path_hot + FILE_NAME, "w"), indent=4)


        input_groups = []
        key_group = Single_group(f, client_dist, object_size, metadata_size_default,
                                 int(num_objects * cold_ratio_p), 0, read_ratio,
                                 float("{:.2f}".format(1 - read_ratio)), lat, lat,
                                 duration_default, time_to_decode_default)
        input_groups.append(key_group.__dict__)
        FILE_NAME = "workload_" + str(cold_ratio) + ".json"
        json.dump({"input_groups": input_groups}, open(files_path_cold + FILE_NAME, "w"), indent=4)


        #
        #
        #
        # # client_dists testcases
        # input_groups = []
        # for cd in client_dists:
        #     key_group = Single_group(f, client_dists[cd], object_size_default, metadata_size_default, num_objects_default,
        #                              arrival_rate_default, read_ratio_default, write_ratio_default, SLO_read_default,
        #                              SLO_write_default, duration_default, time_to_decode_default)
        #     input_groups.append(key_group.__dict__)
        # FILE_NAME = "client_dists.json"
        # json.dump({"input_groups": input_groups}, open(files_path + FILE_NAME, "w"), indent=4)
        #
        # # object_sizes testcases
        # input_groups = []
        # for object_size in object_sizes:
        #     key_group = Single_group(f, client_dist_default, object_size, metadata_size_default,
        #                              num_objects_default,
        #                              arrival_rate_default, read_ratio_default, write_ratio_default, SLO_read_default,
        #                              SLO_write_default, duration_default, time_to_decode_default)
        #     input_groups.append(key_group.__dict__)
        # FILE_NAME = "object_sizes.json"
        # json.dump({"input_groups": input_groups}, open(files_path + FILE_NAME, "w"), indent=4)
        #
        # # arrival_rates testcases
        # input_groups = []
        # for ar in arrival_rates:
        #     key_group = Single_group(f, client_dist_default, object_size_default, metadata_size_default,
        #                              num_objects_default,
        #                              ar, read_ratio_default, write_ratio_default, SLO_read_default,
        #                              SLO_write_default, duration_default, time_to_decode_default)
        #     input_groups.append(key_group.__dict__)
        # FILE_NAME = "arrival_rates.json"
        # json.dump({"input_groups": input_groups}, open(files_path + FILE_NAME, "w"), indent=4)
        #
        # # read_ratios testcases
        # input_groups = []
        # for rr in read_ratios:
        #     key_group = Single_group(f, client_dist_default, object_size_default, metadata_size_default,
        #                              num_objects_default,
        #                              arrival_rate_default, read_ratios[rr], float("{:.2f}".format(1 - read_ratios[rr])), SLO_read_default,
        #                              SLO_write_default, duration_default, time_to_decode_default)
        #     input_groups.append(key_group.__dict__)
        # FILE_NAME = "read_ratios.json"
        # json.dump({"input_groups": input_groups}, open(files_path + FILE_NAME, "w"), indent=4)
        #
        # # SLO_reads testcases
        # input_groups = []
        # for lat in SLO_reads:
        #     key_group = Single_group(f, client_dist_default, object_size_default, metadata_size_default,
        #                              num_objects_default,
        #                              arrival_rate_default, read_ratio_default, write_ratio_default, lat,
        #                              lat, duration_default, time_to_decode_default)
        #     input_groups.append(key_group.__dict__)
        # FILE_NAME = "latencies.json"
        # json.dump({"input_groups": input_groups}, open(files_path + FILE_NAME, "w"), indent=4)

