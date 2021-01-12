import json
import sys
import matplotlib.pyplot as plt
import os
import time
from parameters import *

availability_targets = [0, 1, 2]

client_dists = {
    "uniform": [1/9 for _ in range(9)],
    #"skewed_single": [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025],
    #"skewed_double": [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]
    "cheap_skewed": [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9],
    "expensive_skewed": [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.9, 0.025, 0.025],
    "Japan_skewed": [0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.0, 0.05, 0.025]
}

read_ratios = {"HW": 0.1, "RW": 0.5, "HR": 0.9}

# Default values
availability_target  = 2
client_dist          = [1/9 for _ in range(9)]
object_size          = 0.0001 # in GB
metadata_size        = 0.000000001 # in GB
num_objects          = 1000000
arrival_rate         = 200
read_ratio           = 0.1
write_ratio          = 0.9
SLO_read             = 1000
SLO_write            = 1000
duration             = 3600
time_to_decode       = 0.00028


if __name__ == "__main__":

    # need path to create output file
    if len(sys.argv) != 2:
        print("USAGE: python3 generate_input.py <path>")
        exit(1)

    directory = sys.argv[1]

    for f in availability_targets:
        availability_target = f
        FILES_PATH = directory
        os.mkdir(FILES_PATH + "f=" + str(f))
        FILES_PATH = FILES_PATH + "f=" + str(f) + "/"

        for cd in client_dists:
            client_dist = client_dists[cd]
            os.mkdir(FILES_PATH + cd)
            FILES_PATH_DIST = FILES_PATH + cd + "/"

            for rr in read_ratios:
                FILE_NAME_TEMP = cd + "_" + rr + "_"
                read_ratio = read_ratios[rr];
                write_ratio = 1 - read_ratio;

                _init_group = single_group(
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
                    time_to_decode)

                ############
                FILE_NAME = FILES_PATH_DIST + FILE_NAME_TEMP + "arrival_rate.json"
                delta = 50
                start = 200
                end   = 1000
                input_groups = vary_arrival_rate(_init_group,delta, start, end)
                json.dump({"input_groups": input_groups}, open(FILE_NAME,"w"), indent=4)
                # reset property
                _init_group.arrival_rate = arrival_rate
                print(FILE_NAME + "...DONE")
                ############

                ############
                # object size here is in bytes
                # conversion to GB is done in method
                ############
                FILE_NAME = FILES_PATH_DIST + FILE_NAME_TEMP + "object_size.json"
                delta = 1000
                start = 1
                end   = 100000
                input_groups = vary_object_size(_init_group,delta, start, end)
                json.dump({"input_groups": input_groups}, open(FILE_NAME,"w"), indent=4)
                # reset property
                _init_group.object_size = object_size
                print(FILE_NAME + "...DONE")
                ############


                ############
                FILE_NAME = FILES_PATH_DIST + FILE_NAME_TEMP + "object_count.json"
                delta = 10000
                start = 1
                end   = 1000000
                input_groups = vary_object_count(_init_group,delta, start, end)
                json.dump({"input_groups": input_groups}, open(FILE_NAME,"w"), indent=4)
                # reset property
                _init_group.num_objects = num_objects
                print(FILE_NAME + "...DONE")
                ############

