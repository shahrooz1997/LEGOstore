import json
import sys
import matplotlib.pyplot as plt
import os
import time
from parameters import *

if __name__ == "__main__":

    # need path to create output file
    if len(sys.argv) != 2:
        print("USAGE: python3 generate_input.py <path>")
        exit(1)

    FILES_PATH = sys.argv[1]
    
    # initial settings for a group
    availability_target  = 2 
    client_dist          = [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]
    object_size          = 0.000001
    metadata_size        = 0.000000001
    num_objects          = 1000000
    arrival_rate         = 50
    read_ratio           = 0.1
    write_ratio          = 0.9
    SLO_read             = 1000
    SLO_write            = 1000
    duration             = 3600
    time_to_decode       = 0.00028


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
    FILE_DESCRIP = 'skewed_double_HW_arrival_rate.json'
    print(FILE_DESCRIP + "...START")
    ABS_PATH = FILES_PATH + FILE_DESCRIP
    delta = 50
    start = 200
    end   = 10000
    input_groups = vary_arrival_rate(_init_group,delta, start, end)
    json.dump({"input_groups": input_groups}, open(ABS_PATH,"w"), indent=4)
    # reset property
    _init_group.arrival_rate = 1000
    ############
    print(FILE_DESCRIP + "...DONE")
    ############
    # object size here is in bytes
    # conversion to GB is done in method
    ############
    FILE_DESCRIP = 'skewed_double_HW_object_size.json'
    print(FILE_DESCRIP + "...START")
    ABS_PATH = FILES_PATH + FILE_DESCRIP
    delta = 100 
    start = 1
    end   = 100000
    input_groups = vary_object_size(_init_group,delta, start, end)
    json.dump({"input_groups": input_groups}, open(ABS_PATH,"w"), indent=4)
    # reset property
    _init_group.object_size = object_size
    ############
    print(FILE_DESCRIP + "...DONE")
    ############
    FILE_DESCRIP = 'skewed_double_HW_object_count.json'
    print(FILE_DESCRIP + "...START")
    ABS_PATH = FILES_PATH + FILE_DESCRIP
    delta = 500 
    start = 500
    end   = 1000000
    input_groups = vary_object_count(_init_group,delta, start, end)
    json.dump({"input_groups": input_groups}, open(ABS_PATH,"w"), indent=4)
    # reset property
    _init_group.num_objects = num_objects
    ############
    print(FILE_DESCRIP + "...DONE")

    
 



