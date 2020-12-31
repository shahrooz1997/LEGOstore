#!/usr/bin/python3

import os, sys, time, json

# availability_targets = [0, 1, 2]

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

    start_time = time.time()

    # directory = "Sense/"
    directory = "../../data/Sense/"

    for f in availability_targets:
        availability_target = f
        FILES_PATH = directory
        FILES_PATH = FILES_PATH + "f=" + str(f) + "/"

        for cd in client_dists:
            client_dist = client_dists[cd]
            FILES_PATH_DIST = FILES_PATH + cd + "/"

            for rr in read_ratios:
                FILE_NAME_TEMP = cd + "_" + rr + "_"
                read_ratio = read_ratios[rr];
                write_ratio = 1 - read_ratio;

                for metric in metrics:
                    FILE_NAME = FILES_PATH_DIST + FILE_NAME_TEMP + metric + ".json"
                    if len(sys.argv) == 3: # Baseline
                        RES_FILE_NAME = FILES_PATH_DIST + "res_baseline_" + FILE_NAME_TEMP + metric + ".json"
                        COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i " + FILE_NAME + " -o " + RES_FILE_NAME + " -H min_cost -b -t abd -v"
                    else:
                        RES_FILE_NAME = FILES_PATH_DIST + "res_" + FILE_NAME_TEMP + metric + ".json"
                        COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i " + FILE_NAME + " -o " + RES_FILE_NAME + " -H min_cost -v"
                    print("running", FILE_NAME, "...")
                    os.system(COMMAND)
                    print("finishehd", FILE_NAME, "...")
                    print("duration:", time.time() - start_time)
                    start_time = time.time()
    
    # COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i Sense/uniform/{} -o Sense/uniform/res_{} -H min_cost -v"
    # files = [
    #     'uniform_HR_arrival_rate.json',
    #     'uniform_HR_object_count.json',
    #     'uniform_HR_object_size.json',
    #     'uniform_HW_arrival_rate.json',
    #     'uniform_HW_object_count.json',
    #     'uniform_HW_object_size.json',
    #     'uniform_RW_arrival_rate.json',
    #     'uniform_RW_object_count.json',
    #     'uniform_RW_object_size.json'
    # ]
    #
    # for filename in files:
    #     print("running", filename, "...")
    #     os.system(COMMAND.format(filename, filename))
    #     print("finishehd", filename, "...")
    #
    # print("duration:", time.time() - start_time)
    # start_time = time.time()
    #
    #
    # ###############
    # COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i Sense/skewed_single_source/{} -o Sense/skewed_single_source/res_{} -H min_cost"
    # files = [
    #     'skewed_single_HR_arrival_rate.json',
    #     'skewed_single_HR_object_count.json',
    #     'skewed_single_HR_object_size.json',
    #     'skewed_single_HW_arrival_rate.json',
    #     'skewed_single_HW_object_count.json',
    #     'skewed_single_HW_object_size.json',
    #     'skewed_single_RW_arrival_rate.json',
    #     'skewed_single_RW_object_count.json',
    #     'skewed_single_RW_object_size.json'
    # ]
    #
    # for filename in files:
    #     print("running", filename, "...")
    #     os.system(COMMAND.format(filename, filename))
    #     print("finishehd", filename, "...")
    #
    #
    #
    # print("duration:", time.time() - start_time)
    # start_time = time.time()
    #
    # ##########################
    # COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i Sense/skewed_double_source/{} -o Sense/skewed_double_source/res_{} -H min_cost"
    #
    # files = [
    #     'skewed_double_HR_arrival_rate.json',
    #     'skewed_double_HR_object_count.json',
    #     'skewed_double_HR_object_size.json',
    #     'skewed_double_HW_arrival_rate.json',
    #     'skewed_double_HW_object_count.json',
    #     'skewed_double_HW_object_size.json',
    #     'skewed_double_RW_arrival_rate.json',
    #     'skewed_double_RW_object_count.json',
    #     'skewed_double_RW_object_size.json'
    # ]
    #
    # for filename in files:
    #     print("running", filename, "...")
    #     os.system(COMMAND.format(filename, filename))
    #     print("finishehd", filename, "...")
    #
    #
    # print("duration:", time.time() - start_time)
