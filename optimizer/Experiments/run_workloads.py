#!/usr/bin/python3

import os, sys, time, json
from collections import OrderedDict

availability_targets = [1, 2]
baselines = [OrderedDict([("ABD", "-b -t abd"), ("CAS", "-b -t cas"), ("fixed_CAS", "-b -t cas -m 4 -k 2")]),
             OrderedDict([("ABD", "-b -t abd"), ("CAS", "-b -t cas"), ("fixed_CAS", "-b -t cas -m 6 -k 2")])]

run_baseline = True

if __name__ == "__main__":

    start_time = time.time()

    directory = "workloads"
    for f_index, f in enumerate(availability_targets):
        files_path = os.path.join(directory, "f=" + str(f))

        workload_files = [f for f in os.listdir(files_path) if os.path.isfile(os.path.join(files_path, f)) and f[0:4] != "res_"]
        print(workload_files)

        for file_name in workload_files:
            if not run_baseline:
                result_file_name = "res_" + file_name
                print("running", result_file_name, "...")
                os.system("python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + os.path.join(files_path, file_name) + " -o " + os.path.join(files_path, result_file_name) + " -H min_cost -v")
                print("finishehd", result_file_name, " in ", time.time() - start_time, "s")
                start_time = time.time()
            else:
                for baseline in baselines[f_index]:
                    result_file_name = "res_baseline_" + baseline + "_" + file_name
                    # print("running", result_file_name, "...")
                    os.system("python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + os.path.join(files_path,
                                                                                                              file_name) + " -o " + os.path.join(
                        files_path, result_file_name) + " -H min_cost -v " + baselines[f_index][baseline])
                    print("python3 ../placement.py -f ../tests/inputtests/dc_gcp.json -i " + os.path.join(files_path,
                                                                                                              file_name) + " -o " + os.path.join(
                        files_path, result_file_name) + " -H min_cost -v " + baselines[f_index][baseline])
                    # print("finishehd", result_file_name, " in ", time.time() - start_time, "s")
                    start_time = time.time()
            # if len(sys.argv) == 3:  # Baseline
            #     RES_FILE_NAME = FILES_PATH_DIST + "res_baseline_" + FILE_NAME_TEMP + metric + ".json"
            #     COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i " + FILE_NAME + " -o " + RES_FILE_NAME + " -H min_cost -b -t abd -v"
            # else:
            #     RES_FILE_NAME = FILES_PATH_DIST + "res_" + FILE_NAME_TEMP + metric + ".json"
            #     COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i " + FILE_NAME + " -o " + RES_FILE_NAME + " -H min_cost -v"
            # print("running", FILE_NAME, "...")
            # os.system(COMMAND)
            # print("finishehd", FILE_NAME, "...")
            # print("duration:", time.time() - start_time)
            # start_time = time.time()

        # for cd in client_dists:
        #     client_dist = client_dists[cd]
        #     FILES_PATH_DIST = FILES_PATH + cd + "/"
        #
        #     for rr in read_ratios:
        #         FILE_NAME_TEMP = cd + "_" + rr + "_"
        #         read_ratio = read_ratios[rr];
        #         write_ratio = 1 - read_ratio;
        #
        #         for metric in metrics:
        #             FILE_NAME = FILES_PATH_DIST + FILE_NAME_TEMP + metric + ".json"
        #             if len(sys.argv) == 3: # Baseline
        #                 RES_FILE_NAME = FILES_PATH_DIST + "res_baseline_" + FILE_NAME_TEMP + metric + ".json"
        #                 COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i " + FILE_NAME + " -o " + RES_FILE_NAME + " -H min_cost -b -t abd -v"
        #             else:
        #                 RES_FILE_NAME = FILES_PATH_DIST + "res_" + FILE_NAME_TEMP + metric + ".json"
        #                 COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i " + FILE_NAME + " -o " + RES_FILE_NAME + " -H min_cost -v"
        #             print("running", FILE_NAME, "...")
        #             os.system(COMMAND)
        #             print("finishehd", FILE_NAME, "...")
        #             print("duration:", time.time() - start_time)
        #             start_time = time.time()
    
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
