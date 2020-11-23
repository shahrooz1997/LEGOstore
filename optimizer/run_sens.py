import os, sys, time, json


if __name__ == "__main__":

    start_time = time.time()
    
    
    COMMAND = "python3 placement.py -f tests/inputtests/dc_gcp.json -i Sense/uniform/{} -o Sense/uniform/res_{} -H min_cost -v"
    files = [
        'uniform_HR_arrival_rate.json',
        'uniform_HR_object_count.json',
        'uniform_HR_object_size.json',
        'uniform_HW_arrival_rate.json',
        'uniform_HW_object_count.json',
        'uniform_HW_object_size.json',
        'uniform_RW_arrival_rate.json',
        'uniform_RW_object_count.json',
        'uniform_RW_object_size.json'
    ]

    for filename in files:
        print("running", filename, "...")
        os.system(COMMAND.format(filename, filename))
        print("finishehd", filename, "...")

    print("duration:", time.time() - start_time)
    start_time = time.time()


    ###############
    COMMAND = "python3 placement.py -f tests/inputtests/dc.json -i Sense/skewed_single_source/{} -o Sense/skewed_single_source/res_{} -H min_cost"
    files = [
        'skewed_single_HR_arrival_rate.json',
        'skewed_single_HR_object_count.json',
        'skewed_single_HR_object_size.json',
        'skewed_single_HW_arrival_rate.json',
        'skewed_single_HW_object_count.json',
        'skewed_single_HW_object_size.json',
        'skewed_single_RW_arrival_rate.json',
        'skewed_single_RW_object_count.json',
        'skewed_single_RW_object_size.json'
    ]

    for filename in files:
        print("running", filename, "...")
        os.system(COMMAND.format(filename, filename))
        print("finishehd", filename, "...")
    


    print("duration:", time.time() - start_time)
    start_time = time.time()

    ##########################
    COMMAND = "python3 placement.py -f tests/inputtests/dc.json -i Sense/skewed_double_source/{} -o Sense/skewed_double_source/res_{} -H min_cost"
    
    files = [
        'skewed_double_HR_arrival_rate.json',
        'skewed_double_HR_object_count.json',
        'skewed_double_HR_object_size.json',
        'skewed_double_HW_arrival_rate.json',
        'skewed_double_HW_object_count.json',
        'skewed_double_HW_object_size.json',
        'skewed_double_RW_arrival_rate.json',
        'skewed_double_RW_object_count.json',
        'skewed_double_RW_object_size.json'
    ]

    for filename in files:
        print("running", filename, "...")
        os.system(COMMAND.format(filename, filename))
        print("finishehd", filename, "...")
    

    print("duration:", time.time() - start_time)
