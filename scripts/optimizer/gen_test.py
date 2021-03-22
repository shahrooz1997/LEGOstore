import sys
import json
import copy
import random

path = sys.argv[1]
key_dist = sys.argv[2]

read_ratios = [0.968, 0.5, 0.032]
client_dists = [ [0.11, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11, 0.12],\
                [0.9, 0.01, 0.01, 0.03, 0.01, 0.01, 0.01, 0.01, 0.01], \
                [0.3, 0.01, 0.01, 0.04, 0.01, 0.01, 0.6, 0.01, 0.01] ]
obj_size = [1, 10, 100] # in KB
storage = [10, 1000] # in GB
duration = 3600 # in hr
_dist = [(0.1, 0.01), (0.2, 0.02), (0.2, 0.015), (0.15, 0.01), (0.15, 0.02), (0.05, 0.01), (0.05, 0.015), (0.05, 0.5), (0.05, 0.4)]

for size in obj_size:
    for store in storage:
        inp = {"input_groups": []}
        total_objs = (store*1000000)/size
        _dst = copy.deepcopy(_dist)
        random.shuffle(_dst)
        i = 0
        for dist in client_dists:
            for read_ratio in read_ratios:
                d = { "availability_target": 2,\
                      "object_size": size/1000000,\
                      "metadata_size": 0.0000001,\
                      "SLO_read": 1000,\
                      "SLO_write": 1000,\
                      "beta": 0.1,\
                      "duration": duration,\
                      "time_to_decode": 0.00028
                    }
                d["read_ratio"] = read_ratio
                d["write_ratio"] = 1-read_ratio
                d["client_dist"] = dist
                if key_dist == 'u':
                    num_objects = int(total_objs/9)
                    d["num_objects"] = num_objects
                    d["arrival_rate"] = 300
                else:
                    d["num_objects"] = int(_dst[i][0]*total_objs)
                    d["arrival_rate"] = int(_dst[i][1]*2700)
                inp.setdefault("input_groups", []).append(d)
                i+=1

        fname = "%s/%s_%s.json"%(path,size,store)
        with open(fname, 'w') as fp:
            json.dump(inp, fp, indent=2)
