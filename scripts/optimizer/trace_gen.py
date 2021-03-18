import sys
import csv
import json
import math
import random
import argparse
import pandas as pd
from argparse import ArgumentParser
from factory import obj_factory, json_to_obj
from cls import Group
from constants.opt_consts import GROUP

def parse_args():
    parser = ArgumentParser(description = 'Process cmd args for placements')
    parser.add_argument('-f','--file-name', dest='file_name', required=True)
    parser.add_argument('-o','--out-file', dest='outfile', required=False)
    parser.add_argument('-i','--out-id', dest='outid', required=False)
    args = parser.parse_args()
    return args

def process_input(file_name):
    datacenters = []
    groups = []
    with open(file_name, "r", encoding="utf-8-sig") as f:
        data = json.load(f)
    for grp in data.get("input_groups"):
        grp_obj = json_to_obj(grp, GROUP)
        groups.append(grp_obj)
    return groups

def main(args, groups):
    out_file = args.outfile
    if out_file is None:
        out_file = './tests/traces/trace_%s/trace.csv'%args.outid
    num_dcs = len(groups[0].client_dist)
    client_files = ['./tests/traces/trace_%s/dc_%s.csv'%(args.outid, i) \
                        for i in range(num_dcs)]
    start_key_id = 0
    trace = []
    keys_dmp = []
    cl_trace = {}
    num_keys = 0
    for i, grp in enumerate(groups):
        #keys = [j for j in range(start_key_id, start_key_id+int(grp.num_objects))]
        low = start_key_id
        high = start_key_id+int(grp.num_objects)-1
        num_keys += (high-low+1)
        reads = math.ceil(grp.read_ratio*grp.arrival_rate*grp.duration)
        writes = math.ceil(grp.write_ratio*grp.arrival_rate*grp.duration)
        reqs = [[random.randint(low, high), 'r', i] for _ in range(reads)]+\
                    [[random.randint(low, high), 'w', i] for _ in range(writes)]
        random.shuffle(reqs)

        #TODO add client distribution as feature to key
        """
        df = pd.DataFrame(reqs)
        df = df.rename(columns={0: 'key', 1: 'op', 2: 'group'})
        for key in range(low, high+1):
            tmp = df[df['key']==key]
            reads = sum(tmp['op']=='r')
            writes = sum(tmp['op']=='w')
            ratio = reads/(reads+writes)
            keys_dmp.append([(reads+writes)/grp.num_objects, ratio])
        """

        #break
        # Divide into per client trace
        start_idx = 0
        for client, dist in enumerate(grp.client_dist):
            length = math.ceil(len(reqs)*dist)
            cl_reqs = reqs[start_idx:start_idx+length]
            cl_reqs = [x+[client] for x in cl_reqs]
            random.shuffle(cl_reqs)
            cl_trace.setdefault(client, []).extend(cl_reqs)
            start_idx += length
            print(client, len(cl_reqs))
        start_key_id += int(grp.num_objects)
        #print(cl_trace)
    # Write to files
    
    for k, val in cl_trace.items():
        random.shuffle(val)
        with open(client_files[int(k)], "w") as f:
            writer = csv.writer(f)
            writer.writerows(val)
        trace += val

    with open(out_file, "w") as f:
        writer = csv.writer(f)
        writer.writerows(trace)

    counts = [[0]*(2+num_dcs) for _ in range(num_keys)]
    for line in trace:
        key = int(line[0])
        if line[1] == 'r':
            counts[key][-2] += 1
        else:
            counts[key][-1] += 1
        counts[key][int(line[3])] += 1

    
    for c in counts:
        total = c[-1]+c[-2]
        if total == 0:
            continue
        rate = total/3600
        r_ratio = c[-2]/c[-1] if c[-1] else 1
        s_dist = [e/total for e in c[:-2]]
        feature = [rate, r_ratio]
        feature.extend(s_dist)
        keys_dmp.append(feature)
    
    keys_file = './tests/traces/trace_%s/keys.csv'%args.outid
    with open(keys_file, "w") as f:
        writer = csv.writer(f)
        writer.writerows(keys_dmp)



if __name__ == "__main__":
    args = parse_args()
    groups = process_input(args.file_name)
    main(args, groups)
