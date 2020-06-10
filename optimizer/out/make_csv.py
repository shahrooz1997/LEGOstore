import json
import csv
import sys

fname = sys.argv[1]
file_name = fname+'opt.json'
with open(file_name, "r", encoding="utf-8-sig") as f:
    do = json.load(f)
do = do["output"]
file_name = fname+'abd.json'
with open(file_name, "r", encoding="utf-8-sig") as f:
    da = json.load(f)
da = da["output"]

file_name = fname+'cas.json'
with open(file_name, "r", encoding="utf-8-sig") as f:
    dc = json.load(f)
dc = dc["output"]

file_name = fname+'rep.json'
with open(file_name, "r", encoding="utf-8-sig") as f:
    dr = json.load(f)
dr = dr["output"]

gc = ["Get Cost"]
pc = ["Put Cost"]
sc = ["Storage Cost"]
vc = ["VM Cost"]

o = []
a = []
r = []

print(do, type(do))

for i in range(len(do)):
    gc.extend([do[i]["get_cost"], da[i]["get_cost"], dc[i]["get_cost"], dr[i]["get_cost"]])
    pc.extend([do[i]["put_cost"], da[i]["put_cost"], dc[i]["put_cost"], dr[i]["put_cost"]])
    sc.extend([do[i]["storage_cost"], da[i]["storage_cost"], dc[i]["storage_cost"], dr[i]["storage_cost"]])
    vc.extend([do[i]["vm_cost"], da[i]["vm_cost"], dc[i]["vm_cost"], dr[i]["vm_cost"]])

    o.extend([do[i]["get_cost"], do[i]["put_cost"], do[i]["storage_cost"], do[i]["vm_cost"]])
    a.extend([da[i]["get_cost"], da[i]["put_cost"], da[i]["storage_cost"], da[i]["vm_cost"]])
    r.extend([dr[i]["get_cost"], dr[i]["put_cost"], dr[i]["storage_cost"], dr[i]["vm_cost"]])


tot = []
tot.append(gc)
tot.append(pc)
tot.append(sc)
tot.append(vc)

out_name = fname+"res.csv"
with open(out_name, "w") as f:
    writer = csv.writer(f)
    writer.writerows(tot)
"""
tot = []
tot.append(o)
tot.append(a)
tot.append(r)

with open('dom_cost.csv', "w") as f:
    writer = csv.writer(f)
    writer.writerows(tot)
"""
