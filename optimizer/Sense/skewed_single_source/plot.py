import matplotlib.pyplot as plt
import json
import os, sys, time

if __name__ == "__main__":

    os.system('mkdir -p graphs')
    
    FILE_FORMAT = 'res_{}_{}_{}.json'
    cli_distrib = ['skewed_single']
    rw_ratio = ['HR', 'HW', 'RW']
    properties = ['arrival_rate', 'object_count','object_size']

    
    for d in cli_distrib:
        for rw in rw_ratio:
            for p in properties:
                print("plotting ", FILE_FORMAT.format(d,rw,p), "...") 
                total_costs = []
                network_costs = []
                _data = json.load(open(FILE_FORMAT.format(d,rw,p),"r"))["output"]
                for ele in _data:
                    get_cost = float(ele["get_cost"])
                    put_cost = float(ele["put_cost"])
                    network_costs.append(get_cost + put_cost)
                    total_costs.append(ele["total_cost"])

                if p == "arrival_rate":
                    granularity = 50 
                    start       = 200
                    end         = 1000
                elif p == "object_count":
                    granularity = 10000 
                    start       = 1
                    end         = 1000000
                else:
                    granularity = 1000
                    start       = 1
                    end         = 100000
                    
                x_axis = list(range(start, end, granularity))
                print(len(x_axis), len(total_costs))
                plt.scatter(x_axis,total_costs)
                plt.ylabel("total_cost")
                plt.xlabel(p)
                print("saving ", FILE_FORMAT.format(d,rw,p), "...") 
                plt.savefig("graphs/" + 
                            FILE_FORMAT.format(d,rw,p) + ".png")
                plt.clf()
                print("cleared figure and move to next...")
                
                










         
    

    
    
    
