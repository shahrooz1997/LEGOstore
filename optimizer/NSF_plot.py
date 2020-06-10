import json
import matplotlib.pyplot as plt


if __name__ == "__main__":
    
    network_costs = []
    total_costs = []
    arrival_rates = list(range(450, 651, 10)) 
    with open("NSF_output/NSF_proposal_res.json") as fd:
        _outputs = json.load(fd)["output"]
        for ele in _outputs:
            get_cost = float(ele["get_cost"])
            put_cost = float(ele["put_cost"])
            network_costs.append(get_cost + put_cost)
            total_costs.append(float(ele["total_cost"]))

        assert(len(network_costs) == len(total_costs))


    x = arrival_rates
    #y = [network_costs[i]/total_costs[i] for i in range(len(total_costs))]
    #y = network_costs
    y = total_costs
    plt.scatter(x, y)
    plt.show()

     
    
