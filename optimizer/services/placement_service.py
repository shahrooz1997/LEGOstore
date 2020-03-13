import json
from itertools import product
from utils import combinations, generate_placement_params
from constants.opt_consts import FUNC_HEURISTIC_MAP, CAS, ABD, CAS_K_1, REP

def min_latency_abd(datacenters, group, params):
    """ Latency based greedy heuristic
    """
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 99999999999
    min_get_cost = 0
    min_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    m_g = 0
    storage_cost, vm_cost = 0, 0
    for m, q1, q2 in params:
        # May pre-compute this. (though itertool is optimized)
        # Get possible combination of DCs (of size m) from the set of DCs
        possible_dcs = combinations(dc_ids, m)
        for dcs in possible_dcs:
            _latencies = []
            #latency = 0
            _get_cost = 0
            _put_cost = 0
            combination = []
            for datacenter in datacenters:
                latency_list = [(d,datacenter.latencies[d]) for d in dcs]
                latency_list.sort(key=lambda x: x[1])
                # Get possible combination of DCs (of size q1 and q2) from the 
                # m-sized set of DCs.
                possible_quorum_dcs = []
                _iq1 = [l[0] for l in latency_list[:q1]]
                _iq2 = [l[0] for l in latency_list[:q2]]
                # Check if the selection meets latency constraints
                i = int(datacenter.id)
                _latencies.append( max([datacenter.latencies[j] for j in _iq1])+\
                                     max(datacenter.latencies[k] for k in _iq2))

                _get_cost += group.client_dist[i] * \
                                (sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                                    sum([datacenters[i].network_cost if k!=datacenter.id else 0.01 for k in _iq2]))
                _put_cost += group.client_dist[i] * \
                                (group.metadata_size*sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                                    group.object_size*sum([datacenters[i].network_cost if k!=datacenter.id else 0.01 for k in _iq2]))
                combination.append([dcs, _iq1, _iq2])
            latency = max(_latencies)
            if latency < group.slo_read and latency < group.slo_write:
                get_cost = group.read_ratio*group.arrival_rate*group.object_size*_get_cost
                put_cost = group.write_ratio*group.arrival_rate*_put_cost
                _storage_cost = group.num_objects*\
                                sum([datacenters[i].details["storage_cost"]/(730*3600) for i in dcs])*\
                                    group.object_size
                _vm_cost = sum([datacenters[i].details["price"]/3600 for i in dcs])
                if group.duration*(get_cost+put_cost+_storage_cost + _vm_cost) < mincost:
                    mincost = group.duration*(get_cost+put_cost+_storage_cost + _vm_cost)#+_vm_cost)
                    storage_cost, vm_cost = _storage_cost, _vm_cost
                    min_get_cost, min_put_cost = get_cost, put_cost
                    selected_placement = combination
                    read_lat, write_lat = latency, latency
                    m_g = m
        
    # Calculate other costs
    if selected_placement is None:
        return None
    selected_dcs = selected_placement[0][0]
    iq1 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq2 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    # Generate iq1, iq2 
    for i, val in enumerate(selected_placement):
        for j in val[1]:
            iq1[i][j] = 1
        for j in val[2]:
            iq2[i][j] = 1

    return (m_g, selected_dcs, iq1, iq2, read_lat, write_lat, 
                min_get_cost, min_put_cost, storage_cost, vm_cost)


def min_latency_cas(datacenters, group, params):
    """ Latency based heuristic
    """
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 99999999999
    min_get_cost = 0
    min_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    M, K = 0, 0
    storage_cost, vm_cost = 0, 0
    for m_g, k_g, q1, q2, q3, q4 in params:
        possible_dcs = combinations(dc_ids, m_g)
        for dcs in possible_dcs:
            get_lat = 0
            put_lat = 0
            _get_latencies = []
            _put_latencies = []
            _get_cost = 0
            _put_cost = 0
            combination = []
            for datacenter in datacenters:
                # Get possible combination of DCs (of size q1 and q2) from the 
                # m-sized set of DCs.
                latency_list = [(d,datacenter.latencies[d]) for d in dcs]
                latency_list.sort(key=lambda x: x[1])
                _iq1 = [l[0] for l in latency_list[:q1]]
                _iq2 = [l[0] for l in latency_list[:q2]]
                _iq3 = [l[0] for l in latency_list[:q3]]
                _iq4 = [l[0] for l in latency_list[:q4]]
                # Check if the selection meets latency constraints
                i = int(datacenter.id)
                _get_latencies.append(max([datacenter.latencies[j] for j in _iq1]) + \
                                        max([datacenter.latencies[k] for k in _iq4]))
                _put_latencies.append(max([datacenter.latencies[j] for j in _iq1]) + \
                                        max([datacenter.latencies[k] for k in _iq2]) + \
                                            max([datacenter.latencies[m] for m in _iq3]))
                _get_cost += group.client_dist[i] * \
                                (group.metadata_size*sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                                    (group.object_size/k_g)*sum([datacenters[k].network_cost if k!=datacenter.id else 0.01 for k in _iq4]))
                _put_cost += group.client_dist[i] * \
                                (group.metadata_size*(sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                                                        sum([datacenters[i].network_cost if k!=datacenter.id else 0.01 for k in _iq3])) + \
                                    (group.object_size/k_g)*sum([datacenters[i].network_cost if m!=datacenter.id else 0.01 for m in _iq2]))

                combination.append([dcs, _iq1, _iq2, _iq3, _iq4])
            get_lat = max(_get_latencies)
            put_lat = max(_put_latencies)
            if get_lat < group.slo_read and put_lat < group.slo_write:
                get_cost = group.read_ratio*group.arrival_rate*_get_cost
                put_cost = group.write_ratio*group.arrival_rate*_put_cost
                _storage_cost = group.num_objects*sum([datacenters[i].details["storage_cost"]/(730*3600) \
                                                        for i in dcs])*(group.object_size/k_g)
                _vm_cost = sum([datacenters[i].details["price"]/3600 for i in dcs])
                if group.duration*(get_cost+put_cost+_storage_cost + _vm_cost) < mincost:
                    mincost = group.duration*(get_cost+put_cost+_storage_cost + _vm_cost)#+_vm_cost)
                    min_get_cost, min_put_cost = get_cost, put_cost
                    storage_cost, vm_cost = _storage_cost, _vm_cost
                    read_lat, write_lat = get_lat, put_lat
                    selected_placement = combination
                    M, K = m_g, k_g
    # Calculate other costs
    if selected_placement is None:
        return None
    selected_dcs = selected_placement[0][0]
    iq1 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq2 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq3 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq4 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    for i, val in enumerate(selected_placement):
        for j in val[1]:
            iq1[i][j] = 1
        for j in val[2]:
            iq2[i][j] = 1
        for j in val[3]:
            iq3[i][j] = 1
        for j in val[4]:
            iq4[i][j] = 1
    return (M, K, selected_dcs, iq1, iq2, iq3, iq4, read_lat, write_lat, \
                min_get_cost, min_put_cost, storage_cost, vm_cost)

def min_cost_abd(datacenters, group, params):
    """ Network cost based greedy heuristic
    """
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 99999999999
    min_get_cost = 0
    min_get_cost = 0
    min_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    m_g = 0
    storage_cost, vm_cost = 0, 0
    cost_dc_list = [(i, d.network_cost) for i, d in enumerate(datacenters)]
    cost_dc_list.sort(key=lambda x: x[1])
    for m, q1, q2 in params:
        # May pre-compute this. (though itertool is optimized)
        # Get possible combination of DCs (of size m) from the set of DCs
        possible_dcs = combinations(dc_ids, m)
        for dcs in possible_dcs:
            latency = 0
            _latencies = []
            _get_cost = 0
            _put_cost = 0
            combination = []
            #cost_list = [elem for elem in cost_dc_list if elem[0] in dcs]
            for datacenter in datacenters:
                # Get possible combination of DCs (of size q1 and q2) from the 
                # m-sized set of DCs.
                cost_list = [elem if elem[0]!= datacenter.id else (elem[0], 0.01) for elem in cost_dc_list if elem[0] in dcs]
                cost_list.sort(key=lambda x: x[1])
                _iq1 = [l[0] for l in cost_list[:q1]]
                _iq2 = [l[0] for l in cost_list[:q2]]
                # Check if the selection meets latency constraints
                i = int(datacenter.id)
                _latencies.append( max([datacenter.latencies[j] for j in _iq1])+\
                                     max(datacenter.latencies[k] for k in _iq2))

                _get_cost += group.client_dist[i] * \
                                (sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                                    sum([datacenters[i].network_cost if k!=datacenter.id else 0.01 for k in _iq2]))
                _put_cost += group.client_dist[i] * \
                                (group.metadata_size*sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                                    group.object_size*sum([datacenters[i].network_cost if k!=datacenter.id else 0.01 for k in _iq2]))
                combination.append([dcs, _iq1, _iq2])
            latency = max(_latencies)
            if latency < group.slo_read and latency < group.slo_write:
                get_cost = group.read_ratio*group.arrival_rate*group.object_size*_get_cost
                put_cost = group.write_ratio*group.arrival_rate*_put_cost
                _storage_cost = group.num_objects*sum([datacenters[i].details["storage_cost"]/(730*3600) \
                                                        for i in dcs])*(group.object_size)
                _vm_cost = sum([datacenters[i].details["price"] for i in dcs])/3600
                if group.duration*(get_cost+put_cost+_storage_cost+_vm_cost) < mincost:
                    mincost = group.duration*(get_cost+put_cost+_storage_cost+_vm_cost)#+_vm_cost)
                    min_get_cost, min_put_cost = get_cost, put_cost
                    storage_cost, vm_cost = _storage_cost, _vm_cost
                    selected_placement = combination
                    read_lat, write_lat = latency, latency
                    m_g = m
    # Calculate other costs
    if selected_placement is None:
        return None
    selected_dcs = selected_placement[0][0]
    iq1 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq2 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    # Generate iq1, iq2 
    for i, val in enumerate(selected_placement):
        for j in val[1]:
            iq1[i][j] = 1
        for j in val[2]:
            iq2[i][j] = 1
    return (m_g, selected_dcs, val[1], val[2], read_lat, write_lat, 
                min_get_cost, min_put_cost, storage_cost, vm_cost)

def min_cost_cas(datacenters, group, params):
    """ Network cost based heuristic
    """
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 99999999999
    min_get_cost = 0
    min_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    cost_dc_list = [(i, d.network_cost) for i, d in enumerate(datacenters)]
    cost_dc_list.sort(key=lambda x: x[1])
    M, K = 0, 0
    storage_cost, vm_cost = 0, 0
    for m_g, k_g, q1, q2, q3, q4 in params:
        # May pre-compute this. (though itertool is optimized)
        # Get possible combination of DCs (of size m) from the set of DCs
        possible_dcs = combinations(dc_ids, m_g)
        for dcs in possible_dcs:
            get_lat = 0
            put_lat = 0
            _get_latencies = []
            _put_latencies = []
            combination = []
            _get_cost = 0
            _put_cost = 0
            #possible_cost_list = [elem for elem in cost_dc_list if elem[0] in dcs]
            for datacenter in datacenters:
                # Get possible combination of DCs (of size q1 and q2) from the 
                # m-sized set of DCs.
                cost_list = [elem if elem[0]!= datacenter.id else (elem[0], 0.01) for elem in cost_dc_list if elem[0] in dcs]
                cost_list.sort(key=lambda x: x[1])
                _iq1 = [l[0] for l in cost_list[:q1]]
                _iq2 = [l[0] for l in cost_list[:q2]]
                _iq3 = [l[0] for l in cost_list[:q3]]
                _iq4 = [l[0] for l in cost_list[:q4]]
                # Check if the selection meets latency constraints
                i = int(datacenter.id)
                _get_latencies.append(max([datacenter.latencies[j] for j in _iq1]) + \
                                        max([datacenter.latencies[k] for k in _iq4]))
                _put_latencies.append(max([datacenter.latencies[j] for j in _iq1]) + \
                                        max([datacenter.latencies[k] for k in _iq2]) + \
                                            max([datacenter.latencies[m] for m in _iq3]))

                _get_cost += group.client_dist[i] * \
                                (group.metadata_size*sum([datacenters[j].network_cost if datacenter.id != j else 0.01 for j in _iq1]) + \
                                    (group.object_size/k_g)*sum([datacenters[k].network_cost if datacenter.id != k else 0.01 for k in _iq4]))
                _put_cost += group.client_dist[i] * \
                                (group.metadata_size*(sum([datacenters[j].network_cost if datacenter.id != j else 0.01 for j in _iq1]) + \
                                                        sum([datacenters[i].network_cost if datacenter.id != k else 0.01 for k in _iq3])) + \
                                    (group.object_size/k_g)*sum([datacenters[i].network_cost if datacenter.id != m else 0.01 for m in _iq2]))

                combination.append([dcs, _iq1, _iq2, _iq3, _iq4])
            get_lat = max(_get_latencies)
            put_lat = max(_put_latencies)
            if get_lat < group.slo_read and put_lat < group.slo_write:
                get_cost = group.read_ratio*group.arrival_rate*_get_cost
                put_cost = group.write_ratio*group.arrival_rate*_put_cost
                _storage_cost = group.num_objects*sum([datacenters[i].details["storage_cost"]/(730*3600) \
                                                        for i in dcs])*(group.object_size/k_g)
                _vm_cost = sum([datacenters[i].details["price"]/3600 for i in dcs])
                tot = (get_cost+put_cost+_storage_cost+_vm_cost)*3600
                #print(m_g, k_g, get_cost*3600, put_cost*3600, _storage_cost*3600, _vm_cost*3600, tot)
                if group.duration*(get_cost+put_cost+_storage_cost+_vm_cost) < mincost:
                    mincost = group.duration*(get_cost+put_cost+_storage_cost+_vm_cost)
                    min_get_cost, min_put_cost = get_cost, put_cost
                    storage_cost, vm_cost = _storage_cost, _vm_cost
                    read_lat, write_lat = get_lat, put_lat
                    selected_placement = combination
                    M, K = m_g, k_g
    # Calculate other costs
    if selected_placement is None:
        return None
    selected_dcs = selected_placement[0][0]
    iq1 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq2 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq3 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq4 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    # Generate iq1, iq2 
    for i, val in enumerate(selected_placement):
        for j in val[1]:
            iq1[i][j] = 1
        for j in val[2]:
            iq2[i][j] = 1
        for j in val[3]:
            iq3[i][j] = 1
        for j in val[4]:
            iq4[i][j] = 1
    return (M, K, selected_dcs, val[1], val[2], val[3], val[4], read_lat, write_lat, \
                min_get_cost, min_put_cost, storage_cost, vm_cost)

def brute_force_abd(datacenters, group, params):
    """ Find placement for ABD using brute force
    """
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 999999
    min_get_cost = 0
    min_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    m_g = 0
    for m, q1, q2 in params:
        # May pre-compute this. (though itertool is optimized)
        # Get possible combination of DCs (of size m) from the set of DCs
        possible_dcs = combinations(dc_ids, m)
        for dcs in possible_dcs:
            # Get possible combination of DCs (of size q1 and q2) from the 
            # m-sized set of DCs.
            possible_quorum_dcs = []
            possible_quorum_dcs.append(combinations(dcs, q1))
            possible_quorum_dcs.append(combinations(dcs, q2))
            # Check if the selection meets latency constraints
            d = []
            for dc in datacenters:
                col = []
                for _iq1 in possible_quorum_dcs[0]: 
                    #_iq1 stores indices of non-zero iq1 variables
                    for _iq2 in possible_quorum_dcs[1]:
                        lat = group.client_dist[int(dc.id)] * \
                                (max([dc.latencies[j] for j in _iq1]) + \
                                    max([dc.latencies[k] for k in _iq2]))
                        col.append((lat, dcs, _iq1, _iq2))
                d.append(col)
            #print(len(d), len(d[0])) 
            for comb in product(*d):
                lat = 0
                _get_cost = 0
                _put_cost = 0
                for i, val in enumerate(comb):
                    lat += val[0]
                    _iq1 = val[2]
                    _iq2 = val[3]
                    _get_cost += group.client_dist[i] * \
                                    (sum([datacenters[i].network_cost for j in _iq1]) + \
                                        sum([datacenters[k].network_cost for k in _iq2]))
                    _put_cost += group.client_dist[i] * \
                                    (group.metadata_size*sum([datacenters[i].network_cost for j in _iq1]) + \
                                        group.object_size*sum([datacenters[k].network_cost for k in _iq2]))
                if lat < group.slo_read and lat < group.slo_write:
                    # Calculate cost
                    get_cost = group.read_ratio*group.arrival_rate*group.object_size*_get_cost
                    put_cost = group.read_ratio*group.arrival_rate*_put_cost
                    if (get_cost+put_cost) < mincost:
                        mincost = get_cost+put_cost
                        min_get_cost = get_cost
                        min_put_cost = put_cost
                        selected_placement = comb
                        read_lat = lat
                        write_lat = lat
                        m_g = m
    # Calculate other costs
    selected_dcs = selected_placement[0][1]
    storage_cost = group.num_objects*sum([datacenters[i].details["storage_cost"] \
                                            for i in selected_dcs])*group.object_size
    vm_cost = sum([datacenters[i].details["price"] for i in selected_dcs])
    iq1 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq2 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    # Generate iq1, iq2 
    for i, val in enumerate(selected_placement):
        for j in val[2]:
            iq1[i][j] = 1
        for j in val[3]:
            iq2[i][j] = 1
    return (m_g, selected_dcs, iq1, iq2, read_lat, write_lat, 
                min_get_cost, min_put_cost, storage_cost, vm_cost)
    

def brute_force_cas(datacenters, group, params):
    """ Find placement for CAS using brute force
    """
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 999999
    min_get_cost = 0
    min_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    M, K = 0, 0
    for param in params:
        # May pre-compute this. (though itertool is optimized)
        # Get possible combination of DCs (of size m) from the set of DCs
        m_g = param[0]
        k_g = param[1]
        q_sizes = param[2:]
        possible_dcs = combinations(dc_ids, m_g)
        for dcs in possible_dcs:
            # Get possible combination of DCs (of size q1 and q2) from the 
            # m-sized set of DCs.
            possible_quorum_dcs = []
            for size in q_sizes:
                possible_quorum_dcs.append(combinations(dcs, size))
            # Check if the selection meets latency constraints
            d = []
            for dc in datacenters:
                col = []
                for _iq1 in possible_quorum_dcs[0]: 
                    #_iq1 stores indices of non-zero iq1 variables
                    for _iq2 in possible_quorum_dcs[1]:
                        for _iq3 in possible_quorum_dcs[2]:
                            for _iq4 in possible_quorum_dcs[3]:
                                get_lat = group.client_dist[int(dc.id)] * \
                                            (max([dc.latencies[j] for j in _iq1]) + \
                                                max([dc.latencies[k] for k in _iq4]))
                                put_lat = group.client_dist[int(dc.id)] * \
                                            (max([dc.latencies[j] for j in _iq1]) + \
                                                max([dc.latencies[k] for k in _iq2]) + \
                                                    max([dc.latencies[m] for m in _iq3]))
                                col.append((get_lat, put_lat, dcs, _iq1, _iq2, _iq3, _iq4))
                d.append(col)
            #print(len(d), len(d[0])) 
            for comb in product(*d):
                get_lat = 0
                put_lat = 0
                _get_cost = 0
                _put_cost = 0
                for i, val in enumerate(comb):
                    get_lat += val[0]
                    put_lat += val[1]
                    _iq1 = val[3]
                    _iq2 = val[4]
                    _iq3 = val[5]
                    _iq4 = val[6]
                    _get_cost += group.client_dist[i] * \
                                    (group.metadata_size*sum([datacenters[i].network_cost for j in _iq1]) + \
                                        (group.object_size/k_g)*sum([datacenters[k].network_cost for k in _iq4]))
                    _put_cost += group.client_dist[i] * \
                                    (group.metadata_size*(sum([datacenters[i].network_cost for j in _iq1]) + \
                                                            sum([datacenters[i].network_cost for k in _iq3])) + \
                                        (group.object_size/k_g)*sum([datacenters[m].network_cost for m in _iq2]))
                if get_lat < group.slo_read and put_lat < group.slo_write:
                    # Calculate cost
                    get_cost = group.read_ratio*group.arrival_rate*_get_cost
                    put_cost = group.read_ratio*group.arrival_rate*_put_cost
                    if (get_cost+put_cost) < mincost:
                        mincost = get_cost+put_cost
                        min_get_cost, min_put_cost = get_cost, put_cost
                        read_lat, write_lat = get_lat, put_lat
                        selected_placement = comb
                        M, K = m_g, k_g
    # Calculate other costs
    selected_dcs = selected_placement[0][2]
    storage_cost = group.num_objects*sum([datacenters[i].details["storage_cost"] \
                                            for i in selected_dcs])*group.object_size/K
    vm_cost = sum([datacenters[i].details["price"] for i in selected_dcs])
    iq1 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq2 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq3 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq4 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    # Generate iq1, iq2 
    for i, val in enumerate(selected_placement):
        for j in val[3]:
            iq1[i][j] = 1
        for j in val[4]:
            iq2[i][j] = 1
        for j in val[5]:
            iq3[i][j] = 1
        for j in val[6]:
            iq4[i][j] = 1
    return (selected_dcs, iq1, iq2, iq3, iq4, M, K, read_lat, write_lat, \
                min_get_cost, min_put_cost, storage_cost, vm_cost)

def get_placement(obj, heuristic, M, K, verbose, grp, use_protocol_param=False):
    N = len(obj.datacenters)
    G = len(obj.groups)
    # Iterate over groups and find placements for each group
    result = []
    time_period = 0
    groups = obj.groups
    if grp:
        grp = grp.split(",")
        groups = [obj.groups[int(i)] for i in grp]
    for i, group in enumerate(groups):
        #print("group %s"%i)
        _res = []
        protocols =  [CAS, ABD, REP]
        if use_protocol_param is True:
            protocols = [obj.protocol]
        for protocol in protocols:
            d = {}
            f = group.availability_target
            if protocol==REP:
                params = generate_placement_params(N, f, protocol, 1, M)

            else:
                params = generate_placement_params(N, f, protocol, K, M)
            ret = eval(FUNC_HEURISTIC_MAP[protocol][heuristic])(obj.datacenters, group, params)
            d["id"] = i
            if ret is None:
                d["total_cost"] = float("inf")
                d["protocol"] = protocol
                d["m"] = "INVALID"
                _res.append([d, d["total_cost"]])
                continue
            d["protocol"] = protocol
            d["m"] = ret[0]
            if protocol in [CAS, REP]: d["k"] = ret[1]
            if (protocol == CAS and d["k"]==1) or protocol==REP:
                d["protocol"] = REP
            d["selected_dcs"] = ret[1] if protocol==ABD else ret[2]
            d["get_cost"] = group.duration*(ret[6] if protocol==ABD else ret[9])
            d["put_cost"] = group.duration*(ret[7] if protocol==ABD else ret[10])
            d["storage_cost"] = group.duration*(ret[8] if protocol==ABD else ret[11])
            d["vm_cost"] = group.duration*(ret[9] if protocol==ABD else ret[12])
            total_cost = d["get_cost"]+d["put_cost"]+d["storage_cost"]+d["vm_cost"]
            d["total_cost"] = total_cost
            d["get_latency"] = ret[4] if protocol==ABD else ret[7]
            d["put_latency"] = ret[5] if protocol==ABD else ret[8]
            if verbose:
                #print("Verbose is ", verbose)
                d["iq1"] = ret[2] if protocol==ABD else ret[3]
                d["iq2"] = ret[3] if protocol==ABD else ret[4]
                if protocol in [CAS, REP]:
                    d["iq3"] = ret[5]
                    d["iq4"] = ret[6]
            _res.append([d, total_cost])
        time_period += (group.duration/3600)
        if len(_res)>0:
            _res.sort(key = lambda x: x[1])
            result.append(_res[0][0])
    if len(result)>0:
        overall_cost = sum([d["total_cost"] for d in result if d.get("total_cost")!=float("inf")])
        get_cost = sum([d["get_cost"] for d in result if d.get("get_cost")!=float("inf") and d.get("get_cost")!=None])
        put_cost = sum([d["put_cost"] for d in result if d.get("put_cost")!=float("inf") and d.get("put_cost")!=None])
        storage_cost = sum([d["storage_cost"] for d in result if d.get("storage_cost")!=float("inf") and d.get("storage_cost")!=None])
        vm_cost = sum([d["vm_cost"] for d in result if d.get("vm_cost")!=float("inf") and d.get("vm_cost")!=None])
        d = {"output": result,\
             "overall_cost": overall_cost, \
             "get_cost": get_cost,\
             "put_cost": put_cost,\
             "storage_cost": storage_cost, \
             "vm_cost": vm_cost,\
             "time_period_hrs": time_period}
    obj.placements = d
