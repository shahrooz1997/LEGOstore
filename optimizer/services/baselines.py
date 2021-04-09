
from itertools import product
from utils import combinations, generate_placement_params, compute_vm_cost
from constants.opt_consts import FUNC_HEURISTIC_MAP, CAS, ABD, CAS_K_1, REP
import math

def baseline1_abd(datacenters, group, params):
    """ Latency based greedy heuristic
    """
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 99999999999
    min_lat = 99999999999
    # min_get_lat = 99999999999
    selected_get_cost = 0
    selected_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    selected_full_placement_info = None
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
            full_placement_info = []
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

                this_client_get_latency = max([datacenter.latencies[j] for j in _iq1])+\
                                      max(datacenter.latencies[k] for k in _iq2)
                this_client_put_latency = max([datacenter.latencies[j] for j in _iq1])+\
                                      max(datacenter.latencies[k] for k in _iq2)

                if group.client_dist[i] != 0:
                    _latencies.append(max([datacenter.latencies[j] for j in _iq1])+\
                                      max(datacenter.latencies[k] for k in _iq2))

                this_client_get_cost = group.object_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                       group.object_size * sum([datacenters[i].network_cost[k] for k in _iq2])
                this_client_put_cost = group.metadata_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                       group.object_size * sum([datacenters[i].network_cost[k] for k in _iq2])

                # _get_cost += group.client_dist[i] * \
                #                 (sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                #                     sum([datacenters[i].network_cost if k!=datacenter.id else 0.01 for k in _iq2]))
                # _put_cost += group.client_dist[i] * \
                #                 (group.metadata_size*sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                #                     group.object_size*sum([datacenters[i].network_cost if k!=datacenter.id else 0.01 for k in _iq2]))

                _get_cost += group.client_dist[i] * \
                             (sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                              sum([datacenters[i].network_cost[k] for k in _iq2]))
                _put_cost += group.client_dist[i] * \
                             (group.metadata_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                              group.object_size * sum([datacenters[i].network_cost[k] for k in _iq2]))
                combination.append([dcs, _iq1, _iq2])
                full_placement_info.append([dcs, _iq1, _iq2, [], [], this_client_get_cost, this_client_put_cost, this_client_get_latency, this_client_put_latency])

            latency = max(_latencies)
            if latency < group.slo_read and latency < group.slo_write:
                get_cost = group.read_ratio*group.arrival_rate*group.object_size*_get_cost
                put_cost = group.write_ratio*group.arrival_rate*_put_cost
                _storage_cost = group.num_objects*\
                                sum([datacenters[i].details["storage_cost"]/(730*3600) for i in dcs])*\
                                    group.object_size
                # _vm_cost = group.arrival_rate / 600. * sum([datacenters[i].details["price"] for i in dcs])/3600
                _vm_cost = compute_vm_cost(group, datacenters, full_placement_info)
                if latency < min_lat: # group.duration*(get_cost+put_cost+_storage_cost + _vm_cost) < mincost:
                    mincost = group.duration*(get_cost+put_cost+_storage_cost + _vm_cost)#+_vm_cost)
                    min_lat = latency
                    storage_cost, vm_cost = _storage_cost, _vm_cost
                    selected_get_cost, selected_put_cost = get_cost, put_cost
                    selected_placement = combination
                    selected_full_placement_info = full_placement_info
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

    return (m_g, selected_dcs, selected_full_placement_info, read_lat, write_lat,
                selected_get_cost, selected_put_cost, storage_cost, vm_cost)

def baseline1_cas(datacenters, group, params):
    """ Latency based heuristic
    """
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 99999999999
    min_put_lat = 99999999999
    min_get_lat = 99999999999
    min_avg_lat = 99999999999
    selected_get_cost = 0
    selected_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    selected_full_placement_info = None
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
            full_placement_info = []
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

                this_client_get_latency = max([datacenter.latencies[j] for j in _iq1]) + \
                                            max([datacenter.latencies[k] for k in _iq4])
                this_client_put_latency = max([datacenter.latencies[j] for j in _iq1]) + \
                                            max([datacenter.latencies[k] for k in _iq2]) + \
                                                max([datacenter.latencies[m] for m in _iq3])

                if group.client_dist[i] != 0:
                    _get_latencies.append(max([datacenter.latencies[j] for j in _iq1]) + \
                                            max([datacenter.latencies[k] for k in _iq4]))
                    _put_latencies.append(max([datacenter.latencies[j] for j in _iq1]) + \
                                            max([datacenter.latencies[k] for k in _iq2]) + \
                                                max([datacenter.latencies[m] for m in _iq3]))

                this_client_get_cost = group.metadata_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                       (group.object_size / k_g) * sum([datacenters[k].network_cost[i] for k in _iq4])
                this_client_put_cost = group.metadata_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                       group.metadata_size * sum([datacenters[i].network_cost[k] for k in _iq3]) + \
                                       group.object_size / k_g * sum([datacenters[i].network_cost[m] for m in _iq2])

                _get_cost += group.client_dist[i] * \
                             (group.metadata_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                              (group.object_size / k_g) * sum([datacenters[k].network_cost[i] for k in _iq4]))
                _put_cost += group.client_dist[i] * \
                             (group.metadata_size * (sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                                     sum([datacenters[i].network_cost[k] for k in _iq3])) + \
                              (group.object_size / k_g) * sum([datacenters[i].network_cost[m] for m in _iq2]))

                # _get_cost += group.client_dist[i] * \
                #                 (group.metadata_size*sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                #                     (group.object_size/k_g)*sum([datacenters[k].network_cost if k!=datacenter.id else 0.01 for k in _iq4]))
                # _put_cost += group.client_dist[i] * \
                #                 (group.metadata_size*(sum([datacenters[j].network_cost if j!=datacenter.id else 0.01 for j in _iq1]) + \
                #                                         sum([datacenters[i].network_cost if k!=datacenter.id else 0.01 for k in _iq3])) + \
                #                     (group.object_size/k_g)*sum([datacenters[i].network_cost if m!=datacenter.id else 0.01 for m in _iq2]))

                combination.append([dcs, _iq1, _iq2, _iq3, _iq4])
                full_placement_info.append([dcs, _iq1, _iq2, _iq3, _iq4, this_client_get_cost, this_client_put_cost, this_client_get_latency, this_client_put_latency])

            get_lat = max(_get_latencies)
            put_lat = max(_put_latencies)
            avg_lat = group.read_ratio * get_lat + group.write_ratio * put_lat
            if get_lat < group.slo_read and put_lat < group.slo_write:

                get_cost = group.read_ratio*group.arrival_rate*_get_cost
                put_cost = group.write_ratio*group.arrival_rate*_put_cost
                _storage_cost = group.num_objects*sum([datacenters[i].details["storage_cost"]/(730*3600) \
                                                        for i in dcs])*(group.object_size/k_g)
                # _vm_cost = group.arrival_rate / 600. * sum([datacenters[i].details["price"]/3600 for i in dcs])
                _vm_cost = compute_vm_cost(group, datacenters, full_placement_info)
                if avg_lat < min_avg_lat: #put_lat <= min_put_lat and get_lat <= min_get_lat: # group.duration*(get_cost+put_cost+_storage_cost + _vm_cost) < mincost:
                    # print(get_lat, put_lat)
                    mincost = group.duration*(get_cost+put_cost+_storage_cost + _vm_cost)#+_vm_cost)
                    min_put_lat = put_lat
                    min_get_lat = get_lat
                    min_avg_lat = avg_lat
                    selected_get_cost, selected_put_cost = get_cost, put_cost
                    storage_cost, vm_cost = _storage_cost, _vm_cost
                    read_lat, write_lat = get_lat, put_lat
                    selected_placement = combination
                    selected_full_placement_info = full_placement_info
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
    return (M, K, selected_dcs, selected_full_placement_info, read_lat, write_lat, \
                selected_get_cost, selected_put_cost, storage_cost, vm_cost)

def cheapest_dcs(m, prices):
    prices = [(i, d[0]) for i, d in prices]
    prices.sort(key=lambda x: x[1])
    return [dc[0] for dc in prices[:m]]

def baseline0_abd(datacenters, group, params):
    """ Network cost based greedy heuristic
    """
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 99999999999
    min_get_cost = 0
    min_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    selected_full_placement_info = None
    m_g = 0
    storage_cost, vm_cost = 0, 0
    cost_dc_list = [(i, d.network_cost) for i, d in enumerate(datacenters)]

    # network prices are average, not different for each pair
    cost_dc_list_avg = []
    for c in cost_dc_list:
        the_sum = 0
        for p in c[1]:
            the_sum += p
        avg = the_sum / len(c)
        avg_prices = [avg for _ in range(len(c[1]))]
        cost_dc_list_avg.append((c[0], avg_prices))

    # only int(N/2) + 1 as the quorums' sizes are acceptable
    new_params = []
    for m, q1, q2 in params:
        if q1 > int(m/2) and q2 > int(m/2):
            new_params.append([m, q1, q2])
    params = new_params

    # cost_dc_list.sort(key=lambda x: x[1])
    for m, q1, q2 in params:
        # May pre-compute this. (though itertool is optimized)
        # Get possible combination of DCs (of size m) from the set of DCs
        # possible_dcs = combinations(dc_ids, m)
        possible_dcs = [cheapest_dcs(m, cost_dc_list_avg)]
        for dcs in possible_dcs:
            latency = 0
            _latencies = []
            _get_cost = 0
            _put_cost = 0
            combination = []
            full_placement_info = []
            #cost_list = [elem for elem in cost_dc_list if elem[0] in dcs]
            for datacenter in datacenters:
                # Get possible combination of DCs (of size q1 and q2) from the
                # m-sized set of DCs.
                # cost_list = [(elem[0], elem[1][datacenter.id]) for elem in cost_dc_list if elem[0] in dcs]
                # cost_list.sort(key=lambda x: x[1])
                _iq1 = cheapest_dcs(q1, cost_dc_list_avg) #[l[0] for l in cost_list[:q1]]
                _iq2 = cheapest_dcs(q2, cost_dc_list_avg) #[l[0] for l in cost_list[:q2]]
                # Check if the selection meets latency constraints
                i = int(datacenter.id)

                this_client_get_latency = max([datacenter.latencies[j] for j in _iq1])+\
                                          max(datacenter.latencies[k] for k in _iq2)
                this_client_put_latency = max([datacenter.latencies[j] for j in _iq1])+\
                                          max(datacenter.latencies[k] for k in _iq2)

                if group.client_dist[i] != 0:
                    _latencies.append(max([datacenter.latencies[j] for j in _iq1])+\
                                      max(datacenter.latencies[k] for k in _iq2))

                this_client_get_cost = group.object_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                        group.object_size * sum([datacenters[i].network_cost[k] for k in _iq2])
                this_client_put_cost = group.metadata_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                        group.object_size * sum([datacenters[i].network_cost[k] for k in _iq2])

                _get_cost += group.client_dist[i] * \
                                (sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                    sum([datacenters[i].network_cost[k] for k in _iq2]))
                _put_cost += group.client_dist[i] * \
                                (group.metadata_size*sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                    group.object_size*sum([datacenters[i].network_cost[k] for k in _iq2]))
                combination.append([dcs, _iq1, _iq2])
                full_placement_info.append([dcs, _iq1, _iq2, [], [], this_client_get_cost, this_client_put_cost, this_client_get_latency, this_client_put_latency])

            latency = max(_latencies)
            if latency < group.slo_read and latency < group.slo_write:
                get_cost = group.read_ratio*group.arrival_rate*group.object_size*_get_cost
                put_cost = group.write_ratio*group.arrival_rate*_put_cost
                _storage_cost = group.num_objects*sum([datacenters[i].details["storage_cost"]/(730*3600) \
                                                        for i in dcs])*(group.object_size)
                # _vm_cost = group.arrival_rate / 600. * sum([datacenters[i].details["price"] for i in dcs])/3600
                _vm_cost = compute_vm_cost(group, datacenters, full_placement_info)
                if group.duration*(get_cost+put_cost+_storage_cost+_vm_cost) < mincost:
                    mincost = group.duration*(get_cost+put_cost+_storage_cost+_vm_cost)#+_vm_cost)
                    min_get_cost, min_put_cost = get_cost, put_cost
                    storage_cost, vm_cost = _storage_cost, _vm_cost
                    selected_placement = combination
                    selected_full_placement_info = full_placement_info
                    read_lat, write_lat = latency, latency
                    m_g = m
    # Calculate other costs
    if selected_placement is None:
        # return (m_g, None, selected_full_placement_info, read_lat, write_lat,
        #         min_get_cost, min_put_cost, storage_cost, vm_cost)
        print("WARN: No placement with the heuristic min cost was found.")
        return None
        # return min_latency_abd(datacenters, group, params)

    selected_dcs = selected_placement[0][0]
    iq1 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    iq2 = [[0]*len(dc_ids) for _ in range(len(dc_ids))]
    # Generate iq1, iq2
    for i, val in enumerate(selected_placement):
        for j in val[1]:
            iq1[i][j] = 1
        for j in val[2]:
            iq2[i][j] = 1
    return (m_g, selected_dcs, selected_full_placement_info, read_lat, write_lat,
                min_get_cost, min_put_cost, storage_cost, vm_cost)

def baseline0_cas(datacenters, group, params):
    """ Network cost based heuristic
    """
    # print("STart")
    dc_ids = [int(dc.id) for dc in datacenters]
    mincost = 99999999999
    min_get_cost = 0
    min_put_cost = 0
    read_lat = 0
    write_lat = 0
    selected_placement = None
    selected_full_placement_info = None
    cost_dc_list = [(i, d.network_cost) for i, d in enumerate(datacenters)]
    # cost_dc_list.sort(key=lambda x: x[1])

    # network prices are average, not different for each pair
    cost_dc_list_avg = []
    for c in cost_dc_list:
        the_sum = 0
        for p in c[1]:
            the_sum += p
        avg = the_sum / len(c)
        avg_prices = [avg for _ in range(len(c[1]))]
        cost_dc_list_avg.append((c[0], avg_prices))

    # only int((N+k)/2) + 1 as the quorums' sizes are acceptable
    new_params = []
    for m_g, k_g, q1, q2, q3, q4 in params:
        if q1 >= math.ceil((m_g+k_g)/2) and q2 >= math.ceil((m_g+k_g)/2) and q3 >= math.ceil((m_g+k_g)/2) and q4 >= math.ceil((m_g+k_g)/2):
            new_params.append([m_g, k_g, q1, q2, q3, q4])
    params = new_params

    M, K = 0, 0
    storage_cost, vm_cost = 0, 0
    for m_g, k_g, q1, q2, q3, q4 in params:
        # May pre-compute this. (though itertool is optimized)
        # Get possible combination of DCs (of size m) from the set of DCs
        # possible_dcs = combinations(dc_ids, m_g)
        possible_dcs = [cheapest_dcs(m_g, cost_dc_list_avg)]
        for dcs in possible_dcs:
            get_lat = 0
            put_lat = 0
            _get_latencies = []
            _put_latencies = []
            combination = []
            _get_cost = 0
            _put_cost = 0
            full_placement_info = []
            #possible_cost_list = [elem for elem in cost_dc_list if elem[0] in dcs]
            # print("start")
            for datacenter in datacenters:
                # Get possible combination of DCs (of size q1 and q2) from the
                # m-sized set of DCs.
                # cost_list = [(elem[0], elem[1][datacenter.id]) for elem in cost_dc_list if elem[0] in dcs]
                # cost_list.sort(key=lambda x: x[1])
                _iq1 = cheapest_dcs(q1, cost_dc_list_avg) #[l[0] for l in cost_list[:q1]]
                _iq2 = cheapest_dcs(q2, cost_dc_list_avg) #[l[0] for l in cost_list[:q2]]
                _iq3 = cheapest_dcs(q3, cost_dc_list_avg) #[l[0] for l in cost_list[:q3]]
                _iq4 = cheapest_dcs(q4, cost_dc_list_avg) #[l[0] for l in cost_list[:q4]]
                # Check if the selection meets latency constraints
                i = int(datacenter.id)

                this_client_get_latency = max([datacenter.latencies[j] for j in _iq1]) + \
                                            max([datacenter.latencies[k] for k in _iq4])
                this_client_put_latency = max([datacenter.latencies[j] for j in _iq1]) + \
                                        max([datacenter.latencies[k] for k in _iq2]) + \
                                            max([datacenter.latencies[m] for m in _iq3])

                if group.client_dist[i] != 0:
                    _get_latencies.append(max([datacenter.latencies[j] for j in _iq1]) + \
                                            max([datacenter.latencies[k] for k in _iq4]))
                    _put_latencies.append(max([datacenter.latencies[j] for j in _iq1]) + \
                                        max([datacenter.latencies[k] for k in _iq2]) + \
                                            max([datacenter.latencies[m] for m in _iq3]))

                this_client_get_cost = group.metadata_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                        (group.object_size / k_g) * sum([datacenters[k].network_cost[i] for k in _iq4])
                this_client_put_cost = group.metadata_size * sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                        group.metadata_size * sum([datacenters[i].network_cost[k] for k in _iq3]) + \
                                        group.object_size / k_g * sum([datacenters[i].network_cost[m] for m in _iq2])

                # print("obj size:", group.object_size / k_g, "sum of network: ", sum([datacenters[k].network_cost[i] for k in _iq4]), "q4=", _iq4)

                _get_cost += group.client_dist[i] * \
                                (group.metadata_size*sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                    (group.object_size/k_g)*sum([datacenters[k].network_cost[i] for k in _iq4]))
                _put_cost += group.client_dist[i] * \
                                (group.metadata_size*(sum([datacenters[j].network_cost[i] for j in _iq1]) + \
                                                        sum([datacenters[i].network_cost[k] for k in _iq3])) + \
                                    (group.object_size/k_g)*sum([datacenters[i].network_cost[m] for m in _iq2]))

                combination.append([dcs, _iq1, _iq2, _iq3, _iq4])
                full_placement_info.append([dcs, _iq1, _iq2, _iq3, _iq4, this_client_get_cost, this_client_put_cost, this_client_get_latency, this_client_put_latency])

            get_lat = max(_get_latencies)
            put_lat = max(_put_latencies)
            #debugging
            # dcs_new_list = list(dcs)
            # dcs_new_list.sort()
            # if k_g == 4 and dcs_new_list == [0,1,2,5,7,8] and q1 == 2 and q2 == 5 and q3 == 5 and q4 == 5:
            #     get_cost = group.read_ratio * group.arrival_rate * _get_cost
            #     put_cost = group.write_ratio * group.arrival_rate * _put_cost
            #     _storage_cost = group.num_objects * sum([datacenters[i].details["storage_cost"] / (730 * 3600) \
            #                                              for i in dcs]) * (group.object_size / k_g)
            #     _vm_cost = sum([datacenters[i].details["price"] / 3600 for i in dcs])
            #     tot = (get_cost + put_cost + _storage_cost + _vm_cost) * 3600
            #     print("get_cost: " + str(get_cost) + ", put_cost: " + str(put_cost) + ", _storage_cost:" + str(_storage_cost) + " _vm_cost: ", str(_vm_cost))
            #     print("tot: " + str(tot))
            if get_lat < group.slo_read and put_lat < group.slo_write:
                get_cost = group.read_ratio*group.arrival_rate*_get_cost
                put_cost = group.write_ratio*group.arrival_rate*_put_cost
                _storage_cost = group.num_objects*sum([datacenters[i].details["storage_cost"]/(730*3600) \
                                                        for i in dcs])*(group.object_size/k_g)
                # _vm_cost = group.arrival_rate / 600. * sum([datacenters[i].details["price"]/3600 for i in dcs])
                _vm_cost = compute_vm_cost(group, datacenters, full_placement_info)
                tot = (get_cost+put_cost+_storage_cost+_vm_cost)*3600
                #print(m_g, k_g, get_cost*3600, put_cost*3600, _storage_cost*3600, _vm_cost*3600, tot)
                if group.duration*(get_cost+put_cost+_storage_cost+_vm_cost) < mincost:
                    # print("chosen")
                    mincost = group.duration*(get_cost+put_cost+_storage_cost+_vm_cost)
                    min_get_cost, min_put_cost = get_cost, put_cost
                    storage_cost, vm_cost = _storage_cost, _vm_cost
                    read_lat, write_lat = get_lat, put_lat
                    selected_placement = combination
                    selected_full_placement_info = full_placement_info
                    M, K = m_g, k_g
                    # print(selected_full_placement_info)
    # Calculate other costs
    if selected_placement is None:
        # return (M, K, None, selected_full_placement_info, read_lat, write_lat, \
        #         min_get_cost, min_put_cost, storage_cost, vm_cost)
        print("WARN: No placement with the heuristic min cost was found.")
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
    return (M, K, selected_dcs, selected_full_placement_info, read_lat, write_lat, \
                min_get_cost, min_put_cost, storage_cost, vm_cost)