import sys
import math
import itertools
import constants.opt_consts as CONSTS

def combinations(iterable, r):
    return list(itertools.combinations(iterable, r))

def gen_abd_params(N, f, K=None, M=None, only_EC=False):
    """ Generate possible values of n, q1, q2
    """
    quorum_params = []
    quorum_params_append = quorum_params.append
    if M is not None:
        n = M
        for q1 in range(math.ceil((n-1)/2), n-f+1):
            for q2 in range(math.ceil((n-1)/2), n-f+1):
                if q1+q2 > n:
                    quorum_params_append((n, q1, q2))

    else:
        for n in range(2*f+1, N+1):
            for q1 in range(math.ceil((n-1)/2), n-f+1):
                for q2 in range(math.ceil((n-1)/2), n-f+1):
                    if q1+q2 > n:
                        quorum_params_append((n, q1, q2))
    return quorum_params

def gen_cas_params(N, f, K=None, M=None, only_EC=False):
    """ Generates all the possible values of n, k, q1, q2, q3, q4 
        for a given value of N = No. of DCs and f = availability target
    """
    quorum_params = []
    quorum_params_append = quorum_params.append
    if M is not None:
        m = M
        min_k = 2 if only_EC else 1
        Ks = range(min_k, m)
        if K is not None:
            Ks = [K]

        for k in Ks:
            for q1 in range(math.ceil((m-k)/2), m-f+1):
                for q2 in range(math.ceil((m-k)/2), m-f+1):
                    for q3 in range(math.ceil((m-k)/2), m-f+1):
                        for q4 in range(math.ceil((m-k)/2), m-f+1):
                            if q1 + q3 > m and q1 + q4 > m and  q2 + q4 >= m + k and q4 > k:
                                quorum_params_append([m, k, q1, q2, q3, q4])

    else:
        for m in range(f+1, N+1):
            k_range = K+1 if K is not None and K<m else m
            min_k = 2 if only_EC else 1
            for k in range(min_k, k_range):
                for q1 in range(math.ceil((m-k)/2), m-f+1):
                    for q2 in range(math.ceil((m-k)/2), m-f+1):
                        for q3 in range(math.ceil((m-k)/2), m-f+1):
                            for q4 in range(math.ceil((m-k)/2), m-f+1):
                                if q1 + q3 > m and q1 + q4 > m and  q2 + q4 >= m + k and q4 > k:
                                    quorum_params_append([m, k, q1, q2, q3, q4])
    return quorum_params

def generate_placement_params(N, f, protocol, k=None, m=None, only_EC=False):
    """ Generate quorum params based on protocol
    """
    return eval(CONSTS.GEN_PARAM_FUNC[protocol])(N, f, k, m, only_EC)

def compute_vm_cost(group, datacenters, full_placement_info):
    ret = 0
    selected_dcs = full_placement_info[0][0]
    # used_vm = [False for _ in selected_dcs]

    for datacenter in datacenters:
        i = int(datacenter.id)
        arrival_rate = group.arrival_rate * group.client_dist[i]
        if arrival_rate == 0:
            continue
        selected_dcs, q1, q2, q3, q4 = full_placement_info[i][0:5]
        num_of_use_vm = [0 for _ in selected_dcs]
        for id in q1 + q2 + q3 + q4:
            num_of_use_vm[selected_dcs.index(id)] += 1
            # used_vm[selected_dcs.index(id)] = True

        for i, id in enumerate(selected_dcs):
            ret += arrival_rate * num_of_use_vm[i] / 600 * (datacenters[id].details["price"] / 3600)

    # for i, in_use in enumerate(used_vm):
    #     if not in_use:
    #         ret += datacenters[selected_dcs[i]].details["price"] / 3600

    return ret

