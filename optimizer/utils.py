import sys
import math
import itertools
import constants.opt_consts as CONSTS

def combinations(iterable, r):
    return list(itertools.combinations(iterable, r))

def gen_abd_params(N, f, K=None, M=None):
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

def gen_cas_params(N, f, K=None, M=None):
    """ Generates all the possible values of n, k, q1, q2, q3, q4 
        for a given value of N = No. of DCs and f = availability target
    """
    quorum_params = []
    quorum_params_append = quorum_params.append
    if M is not None:
        m = M
        k_range = K+1 if K is not None and K<m else m
        for k in range(1, k_range):
            for q1 in range(math.ceil((m-k)/2), m-f+1):
                for q2 in range(math.ceil((m-k)/2), m-f+1):
                    for q3 in range(math.ceil((m-k)/2), m-f+1):
                        for q4 in range(math.ceil((m-k)/2), m-f+1):
                            if q1 + q3 > m and q1 + q4 > m and  q2 + q4 >= m + k and q4 > k:
                                quorum_params_append([m, k, q1, q2, q3, q4])

    else:
        for m in range(f+1, N+1):
            k_range = K+1 if K is not None and K<m else m
            for k in range(1, k_range):
                for q1 in range(math.ceil((m-k)/2), m-f+1):
                    for q2 in range(math.ceil((m-k)/2), m-f+1):
                        for q3 in range(math.ceil((m-k)/2), m-f+1):
                            for q4 in range(math.ceil((m-k)/2), m-f+1):
                                if q1 + q3 > m and q1 + q4 > m and  q2 + q4 >= m + k and q4 > k:
                                    quorum_params_append([m, k, q1, q2, q3, q4])
    return quorum_params

def generate_placement_params(N, f, protocol, k=None, m=None):
    """ Generate quorum params based on protocol
    """
    return eval(CONSTS.GEN_PARAM_FUNC[protocol])(N, f, k, m)

