
from collections import OrderedDict

# #version 3
# availability_targets = [1, 2]
# client_dists = OrderedDict([
#     ("dist_O", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1]),
#     ("dist_LA", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 0.0]),
#     ("dist_LO", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.5]),
#     ("dist_L", [0.0, 0.0, 0.0, 0.0, 1, 0.0, 0.0, 0.0, 0.0]),
#     ("dist_F", [0.0, 0.0, 0.0, 1, 0.0, 0.0, 0.0, 0.0, 0.0]),
#     ("dist_LF", [0.0, 0.0, 0.0, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0]),
#     ("dist_T", [1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
#     # ("uniform", [1/9 for _ in range(9)]),
#     # ("cheap_skewed2", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
#     #("skewed_single", [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025]),
#     #("skewed_double", [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]),
#     # ("cheap_skewed", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
#     # ("expensive_skewed", [0.025, 0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.025])
#     # ("Japan_skewed", [0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.0, 0.05, 0.025])
# ])
# object_sizes = OrderedDict([("1KB", 1*2**10 / 2**30), ("10KB", 10*2**10 / 2**30), ("100KB", 100*2**10 / 2**30)])
# arrival_rates = [50, 200, 500]
# read_ratios = OrderedDict([("HW", 0.1), ("RW", 0.5), ("HR", 0.9)])
# SLO_latencies = [100, 200, 300, 500]
#
# # Default values
# availability_target_default  = 2
# client_dist_default          = [1/9 for _ in range(9)]
# object_size_default          = 1*2**10 / 2**30 # in GB
# metadata_size_default        = 11 / 2**30 # in GB
# num_objects_default          = 10000000
# arrival_rate_default         = 350
# read_ratio_default           = 0.5
# write_ratio_default          = float("{:.2f}".format(1 - read_ratio_default))
# SLO_read_default             = 1000000
# SLO_write_default            = 1000000
# duration_default             = 3600
# time_to_decode_default       = 0.00028
#
# # object_size_name = {10 / 2**30: "1B", 1*2**10 / 2**30: "1KB", 10*2**10 / 2**30: "10KB", 100*2**10 / 2**30: "100KB"}
#
# executions = [OrderedDict([("optimized", ""), ("baseline_fixed_ABD", "-b -t abd -m 3"), ("baseline_fixed_CAS", "-b -t cas -m 6 -k 4"), ("baseline_replication_based", "-b -t replication"), ("baseline_ec_based", "-b -t ec")]),
#              OrderedDict([("optimized", ""), ("baseline_fixed_ABD", "-b -t abd -m 5"), ("baseline_fixed_CAS", "-b -t cas -m 8 -k 4"), ("baseline_replication_based", "-b -t replication"), ("baseline_ec_based", "-b -t ec")])]



#version 1
availability_targets = [1, 2]
client_dists = OrderedDict([
    ("dist_O", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1]),
    ("dist_LA", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 0.0]),
    ("dist_LO", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.5]),
    ("dist_L", [0.0, 0.0, 0.0, 0.0, 1, 0.0, 0.0, 0.0, 0.0]),
    ("dist_F", [0.0, 0.0, 0.0, 1, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ("dist_LF", [0.0, 0.0, 0.0, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0]),
    ("dist_T", [1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    # ("uniform", [1/9 for _ in range(9)]),
    # ("cheap_skewed2", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
    #("skewed_single", [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025]),
    #("skewed_double", [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]),
    # ("cheap_skewed", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
    # ("expensive_skewed", [0.025, 0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.025])
    # ("Japan_skewed", [0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.0, 0.05, 0.025])
])
object_sizes = OrderedDict([("1KB", 1*2**10 / 2**30), ("10KB", 10*2**10 / 2**30), ("100KB", 100*2**10 / 2**30)])
arrival_rates = [50, 100, 200, 350, 500]
read_ratios = OrderedDict([("HW", 0.1), ("RW", 0.5), ("HR", 0.9)])
SLO_latencies = [1000]

# Default values
availability_target_default  = 2
client_dist_default          = [1/9 for _ in range(9)]
object_size_default          = 1*2**10 / 2**30 # in GB
metadata_size_default        = 11 / 2**30 # in GB
num_objects_default          = 10000000
arrival_rate_default         = 350
read_ratio_default           = 0.5
write_ratio_default          = float("{:.2f}".format(1 - read_ratio_default))
SLO_read_default             = 1000000
SLO_write_default            = 1000000
duration_default             = 3600
time_to_decode_default       = 0.00028

# object_size_name = {10 / 2**30: "1B", 1*2**10 / 2**30: "1KB", 10*2**10 / 2**30: "10KB", 100*2**10 / 2**30: "100KB"}

executions = [OrderedDict([("optimized", ""), ("baseline_fixed_ABD", "-b -t abd -m 3"), ("baseline_fixed_CAS", "-b -t cas -m 6 -k 4"), ("baseline_replication_based", "-b -t replication"), ("baseline_ec_based", "-b -t ec")]),
             OrderedDict([("optimized", ""), ("baseline_fixed_ABD", "-b -t abd -m 5"), ("baseline_fixed_CAS", "-b -t cas -m 8 -k 4"), ("baseline_replication_based", "-b -t replication"), ("baseline_ec_based", "-b -t ec")])]


#version 2
# availability_targets = [1, 2]
# client_dists = OrderedDict([
#     # ("uniform", [1/9 for _ in range(9)]),
#     # ("cheap_skewed2", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
#     #("skewed_single", [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025]),
#     #("skewed_double", [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]),
#     ("cheap_skewed", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
#     ("expensive_skewed", [0.025, 0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.025])
#     # ("Japan_skewed", [0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.0, 0.05, 0.025])
# ])
# object_sizes = OrderedDict([("1KB", 1*2**10 / 2**30), ("10KB", 10*2**10 / 2**30), ("100KB", 100*2**10 / 2**30)])
# arrival_rates = [200, 350, 500]
# read_ratios = OrderedDict([("HW", 0.1), ("RW", 0.5), ("HR", 0.9)])
# SLO_latencies = [500, 750, 1000]
#
# # Default values
# availability_target_default  = 2
# client_dist_default          = [1/9 for _ in range(9)]
# object_size_default          = 1*2**10 / 2**30 # in GB
# metadata_size_default        = 11 / 2**30 # in GB
# num_objects_default          = 100000000
# arrival_rate_default         = 350
# read_ratio_default           = 0.5
# write_ratio_default          = float("{:.2f}".format(1 - read_ratio_default))
# SLO_read_default             = 1000000
# SLO_write_default            = 1000000
# duration_default             = 3600
# time_to_decode_default       = 0.00028
#
# # object_size_name = {10 / 2**30: "1B", 1*2**10 / 2**30: "1KB", 10*2**10 / 2**30: "10KB", 100*2**10 / 2**30: "100KB"}
#
# executions = [OrderedDict([("optimized", ""), ("baseline_fixed_ABD", "-b -t abd -m 3"), ("baseline_fixed_CAS", "-b -t cas -m 6 -k 4"), ("baseline_replication_based", "-b -t replication"), ("baseline_ec_based", "-b -t ec")]),
#              OrderedDict([("optimized", ""), ("baseline_fixed_ABD", "-b -t abd -m 5"), ("baseline_fixed_CAS", "-b -t cas -m 8 -k 4"), ("baseline_replication_based", "-b -t replication"), ("baseline_ec_based", "-b -t ec")])]
