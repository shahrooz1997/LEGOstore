
from collections import OrderedDict

#version 3
availability_targets = [1, 2]
client_dists = OrderedDict([
    # ("uniform", [1/9 for _ in range(9)]),
    ("dist_O", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1]),
    ("dist_LA", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 0.0]),
    ("dist_LO", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.5]),
    ("dist_T", [1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ("dist_SY", [0.0, 1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ("dist_SS", [0.0, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ("dist_ST", [0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])


    # ("dist_L", [0.0, 0.0, 0.0, 0.0, 1, 0.0, 0.0, 0.0, 0.0]),
    # ("dist_F", [0.0, 0.0, 0.0, 1, 0.0, 0.0, 0.0, 0.0, 0.0]),
    # ("dist_LF", [0.0, 0.0, 0.0, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0]),

    # ("uniform", [1/9 for _ in range(9)]),
    # ("cheap_skewed2", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
    #("skewed_single", [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025]),
    #("skewed_double", [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]),
    # ("cheap_skewed", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
    # ("expensive_skewed", [0.025, 0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.025])
    # ("Japan_skewed", [0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.0, 0.05, 0.025])
])
object_sizes = OrderedDict([("1KB", 1*2**10 / 2**30), ("10KB", 10*2**10 / 2**30), ("100KB", 100*2**10 / 2**30)])
storage_sizes = OrderedDict([("100GB", 100), ("1TB", 1024), ("10TB", 10 * 1024)]) # in GB
arrival_rates = [50, 200, 500] #[int(50 * 0.9), int(200 * 0.9), int(500 * 0.9)]
read_ratios = OrderedDict([("HW", 0.03225), ("RW", 0.5), ("HR", 0.96774)])
SLO_latencies = [200, 300]

# Default values
availability_target_default  = 2
client_dist_default          = [1/9 for _ in range(9)]
object_size_default          = 1*2**10 / 2**30 # in GB
metadata_size_default        = 100 / 2**30 # in GB
num_objects_default          = 10000000
arrival_rate_default         = 350
read_ratio_default           = 0.5
write_ratio_default          = float("{:.2f}".format(1 - read_ratio_default))
SLO_read_default             = 1000000
SLO_write_default            = 1000000
duration_default             = 3600
time_to_decode_default       = 0.00028

# object_size_name = {10 / 2**30: "1B", 1*2**10 / 2**30: "1KB", 10*2**10 / 2**30: "10KB", 100*2**10 / 2**30: "100KB"}

executions = [OrderedDict([("optimized", "-H min_cost"), ("baseline_fixed_ABD", "-H baseline0 -b -t abd -m 3"), ("baseline_fixed_CAS", "-H baseline0 -b -t cas -m 5 -k 3"), ("baseline_abd_nearest", "-H baseline1 -b -t abd -m 3"), ("baseline_cas_nearest", "-H baseline1 -b -t cas -m 5 -k 3"), ("baseline_abd", "-H min_cost -b -t abd"), ("baseline_cas", "-H min_cost -b -t cas")]),
             OrderedDict([("optimized", "-H min_cost"), ("baseline_fixed_ABD", "-H baseline0 -b -t abd -m 5"), ("baseline_fixed_CAS", "-H baseline0 -b -t cas -m 7 -k 3"), ("baseline_abd_nearest", "-H baseline1 -b -t abd -m 5"), ("baseline_cas_nearest", "-H baseline1 -b -t cas -m 7 -k 3"), ("baseline_abd", "-H min_cost -b -t abd"), ("baseline_cas", "-H min_cost -b -t cas")])]

# executions = [OrderedDict([("optimized", "-H min_cost")]),
#              OrderedDict([("optimized", "-H min_cost")])]

# executions = [OrderedDict([("baseline_abd_nearest", "-H baseline1 -b -t abd -m 3"), ("baseline_cas_nearest", "-H baseline1 -b -t cas -m 4 -k 2")]),
#              OrderedDict([("baseline_abd_nearest", "-H baseline1 -b -t abd -m 5"), ("baseline_cas_nearest", "-H baseline1 -b -t cas -m 6 -k 2")])]


# executions = [OrderedDict([("baseline_abd_nearest", "-H baseline1 -b -t abd -m 3"), ("baseline_cas_nearest", "-H baseline1 -b -t cas -m 5 -k 3")]),
#              OrderedDict([("baseline_abd_nearest", "-H baseline1 -b -t abd -m 5"), ("baseline_cas_nearest", "-H baseline1 -b -t cas -m 7 -k 3")])]

# executions = [OrderedDict([("optimized", "-H min_cost"), ("baseline_fixed_ABD", "-H baseline0 -b -t abd -m 3"), ("baseline_fixed_CAS", "-H baseline0 -b -t cas -m 5 -k 3"), ("baseline_abd", "-H min_cost -b -t abd"), ("baseline_cas", "-H min_cost -b -t cas")]),
#              OrderedDict([("optimized", "-H min_cost"), ("baseline_fixed_ABD", "-H baseline0 -b -t abd -m 5"), ("baseline_fixed_CAS", "-H baseline0 -b -t cas -m 7 -k 3"), ("baseline_abd", "-H min_cost -b -t abd"), ("baseline_cas", "-H min_cost -b -t cas")])]


# #test
# availability_targets = [1, 2]
# client_dists = OrderedDict([
#
#     ("dist_O", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1]),
#     ("dist_LA", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1, 0.0]),
#     ("dist_LO", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.5]),
#
#     # ("uniform", [1/9 for _ in range(9)]),
#     # ("cheap_skewed2", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
#     #("skewed_single", [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025]),
#     #("skewed_double", [0.45, 0.025, 0.025, 0.0, 0.0, 0.0, 0.45, 0.025, 0.025]),
#     # ("cheap_skewed", [0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.9]),
#     # ("expensive_skewed", [0.025, 0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025, 0.025])
#     # ("Japan_skewed", [0.9, 0.0, 0.0, 0.0, 0.0, 0.025, 0.0, 0.05, 0.025])
# ])
# object_sizes = OrderedDict([("10KB", 10*2**10 / 2**30)])
# # num_objects_arr = [1000000, 10000000, 100000000]
# arrival_rates = [500]
# read_ratios = OrderedDict([("HR", 0.9)])
# SLO_latencies = [1000]
#
# # Default values
# availability_target_default  = 2
# client_dist_default          = [1/9 for _ in range(9)]
# object_size_default          = 1*2**10 / 2**30 # in GB
# metadata_size_default        = 100 / 2**30 # in GB
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
# executions = [OrderedDict([("optimized", "-H min_cost"), ("baseline_fixed_ABD", "-H baseline0 -b -t abd -m 3"), ("baseline_fixed_CAS", "-H baseline0 -b -t cas -m 6 -k 4"), ("baseline_abd_nearest", "-H baseline1 -b -t abd"), ("baseline_cas_nearest", "-H baseline1 -b -t cas"), ("baseline_abd", "-H min_cost -b -t abd"), ("baseline_cas", "-H min_cost -b -t cas")]),
#              OrderedDict([("optimized", "-H min_cost"), ("baseline_fixed_ABD", "-H baseline0 -b -t abd -m 5"), ("baseline_fixed_CAS", "-H baseline0 -b -t cas -m 8 -k 4"), ("baseline_abd_nearest", "-H baseline1 -b -t abd"), ("baseline_cas_nearest", "-H baseline1 -b -t cas"), ("baseline_abd", "-H min_cost -b -t abd"), ("baseline_cas", "-H min_cost -b -t cas")])]

# #version 1
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
# arrival_rates = [50, 100, 200, 350, 500]
# read_ratios = OrderedDict([("HW", 0.1), ("RW", 0.5), ("HR", 0.9)])
# SLO_latencies = [1000]
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

distribution_full_name = OrderedDict([
    ("uniform", "All"),
    ("dist_O", "Oregon"),
    ("dist_LA", "Los Angeles"),
    ("dist_LO", "Los Angeles and Oregon"),
    ("dist_L", "London"),
    ("dist_F", "Frankfurt"),
    ("dist_LF", "London and Frankfurt"),
    ("dist_T", "Tokyo"),
    ("dist_SY", "Sydney"),
    ("dist_SS", "Sydney and Singapore"),
    ("dist_ST", "Sydney and Tokyo")
])