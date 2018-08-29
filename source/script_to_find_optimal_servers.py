import sys

def choose_iter(elements, length):
    for i in xrange(len(elements)):
        if length == 1:
            yield (elements[i],)
        else:
            for next in choose_iter(elements[i+1:len(elements)], length-1):
                yield (elements[i],) + next


def choose(l, k):
    return list(choose_iter(l, k))


def get_possible_combos(total_servers, failures):
    # Should be eaisly optimized if required for now go with the flow!
    possible_combos = []
    for i in range(failures + 1, total_servers + 1):
        n = i
        for k in range(1, n):
            for j in range(failures + 1, n - failures + 1):
                q1 = j

                for t in range(failures + 1, n - failures + 1):
                    q2 = t

                    for l in range(failures + 1, n - failures + 1):
                        q3 = l

                        for m in range(failures + 1, n - failures + 1):
                            q4 = m

                            if q1 + q3 > n and q1 + q4 > n and  q2 + q4 >= n + k and q4 > k:
                                possible_combos.append([n, k, q1, q2, q3, q4])

    return possible_combos

if __name__ == '__main__':

    # Possible combinations
    possible_combo = get_possible_combos(9, 2)

    # Latency in ms
    read_latency = int(raw_input("Read latency (in ms): "))
    write_latency = int(raw_input("Write latency (in ms): "))

    per_server_ratio = [1.0/9, 1.0/9, 1.0/9, 1.0/9, 1.0/9, 1.0/9, 1.0/9, 1.0/9, 1.0/9]

    # Object size in bytes # Could be an input too
    object_size = 100000.0

    # Mean arrival rate (request/second)
    mean_arrival_rate = 4

    # Total number of objects 
    total_number_of_objects = 1000000

    # In milliseconds
    data_center_latency = [
    [0,43,136,247,237,165,293,128,109],
    [43,0,169,280,261,186,319,156,144],
    [143,171,0,131,138,194,326,267,236],
    [241,269,177,0,25,90,214,148,162],
    [224,251,133,24,0,76,189,138,140],
    [161,186,194,92,79,0,126,71,83],
    [305,332,324,213,188,123,0,203,191],
    [122,150,253,149,139,68,197,0,24],
    [108,137,229,163,139,82,186,25,0]
    ]

 # #  data_center_latency = [
 # #  [0,43,136,247,237,165,175,128,109],
 # #  [43,0,169,280,261,186,208,156,144],
 # #  [143,171,0,131,138,194,207,267,236],
 # #  [241,269,177,0,25,90,99,148,162],
 # #  [224,251,133,240,76,89,138,140],
 # #  [161,186,194,92,79,0,16,71,83],
 # #  [177,208,204,104,92,18,0,59,76],
 # #  [122,150,253,149,139,68,55,0,24],
 # #  [108,137,229,163,139,82,74,25,0]
 # #  ]

    # Bandwidth in Mbps
    bandwidth = [
         [float("inf"), 76, 55, 31, 57, 37, 45, 99, 107],
         [76, float("inf"), 51, 26, 40, 31, 28, 93, 72],
         [20, 72, float("inf"), 30, 67, 28, 22, 33, 41],
         [54, 42, 48, float("inf"), 350, 44, 45, 79, 80],
         [36, 52, 64, 79, float("inf"), 97, 51, 83, 94],
         [70, 48, 58, 54, 182, float("inf"), 59, 117, 134],
         [48, 43, 39, 56, 64, 77, float("inf"), 69, 68],
         [53, 80, 42, 69, 80, 78, 64, float("inf"), 398],
         [56, 38, 46, 26, 49, 54, 29, 173, float("inf")]
    ]

    # Converting Mbps to Bytes per millisecond
    for row in range(len(bandwidth)):
        for col in range(len(bandwidth)):
            if bandwidth[row][col] != float('inf'):
                bandwidth[row][col] = bandwidth[row][col] * 125

    # Read / Write Ration (read + write == 1)
    read_percentage = 0.1
    write_percentage = 0.9

    # Time to decode the object
    # NO NEED FOR TIME TO ENCODE AS IT CAN BE CONCURRENTLY DONE WHILE GETTING LATEST TIMESTAMP 
    time_to_decode = 1

    # For sanity check purpose, we have added them.
#    data_center_latency = [
#    [0,1,2,3,4,4,3,2,1],
#    [1,0,1,2,3,4,4,3,2],
#    [2,1,0,1,2,3,4,4,3],
#    [3,2,1,0,1,2,3,4,4],
#    [4,3,2,1,0,1,2,3,4],
#    [4,4,3,2,1,0,1,2,3],
#    [3,4,4,3,2,1,0,1,2],
#    [2,3,4,4,3,2,1,0,1],
#    [1,2,3,4,4,3,2,1,0]]

    # Date center IDs
    a = [0,1,2,3,4,5,6,7,8]

    # Cost of Transferring data outside data center
    cost_list = [0.00000000009, 0.00000000008, 0.000000000086, 0.00000000002, 0.00000000002, 0.00000000002, 0.000000000016, 0.00000000002, 0.00000000002]

    # Storage cost for each DC is assumed to be same per second
    storage_cost = 0.00000000010/ (2.628 * 1000000)

    smallest_combo = []
    possible_qourum = []

    cost = sys.maxsize
    latency_read_ = sys.maxsize
    latency_write_ = sys.maxsize

    for n, k, q1, q2, q3, q4 in possible_combo:
        # Choose all possible placement of a size from n
        placement = choose(a, n)

        for values in placement:
            total_latency_q1 = 0
            total_latency_q2 = 0
            total_latency_q3 = 0
            total_latency_q4 = 0
            total_read_transfer_cost = 0
            total_write_transfer_cost = 0
            total_storage_cost = 0

            # For each DC we calculate the latency for each quorum
            for i in a:
                unsorted_list_with_index = []
                for index, v in enumerate(data_center_latency[i]):
                    unsorted_list_with_index.append((index, v))
                sorted_latency = sorted(unsorted_list_with_index, key = lambda x: x[1])

                # For q1 quorum
                temp = q1
                curr_index = 0
                while temp:
                    if sorted_latency[curr_index][0] in values:
                        temp -= 1
                        if temp == 0:
                            total_latency_q1 += (sorted_latency[curr_index][1]) * per_server_ratio[i]
                    curr_index += 1


                # For q4 quorum
                temp = q4
                curr_index = 0
                total_k = 0
                possible_cost = []
                while temp:
                    if sorted_latency[curr_index][0] in values:
                        temp -= 1
                        if sorted_latency[curr_index][0] == i:
                            possible_cost.append(0)
                        else:
                            possible_cost.append(cost_list[sorted_latency[curr_index][0]] * object_size/(k * 1.0))

                        # We dont always need a object to get from that server but we will consider worst case where we have to.
                        if temp == 0:
                            total_latency_q4 += (sorted_latency[curr_index][1] + (object_size/(k*1.0))/bandwidth[curr_index][i] + time_to_decode) * per_server_ratio[i]

                    curr_index += 1
                sorted_possible_cost = sorted(possible_cost, reverse=True)

                for _index in range(0, q4):
                    total_read_transfer_cost += sorted_possible_cost[_index] * per_server_ratio[i]

               # total_read_transfer_cost += total_read_transfer_cost * per_server_ratio[i]

                # For q2 quorum
                temp = q2
                curr_index = 0
                total_data_transfer_servers = q2
                while temp:
                    if sorted_latency[curr_index][0] in values:
                        temp -= 1
                        if sorted_latency[curr_index][0] == i:
                            total_data_transfer_servers -= 1

                        if temp == 0:
                            total_latency_q2 += (sorted_latency[curr_index][1] + (object_size/(1.0*k))/bandwidth[i][curr_index]) * per_server_ratio[i]

                    curr_index += 1

                total_write_transfer_cost += (total_data_transfer_servers * cost_list[i] * 1.0 * object_size * per_server_ratio[i])/ (k * 1.0)

                # For q3 quorum
                temp = q3
                curr_index = 0
                while temp:
                    if sorted_latency[curr_index][0] in values:
                        temp -= 1
                        if temp == 0:
                            total_latency_q3 += sorted_latency[curr_index][1] * per_server_ratio[i]

                    curr_index += 1


            # Latency and bandwodth calculations
            current_read_latency = total_latency_q1 + total_latency_q4
            current_write_latency = total_latency_q1 + total_latency_q2 + total_latency_q3

            total_read_transfer_cost = total_read_transfer_cost * read_percentage * mean_arrival_rate * 3600
            total_write_transfer_cost = total_write_transfer_cost * write_percentage * mean_arrival_rate * 3600
            total_storage_cost = storage_cost * ((n * 1.0 )/k) * object_size * total_number_of_objects * 3600

            if current_read_latency < read_latency and \
                current_write_latency < write_latency and \
                total_read_transfer_cost + total_write_transfer_cost + total_storage_cost < cost:
                latency_read_ = total_latency_q1 + total_latency_q4
                latency_write_ = total_latency_q1 + total_latency_q2 + total_latency_q3
                smallest_combo = values
                possible_qourum = [n, k, q1, q2, q3, q4]
                cost = total_read_transfer_cost + total_write_transfer_cost + total_storage_cost
#                print total_storage_cost
#                print total_read_transfer_cost
#                print total_write_transfer_cost

    print "total cost : " + str(cost) + " total latency read: " + str(latency_read_) + " total_latency_write: " + str(latency_write_) \
            +" combo is : " + str(smallest_combo) + " possibe_quorum : " + str(possible_qourum)
