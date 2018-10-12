import os
import json



if __name__ == '__main__':
    properties = json.load(open("client_config.json"))
    if properties["classes"]["default_class"] == "ABD":
        os.system("rm calculated_latencies.txt output_* individual_times_* socket_times_* coding_times_* timestamp_order.json insert.log")
    else:
        os.system("rm calculated_latencies.txt output_* individual_times_* socket_times_* coding_times_* timestamp_order.json insert.log")
    
    ans = input("delete merged files? y/n ")
    if ans == 'y':
	    os.system("rm *_individual_times.txt *_socket_times.txt *_coding_times.txt *_output.txt")
