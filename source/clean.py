import os
os.system("rm output_* individual_times_* socket_times_* coding_times_* timestamp_order.json insert.log")
ans = input("delete merged files? y/n ")
if ans == 'y':
	os.system("rm *_individual_times.txt *_socket_times.txt *_coding_times.txt *_output.txt")
