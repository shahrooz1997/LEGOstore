import os
os.system("rm output_* individual_times_1* socket_times_* coding_times_* timestamp_result.json")
ans = input("delete merged files? y/n ")
if ans == 'y':
	os.system("rm 1_*")
