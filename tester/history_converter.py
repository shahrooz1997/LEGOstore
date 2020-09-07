#!/usr/bin/python3

import os

class Operation:
    def __init__(self, client_id, operation, key, value, start_time, finish_time):
        self.client_id = client_id
        self.operation = operation
        self.key = key
        self.value = value
        self.start_time = start_time
        self.finish_time = finish_time

    def get_start_time(self):
        return self.start_time

    def get_finish_time(self):
        return self.finish_time

    def get_invoke_string(self):
        ret = "{:process " + str(self.client_id) + ", :type :invoke, :f ";
        if(self.operation == "get"):
            ret += ":read, "
        else:
            ret += ":write, "

        ret += ":value "

        if(self.value == "__Uninitiliazed"):
            ret += "nil"
        else:
            ret += self.value

        ret += "}"

        return ret

    def get_response_string(self):
        ret = "{:process " + str(self.client_id) + ", :type :ok, :f ";
        if(self.operation == "get"):
            ret += ":read, "
        else:
            ret += ":write, "

        ret += ":value "

        if(self.value == "__Uninitiliazed"):
            ret += "nil"
        else:
            ret += self.value

        ret += "}"

        return ret

operation_list = []

def add_operation_in_the_right_place(op):
    invoke = op.get_invoke_string();

    operation_list.append((op.get_start_time(), invoke));

    response = op.get_response_string()

    operation_list.append((op.get_finish_time(), response));




# diraddr = input()
diraddr = "../logs/"



for filename in os.listdir(diraddr):
    with open(os.path.join(diraddr, filename), 'r') as f: # open in readonly mode
        for line in f.readlines():
            words = line.split(",")
            newwords = []
            for word in words:
                if(word[0] == ' '):
                    newwords.append(word[1:])
                else:
                    newwords.append(word)
            op = Operation(int(newwords[0]), newwords[1], newwords[2], newwords[3], int(newwords[4]), int(newwords[5]))
            add_operation_in_the_right_place(op)


operation_list.sort()

# print(operation_list)


f = open("output.txt", "w")
# f.write("Now the file has more content!")


#open and read the file after the appending:
# f = open("demofile2.txt", "r")
# print(f.read()) 

f.write("[")

flag = 0

for op in operation_list:
	# print(str(op[0]))
	# print(": ")
	if(flag == 0):
		flag = 1
		f.write(op[1])
		f.write("\n")
	else:
		f.write(" ")
		f.write(op[1])
		f.write("\n")

f.write("]\n")
f.close()