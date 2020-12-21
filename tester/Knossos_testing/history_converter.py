#!/usr/bin/python3

import os
import glob

class Operation:
    def __init__(self, client_id, operation, key, value, start_time, finish_time):
        self.client_id = client_id
        self.operation = operation
        self.key = key
        self.value = value
        self.start_time = start_time
        self.finish_time = finish_time

    def get_key(self):
        return self.key

    def get_start_time(self):
        return self.start_time

    def get_finish_time(self):
        return self.finish_time

    def get_invoke_string(self):
        ret = "{:process " + str(self.client_id) + ", :type :invoke, :f ";
        if (self.operation == "get"):
            ret += ":read, "
        else:
            ret += ":write, "

        ret += ":value "

        if (self.operation == "get"):
            ret += "nil"
        else:
            if (self.value == "__Uninitiliazed"):
                ret += "nil"
            else:
                ret += self.value

        ret += "}"

        return ret

    def get_response_string(self):
        ret = "{:process " + str(self.client_id) + ", :type :ok, :f ";
        if (self.operation == "get"):
            ret += ":read, "
        else:
            ret += ":write, "

        ret += ":value "

        if (self.value == "__Uninitiliazed"):
            ret += "nil"
        else:
            ret += self.value

        ret += "}"

        return ret


operation_list = {}


def add_operation_in_the_right_place(op):
    if op.get_key() not in operation_list:
        operation_list[op.get_key()] = []

    operation_list[op.get_key()].append((op.get_start_time(), op.get_invoke_string()))
    operation_list[op.get_key()].append((op.get_finish_time(), op.get_response_string()))


# diraddr = input()
diraddr = "../../logs/"

for filename in os.listdir(diraddr):
    with open(os.path.join(diraddr, filename), 'r') as f:  # open in readonly mode
        for line in f.readlines():
            words = line.split(",")
            newwords = []
            for word in words:
                if (word[0] == ' '):
                    newwords.append(word[1:])
                else:
                    newwords.append(word)
            if not newwords[3]:
                print("WARN: An operation does not have any value in file " + filename)
            op = Operation(int(newwords[0]), newwords[1], newwords[2], newwords[3], int(newwords[4]), int(newwords[5]))
            add_operation_in_the_right_place(op)


files = glob.glob("./converted_logs/*")
for f in files:
    os.remove(f)

for key in operation_list:
    operation_list[key].sort()
    f = open("converted_logs/" + key + ".edn", "w")
    f.write("[")
    counter = 0
    for op in operation_list[key]:
        # print(str(op[0]))
        # print(": ")
        if (counter == 0):
            f.write(op[1])
            f.write("\n")
        elif(counter != len(operation_list[key]) - 1):
            f.write(" ")
            f.write(op[1])
            f.write("\n")
        else:
            f.write(" ")
            f.write(op[1])
            f.write("]\n")

        counter += 1

    f.close()

