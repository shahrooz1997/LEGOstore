scripts_to_input = ["testing.edn", "testing.edn"]


output_list= []
output_file = "final.edn"
output = open(output_file, "w")

for file_name in scripts_to_input:
    f = open(file_name, "r")

    for line in f:
        chunks = line.split(" ")
        output_list.append((chunks[-1][:-2], line))


output_list.sort(key=lambda x: x[0])

for data in output_list:
    output.write(data[1])