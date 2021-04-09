#!/usr/bin/python3

import statistics

def main():
    directory = "pairwise_latencies/"

    for i in range(9):
        # print(i, end=" ")
        for j in range(9):
            file_name = directory + "lats_from_server_" + str(i) + "c_to_server_" + str(j) + ".txt"
            file = open(file_name, "r")
            lines = file.readlines()
            times = []
            for line in lines[1:]:
                if line == "\n":
                    break
                time_word = line.split()[6]
                time = float(time_word[time_word.find("=") + 1:])
                times.append(time)
            file.close()

            # print(str(i) + " to " + str(j) + ":", statistics.mean(times), statistics.stdev(times))
            print(statistics.mean(times), statistics.stdev(times), end=" ")
        print()


if __name__ == "__main__":
    main()