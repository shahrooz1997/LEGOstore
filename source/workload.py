import random
import string

class Workload:
    def __init__(self,arrival_distribution, arrival_rate, read_ratio, write_ratio, insert_ratio,
                 initial_count, value_size):
        self.arrival_class = Arrival(arrival_rate, arrival_distribution)

        self.read_ratio = read_ratio
        self.write_ratio = write_ratio
        self.insert_ratio = insert_ratio
        # total key count
        self.key_count = initial_count
        # Size of the value generated
        self.value_size = value_size

        assert(self.read_ratio + self.write_ratio + self.insert_ratio == 1)


    def next(self):
        # Return (interarrival time, key, request_type)
        random_ratio = random.randint(0, 100) / 100.0
        popularity_distribution = random.randint(0, 100)
        if random_ratio < self.read_ratio:
            request_type = "get"
        elif random_ratio < self.read_ratio + self.write_ratio:
            request_type = "put"
        else:
            request_type = "insert"

        inter_arrival_time = self.arrival_class.next()

        if request_type == "insert":
            key = "key" + str(self.key_count)
#            self.key_count += 1
        else:
            if popularity_distribution < 90:
                key = "key" + str(int(random.randint(0, self.key_count) / 10))
            else:
                key = "key" + str(random.randint(0, self.key_count))

        value = None
        if request_type != "get":
            value = ''.join(random.choice(string.ascii_uppercase)
                            for _ in range(self.value_size))

        return (inter_arrival_time, request_type, key, value)


class Arrival:
    def __init__(self, arrival_rate, arrival_distribution):
        self.arrival_rate = arrival_rate
        self.arrival_distribution = arrival_distribution


    def next(self):
        # Returns the interarrival time in millisecond
        if self.arrival_distribution == "uniform":
            return 1000.0/self.arrival_rate
