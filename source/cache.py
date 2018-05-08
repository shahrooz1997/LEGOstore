from lru import LRU
from size import total_size
from garbage_collector import garbage_collector

class Cache:
    def __init__(self, size):
        # Intialize with a
        self.cache = LRU(100000000)
        self.current_size = 0

        # Converting size to Bytes
        self.size = size

    def put(self, key, value):
        data = self.cache.get(key)

        if data:
            item_size = total_size(value) - total_size(data)
        else:
            item_size = total_size(key) + total_size(value)

        evicted_values = []
        # If we exceed the size we need to evict the previous value and insert new value
        while self.current_size + item_size >  self.size:
            data = self.cache.pop_last_item()
            self.current_size -= total_size(data)
            evicted_values.append(data)

        self.cache[key] = value
        self.current_size += item_size

        return evicted_values


    def get(self, key):
        return self.cache.get(key)

    def get_current_size(self):
        return self.current_size


    def keys(self):
        return self.cache.keys()


