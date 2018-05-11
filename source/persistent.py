import rocksdb

class Persistent:
    def __init__(self, directory):
        self.directory = directory
        # If you wanna use something else modify as per convinience
        # Just make sure it supports get and put rest all is FINE!!! :-D
        self.db = rocksdb.DB(self.directory, rocksdb.Options(create_if_missing=True))


    def get(self, key):
        data = self.db.get(str(key).encode())

        if data:
            return eval(data.decode())

        return [None]


    def put(self, key, value):
        return self.db.put(str(key).encode(), str(value).encode())


    def put_in_batch(self, batch):
        for key, value in batch:
            self.db.put(str(key).encode(), str(value).encode())

        return
