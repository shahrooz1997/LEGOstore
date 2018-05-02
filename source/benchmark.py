import rocksdb
import time
import sys
from memcache import Client, _Host, SERVER_MAX_KEY_LENGTH, SERVER_MAX_VALUE_LENGTH
from lru import LRU
from random import choice
from string import ascii_uppercase

start_time = time.time()
a = ''.join(choice(ascii_uppercase) for j in range(64000)).encode()
print(time.time() - start_time)

db = rocksdb.DB("test.db", rocksdb.Options(create_if_missing=True))

start_time = time.time()
for i in range(1,100000):
    db.put(str(i).encode(), a)
end_time = time.time()

print("time to set in rocksdb")
print(end_time - start_time)
print("-------------------")


start_time = time.time()
for i in range(1, 100000):
    b = db.get(str(i).encode())
end_time = time.time()

print("time to get in rocksdb")
print(end_time - start_time)
print("-------------------")

mc = Client(['0.0.0.0:11211'], debug=0)

start_time = time.time()
for i in range(1, 100000):
    mc.set(str(i).encode(), a)
end_time = time.time()

print("time to set in memcached")
print(end_time - start_time)
print("-------------------")

start_time = time.time()
for i in range(1, 100000):
    b = mc.get(str(i).encode())
end_time = time.time()

print("time to get in memcached")
print(end_time - start_time)
print("-------------------")


l = LRU(500001)

start_time = time.time()
for i in range(1, 100000):
    l[str(i).encode()] = a

end_time = time.time()

print("time to set in LRU")
print(end_time - start_time)
print("-------------------")


start_time = time.time()
for i in range(1, 100000):
    b = l[str(i).encode()]

end_time = time.time()

print("time to get in LRU")
print(end_time - start_time)
print("-------------------")

