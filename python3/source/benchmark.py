import rocksdb
import time
import sys
from memcache import Client, _Host, SERVER_MAX_KEY_LENGTH, SERVER_MAX_VALUE_LENGTH
from lru import LRU
from random import choice
from string import ascii_uppercase
import random
import time

data = []
for i in range(0, 50):
	data.append(''.join(choice(ascii_uppercase) for j in range(64000)).encode())

db = rocksdb.DB("test.db", rocksdb.Options(create_if_missing=True))
end_time = 0

for i in range(1,14000):
    time.sleep(0.005)
    start_time = time.time()
    db.put(str(random.randint(0,999999)).encode(), data[random.randint(0, 49)])
    end_time += time.time() - start_time

print("time to set in rocksdb")
print(end_time)
print("-------------------")


#start_time = time.time()
#for i in range(1, 100000):
#    b = db.get(str(i).encode())
#end_time = time.time()

#print("time to get in rocksdb")
#print(end_time - start_time)
#print("-------------------")

mc = Client(['127.0.0.1:11211'], debug=0)

start_time = time.time()
for i in range(1, 14000):
    time.sleep(0.005)
    start_time = time.time()
    mc.set(str(random.randint(0,999999)).encode(), data[random.randint(0, 49)])
    end_time += time.time() - start_time

print("time to set in memcached")
print(end_time - start_time)
print("-------------------")

#
#start_time = time.time()
#for i in range(1, 100000):
#    b = mc.get(str(i).encode())
#end_time = time.time()
#
#print("time to get in memcached")
#print(end_time - start_time)
#print("-------------------")
#
#
#l = LRU(500001)
#
#start_time = time.time()
#for i in range(1, 100000):
#    l[str(i).encode()] = a
#
#end_time = time.time()
#
#print("time to set in LRU")
#print(end_time - start_time)
#print("-------------------")
#
#
#start_time = time.time()
#for i in range(1, 100000):
#    b = l[str(i).encode()]
#
#end_time = time.time()
#
#print("time to get in LRU")
#print(end_time - start_time)
#print("-------------------")
#
