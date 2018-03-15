import memcache
import threading
from threading import Thread

import hashlib

class MC_Wrapper:
    def __init__(self, properties):
        # Read the config file and setup MC router as per its settings
         self.mc = memcache.client(['127.0.0.1:5000'])
         self.number_of_servers = int(properties["quorum"]["total_nodes"])
         self.read_server = int(properties["quorum"]["read_nodes"])
         self.write_server = int(properties["quorum"]["write_nodes"])
         self.dimension = int(properties["quorum"]["dimension"])


    def _get(key, value, lock, sem,  output):
        data = self.mc.get(key, value)
        lock.acquire()
        output.append(data)
        lock.release()
        sem.wait()


    def _put(key, value, sem):
        data = self.mc.set(key, value)
        sem.wait()


    def put(key, value, dimension = None, number_of_servers = None, write_servers = None):
        # Put the value
        if not number_of_servers:
            number_of_servers = self.number_of_servers
    
        if not write_servers:
            write_servers = self.write_servers

        # Prefix for the keys to ensure storing them on different servers
        int key_prefix = int(hashlib.sha1(key).hexdigest(), 16) % number_of_servers

        sem = threading.Barrier(write_server + 1, timeout = 120)
        thread_list = []

        for i in range(0, self.number_of_servers):
            new_key = str(key_prefix) + ":" + key
            thread_list.append(threading.Thread(target=_put, args = (new_key, value, sem)))
            key_prefix = (key_prefix + 1) % number_of_servers

        sem.wait()

        #XXX: Need an implementation to safely kill other threads
        return True


    def get(key, value, dimension = None, number_of_servers = None, read_servers = None):
        # Get the value
        if not number_of_servers:
            number_of_servers = self.number_of_servers
        
        if not read_servers:
            read_servers = self.read_servers

        # Prefix for the keys to ensure storing them on different servers
        int key_prefix = int(hashlib.sha1(key).hexdigest(), 16) % number_of_servers

        sem = threading.Barrier(read_server + 1, timeout = 120)
        thread_list = []
        lock = threading.lock()

        output = []
        for i in range(0, number_of_servers):
            new_key = str(key_prefix) + ":" + key
            thread_list.append(threading.Thread(target=_get, args = (new_key, value, lock, sem, output)))
            key_prefix = (key_prefix + 1) % number_of_servers
        
        sem.wait()

        #XXX: Need an implementation to safely kill other threads
        return output[0]
