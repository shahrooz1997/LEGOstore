#!/usr/bin/env python3

import socket
import threading
import json
import time
import struct
import sys
import os
from reader_writer_lock import ReadWriteLock
from cache import Cache
from ABD_Server import ABD_Server
from garbage_collector import garbage_collector
from persistent import Persistent
#from viveck_1_server import Viveck_1Server
from CAS_Server import CAS_Server

import psutil

ENC_SCHM = "latin-1"
DELIMITER = "+:--:+"


import signal

def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    json.dump(CAS_Server.timeorder_log, open("timeorder.json","w"), indent=4)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)



def thread_cpu_mem_info(period=600):
    while True:
        time.sleep(period)
        mem_util = psutil.virtual_memory()._asdict()["percent"]
        cpu_util = psutil.cpu_percent()
        msg = str(cpu_util) + ":" + str(mem_util)
        os.system("echo '" + msg + "' >> util.log")

class DataServer:
    def __init__(self, db, sock=None, enable_garbage_collector = False):
        if not sock:
            self.sock = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.sock = sock

        # TODO: Remove this once the testing is done
        #self.sock.bind(('0.0.0.0', 10000))

        # Max can handles 2048 connections at one time, can increase it if required
        self.sock.listen(20048)
        # Change cache size here if you want. Ideally there should be a config for it
        # If more configurable variable are there add config and read from it.
        self.cache = Cache(500000000)
        self.persistent = Persistent(db)
        self.lock = ReadWriteLock()
        self.enable_garbage_collector = enable_garbage_collector

        self.encoding_scheme = "latin-1"
        
        self.GC = True

        if self.GC:
            threading.Thread(target=self.garbage_collector_thread,
                             args = (self.cache.cache,
                                     self.lock,
                                     )).start()


    def get(self, key, timestamp, current_class, value_required):
        '''
        Get call to the server

        :param key:
        :param current_class:
        :return:
        raises NotImplementedError if the class is not found
        '''

        if current_class == "ABD":
            return ABD_Server.get(key, timestamp, self.cache, self.persistent, self.lock)
        elif current_class == "Viveck_1":
            output = CAS_Server.get(key, timestamp, self.cache, self.persistent, self.lock, value_required)
            
            if output["value"] == None:
                output["value"] = "None"

            return output["status"] + "+:--:+" + output["value"]

        raise NotImplementedError


    def get_timestamp(self, key, current_class):
        '''
        Get_timestamp call to the server

        :param key:
        :param current_class:
        :return:
        raises NotImplementedError if the class is not found
        '''
        if current_class == "ABD":
            return ABD_Server.get_timestamp(key, self.cache, self.persistent, self.lock)
        elif current_class == "Viveck_1":
            return CAS_Server.get_timestamp(key, self.cache, self.persistent, self.lock)

        raise NotImplementedError


    def garbage_collect(self, lock, cache, persistent):
        '''
        Wrapper for calling garbage collector.
        It waits 30 seconds after garbage collection before the next one is called
        :param lock:
        :param cache:
        :param persistent:
        :return:
        '''
        garbage_collector(lock, cache, persistent)
        time.sleep(5)
        self.lock.acquire_write()
        self.enable_garbage_collector = True
        self.lock.release_write()


    def put(self, key, value, timestamp, current_class):
        '''
        Put call to the server

        :param key:
        :param value:
        :param timestamp:
        :param current_class:
        :return:
        raises NotImplementedError if the class is not found
        '''
        if self.cache.get_current_size() > 100:
            self.lock.acquire_write()
            if self.enable_garbage_collector == True:
                self.enable_garbage_collector = False
                thr = threading.Thread(target=self.garbage_collect, args=(self.lock, self.cache, self.persistent))
                thr.deamon = True
                thr.start()

            self.lock.release_write()

        if current_class == "ABD":
            return ABD_Server.put(key, value, timestamp, self.cache, self.persistent, self.lock)
        elif current_class == "Viveck_1":
            return CAS_Server.put(key, value, timestamp, self.cache, self.persistent, self.lock)

        raise NotImplementedError


    def put_fin(self, key, timestamp, current_class):
        '''
        Put fin call to the server

        :param key:
        :param value:
        :param timestamp:
        :param current_class:
        :return:
        raises NotImplementedError if the class is not found
        '''

        if current_class == "Viveck_1":
            return CAS_Server.put_fin(key, timestamp, self.cache, self.persistent, self.lock)

        raise NotImplementedError


    def garbage_collector_thread(self, cache, lock, period=600):
        while True:
            time.sleep(6)
            lock.acquire_write()
            print("garbage_collecting...")
            list_of_keys = [i for i in cache.keys() if ':' not in i]
            latest_versions = []
            for key in list_of_keys:
                latest_fin = cache.get(key)[1]
                if latest_fin is not None:
                    latest_versions.append(key + ":" + latest_fin)
            for key in cache.keys():
                if '-' in key and key not in latest_versions:
                    del cache[key]
            print("end garbage_collecting...")
            lock.release_write()
            
            #time.sleep(period)
            #print("garbage collecting...")
            



def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def recvall(sock, n):
#    fragments = []
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data
#    while True:
#        chunck = sock.recv(6400)
#        print("reched chinch is :" + str(chunck))
#        if not chunck:
#            break
#        fragments.append(chunck)
#
#    print("reached the final point================================>")
#    return b''.join(fragments)

def server_connection(connection, dataserver):
    #data = recvall(connection)
    #data = connection.recv(6400)
    data = recv_msg(connection)

    
    if not data:
        connection.sendall(json.dumps({"status": "failure", "message": "No data Found"}).encode("latin-1"))
    try:
        data = data.decode("latin-1")
        data_list = data.split("+:--:+")
    except Exception as e:
        connection.sendall(json.dumps({"status": "failure", "message": "sevre1: unable to parse data:" + str(e)}).encode("latin-1"))
        connection.close()
        return

    method = data_list[0]
    try:
        if method == "put":
            output = dataserver.put(data_list[1], data_list[2], data_list[3], data_list[4])
            connection.sendall(json.dumps(output).encode(ENC_SCHM))

        elif method == "get":
            required_value = False
            if len(data_list) > 4 and data_list[4] == "True":
                required_value = True
            output = dataserver.get(data_list[1], data_list[2], data_list[3], required_value)
            connection.sendall(output.encode(ENC_SCHM))
        elif method == "get_timestamp":
            output = dataserver.get_timestamp(data_list[1], data_list[2])
            connection.sendall(json.dumps(output).encode(ENC_SCHM))
        elif method == "put_fin":
            output = dataserver.put_fin(data_list[1],data_list[2],data_list[3])
            connection.sendall(json.dumps(output).encode(ENC_SCHM))
        elif method:
            connection.sendall("MethodNotFound: Unknown method is called".encode("latin-1"))
    except socket.error:
        pass

    connection.close()
    return


def test(data_server):
    while 1:
        connection, address = data_server.sock.accept()
        cthread = threading.Thread(target=server_connection, args=(connection, data_server,))
        cthread.deamon = True
        cthread.start()


if __name__ == "__main__":

    # For purpose of testing the whole code
    socket_port = [10000]
    db_list = ["db.temp"]

    #local testing only 
    #socket_port = [10000,10001,10002,10003,10004,10005,10006,10007,10008]
    #db_list = ["db1.temp","db2.temp","db3.temp","db4.temp","db5.temp","db6.temp","db7.temp","db8.temp","db9.temp"]
    os.system("rm util.log")
    threading.Thread(target= thread_cpu_mem_info, args=()).start()
    
    socket_list = []
    data_server_list = []
    for index, port in enumerate(socket_port):
        socket_list.append(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
        socket_list[-1].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        socket_list[-1].bind(('0.0.0.0', port))
        data_server_list.append(DataServer(db_list[index], socket_list[-1]))

    # For purpose of testing only
    thread_list = []
    for data_server in data_server_list:
        thread_list.append(threading.Thread(target=test, args=(data_server,)))
        thread_list[-1].deamon = True
        thread_list[-1].start()


