import socket
import threading
import json
import time
import struct
from reader_writer_lock import ReadWriteLock
from cache import Cache
from abd_server_protocol import ABDServer
from garbage_collector import garbage_collector
from persistent import Persistent
#from viveck_1_server import Viveck_1Server
from viveck_1_server_1 import Viveck_1Server

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



    def get(self, key, timestamp, current_class, value_required):
        '''
        Get call to the server

        :param key:
        :param current_class:
        :return:
        raises NotImplementedError if the class is not found
        '''
        if current_class == "ABD":
            return ABDServer.get(key, timestamp, self.cache, self.persistent, self.lock)
        elif current_class == "Viveck_1":
            return Viveck_1Server.get(key, timestamp, self.cache, self.persistent, self.lock, value_required)

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
            return ABDServer.get_timestamp(key, self.cache, self.persistent, self.lock)
        elif current_class == "Viveck_1":
            return Viveck_1Server.get_timestamp(key, self.cache, self.persistent, self.lock)

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
            return ABDServer.put(key, value, timestamp, self.cache, self.persistent, self.lock)
        elif current_class == "Viveck_1":
            return Viveck_1Server.put(key, value, timestamp, self.cache, self.persistent, self.lock)

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
            return Viveck_1Server.put_fin(key, timestamp, self.cache, self.persistent, self.lock)

        raise NotImplementedError

def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    print("reached here")
    if not raw_msglen:
        return None
    
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def recvall(sock, n):
#    fragments = []
    data = b''
    print("reached here too")
    while len(data) < n:
        packet = sock.recv(n - len(data))
        print("current packet size: " + str(packet))
        if not packet:
            print("packet size " + str(packet))
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
        connection.sendall(json.dumps({"status": "failure", "message": "No data Found"}).encode("utf-8"))
    try:
        data = json.loads(data.decode('utf-8'))
    except Exception as e:
        connection.sendall(json.dumps({"status": "failure", "message": "sevre1: unable to parse data:" + str(e)}).encode("utf-8"))
        connection.close()
        return

    method = data["method"]
    print(str(data))
    try:
        if method == "put":
            connection.sendall(json.dumps(dataserver.put(data["key"],
                                                         data["value"],
                                                         data["timestamp"],
                                                         data["class"])).encode("utf-8"))
        elif method == "get":
            if "value_required" not in data:
                data["value_required"] = False
            connection.sendall(json.dumps(dataserver.get(data["key"],
                                                         data["timestamp"],
                                                         data["class"],
                                                         data["value_required"])).encode("utf-8"))
        elif method == "get_timestamp":
            connection.sendall(json.dumps(dataserver.get_timestamp(data["key"],
                                                                   data["class"])).encode("utf-8"))
        elif method == "put_fin":
            connection.sendall(json.dumps(dataserver.put_fin(data["key"],
                                                             data["timestamp"],
                                                             data["class"])).encode("utf-8")) 
        elif method:
            connection.sendall("MethodNotFound: Unknown method is called".encode("utf-8"))
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


    #
    # while 1:
    #     connection, address = data_server.sock.accept()
    #     cthread = threading.Thread(target=server_connection, args=(connection, data_server,))
    #     cthread.deamon = True
    #     cthread.start()
