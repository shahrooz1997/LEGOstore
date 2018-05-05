import socket
import threading
import json

from reader_writer_lock import ReadWriteLock
from cache import Cache
from abd_server_protocol import ABDServer
from garbage_collector import garbage_collector
from persistent import Persistent
from viveck_1_server import Viveck_1Server


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
        self.sock.listen(2048)
        self.lock = ReadWriteLock()
        # Change cache size here if you want. Ideally there should be a config for it
        # If more configurable variable are there add config and read from it.
        self.cache = Cache(1000)
        self.persistent = Persistent(db)
        self.lock = ReadWriteLock()

        if enable_garbage_collector:
            threading.Thread(target=garbage_collector, args=(self.lock, self.cache, self.persistent))


    def get(self, key, timestamp, current_class):
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
            return Viveck_1Server.get(key, timestamp, self.cache, self.persistent, self.lock)

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

        if current_class == "ABD":
            return ABDServer.put(key, value, timestamp, self.cache, self.persistent, self.lock)
        elif current_class == "Viveck_1":
            return Viveck_1Server.put(key, value, timestamp, self.cache, self.persistent, self.lock)

        raise NotImplementedError


def server_connection(connection, dataserver):
    data = connection.recv(64000)

    if not data:
        connection.send(json.dumps({"status": "failure", "message": "No data Found"}).encode("utf-8"))
    try:
        data = json.loads(data.decode('utf-8'))
    except Exception as e:
        connection.sendall(json.dumps({"status": "failure", "message": "unable to parse data:" + str(e)}).encode("utf-8"))
        connection.close()
        return

    method = data["method"]
    try:
        if method == "put":
            connection.sendall(json.dumps(dataserver.put(data["key"],
                                                         data["value"],
                                                         data["timestamp"],
                                                         data["class"])).encode("utf-8"))
        elif method == "get":
            connection.sendall(json.dumps(dataserver.get(data["key"],
							 data["timestamp"],
                                                         data["class"])).encode("utf-8"))
        elif method == "get_timestamp":
            connection.sendall(json.dumps(dataserver.get_timestamp(data["key"],
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
    socket_port = [10001, 10002, 20001, 20002]
    db_list = ["db.temp", "db.temp1", "db.temp2", "db.temp3"] 

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
