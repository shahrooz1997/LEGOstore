import socket
import pickle
import threading
import copy
import json


class MetadataServer:

    def __init__(self, sock=None):
        if not sock:
            self.sock = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.sock = sock

        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('0.0.0.0', 30000))
        # Max can handles 1024 connections at one time, can increase it if required
        self.sock.listen(1024)

        self.data = {}
        self.server_list = []
        self.timeout = 0
        self.lock = threading.Lock()


    def _get_metadata(self, key, lock, server_info, output, cv):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server_info["host"], server_info["port"]))
        data = {"method": "get_metadata_internal",
                "key": key}

        sock.send(pickle.dumps(data))
        sock.settimeout(self.timeout)

        try:
            data = sock.recv(64000)
        except:
            print("Metadata Server with host: {1} and port: {2} timeout for get metadata request",
                  (server_info["host"],
                   server_info["port"]))
            return
        else:
            data = json.loads(data.decode("utf-8"))
            if data["status"] == "OK":
                lock.acquire()
                output.append(pickle.loads(data))
                lock.release()
                cv.notify()

        return


    def get_metadata(self, key):

        # Step1: Check if locally available
        if key in self.data:
            return {"status": "OK", "value": self.data[key]}

        # Step2: Check if available globally
        cv = threading.Condition()
        lock = threading.Lock()

        cv.acquire()
        thread_list = []
        output = []

        for metadata_server in self.server_list:
            thread_list.append(threading.Thread(target=self._get_metadata, args=(key,
                                                                        lock,
                                                                        copy.deepcopy(metadata_server),
                                                                        output,
                                                                        cv)))
            thread_list[-1].deamon = True
            thread_list[-1].start()

        cv.wait(timeout=self.timeout)

        lock.acquire()
        if not output:
            return {"status": "Failed", "message": "Timeout error"}
        result = output[0]
        lock.release()

        self.set_metadata(key, result)

        return result


    def get_internal_metadata(self, key):
        if key in self.data:
            return {"status": "OK", "value": self.data["key"] }

        return {"status": "Failed", "message": "NOT FOUND"}


    def set_metadata(self, key, metadata):
        self.lock.acquire()
        self.data[key] = metadata
        self.lock.release()

        return {"status": "OK"}


def server_connection(connection, metaserver):
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

    if method == "get_metadata":
        connection.sendall(json.dumps(metaserver.get_metadata(data["key"])).encode("utf-8"))
    elif method == "set_metadata":
        connection.sendall(json.dumps(metaserver.set_metadata(data["key"], data["value"])).encode("utf-8"))
    elif method == "get_metadata_internal":
        connection.sendall(json.dumps(metaserver.get_metadata_internal(data["value"])).encode("utf-8"))
    elif method:
        connection.sendall(json.dumps({"status": "Failed",
                                       "message": "MethodNotFound: Unknown method is called"}).encode("utf-8"))

    connection.close()
    return


if __name__ == "__main__":
    meta_server = MetadataServer()

    while 1:
        connection, address = meta_server.sock.accept()
        cthread = threading.Thread(target=server_connection, args=(connection, meta_server,))
        cthread.deamon = True
        cthread.start()