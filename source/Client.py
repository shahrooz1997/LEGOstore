import socket
import json

from protocol import Protocol
from data_center import Datacenter
import uuid
import threading


class Client:
    def __init__(self, properties):
        ''' Setting up basic properties of the server
        '''
        self.class_properties = properties["classes"]
        self.local_datacenter = properties["local_datacenter"]

        self.datacenter_list = properties["datacenters"]

        # Same LRU data structure can be used for the same but for now using dictionary
        # https://stackoverflow.com/questions/7143746/difference-between-memcache-and-python-dictionary
        self.local_cache = {}

        self.class_name_to_object = {}
        self.default_class = self.class_properties["default_class"]
        self.metadata_server = self.datacenter_list[self.local_datacenter]["metadata_server"]
        # TODO: Update Datacenter to store local datacenter too
        self.datacenter_info = Datacenter(self.datacenter_list)

        self.id = uuid.uuid4().hex
        self.retry_attempts = int(properties["retry_attempts"])
        self.metadata_timeout = int(properties["metadata_server_timeout"])

        self.initiate_key_classes()


    def initiate_key_classes(self):
        for class_name, class_info in self.class_properties.items():
            self.class_name_to_object[class_name] = Protocol.get_class_protocol(class_name,
                                                                                class_info,
                                                                                self.local_datacenter,
                                                                                self.datacenter_info,
                                                                                self.id)

        return


    def check_validity(self, output):
        # Checks if the status is OK or not
        # @ Returns true pr false based on the status

        if output["status"] == "OK":
            return True

        return False


    def look_up_metadata(self, key):
        ######################
        # Internal call for the client
        # @ This method searches the metadata(class, server_list for the key.
        # @ Returns tuple (class_name, server_list ) if exist else return (None, None)
        # @ Raises Exception in case of timeout or socket error
        ######################
        if key in self.local_cache:
            print(self.local_cache[key])
            data = self.local_cache[key]
            return (data["class_name"], data["server_list"])

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(self.metadata_server["host"], int(self.metadata_server["port"]))
        sock.connect((self.metadata_server["host"], int(self.metadata_server["port"])))

        data = json.dumps({"method": "get_metadata", "key": key})

        sock.sendall(data.encode("utf-8"))
        sock.settimeout(self.metadata_timeout)

        try:
            data = sock.recv(1024)
        except socket.error:
            raise Exception("Unbale to get value from metadata server")
        else:
            data = data.decode("utf-8")
            data = json.loads(data)
            if data["status"] == "OK":
                print("reached here")
                self.local_cache[key] = data["value"]
                print("tadaaa")
                return (data["value"]["class_name"], data["value"]["server_list"])

        return (None, None)


    def insert(self, key, value, class_name=None):
        ######################
        # Insert API for client
        # @ This API is for insert or first put for a particular key.
        # @ We assume that client has this prior knowledge if its put or insert.
        #
        # @ Returns dict with {"status" , "message" } keys
        ######################

        if not class_name:
            class_name = self.default_class

        total_attempts = self.retry_attempts

        # Step1 : Putting key, value as per provided class protocol
        while total_attempts:
            output = self.class_name_to_object[class_name].put(key, value, None, True)

            if self.check_validity(output):
                break

            total_attempts -= 1

        if not total_attempts:
            return {"status": "FAILURE",
                    "message": "Unable to insert message after {0} attempts".format(
                        self.class_properties[class_name]["retry_attemps"])}

        # Step2 : Insert the metadata into the metadata server
        total_attempts = self.retry_attempts
        server_list = output["server_list"]
        meta_data = {"class_name": class_name, "server_list": server_list}

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.metadata_server["host"], int(self.metadata_server["port"])))
        except:
            return {"status": "FAILURE",
                    "message": "Unable to connect the socket to metadata server"}

        data = json.dumps({"method": "set_metadata",
                           "key": key,
                           "value": meta_data})

        while total_attempts:
            sock.send(data.encode("utf-8"))

            # Setting up timeout as per class policies
            sock.settimeout(self.metadata_timeout)
            try:
                data = sock.recv(1024)
                data = json.loads(data.decode("utf-8"))
            except socket.error:
                print("Failed attempt to send data to metadata server...")
            else:
                if data["status"] == "OK":
                    # TODO: Check once if the key is required else remove it
                    self.local_cache[key] = meta_data
                    return {"status" : "OK"}

            total_attempts -= 1

        return {"status": "Failure",
                "message": "Unable to insert message after {0} attemps into the metadataserver".format(
                    self.class_properties[class_name]["retry_attemps"])}


    def put(self, key, value):
        ######################
        # Put API for client
        # @ This API is for put key and value based on assinged key.
        # @ Returns dict with {"status" , "message" } keys
        ######################

        # Step1: Look for the class details and server details for key.
        try:
            class_name, server_list = self.look_up_metadata(key)
        except Exception as e:
            return {"status": "FAILURE", "message" : "Timeout or socket error for getting metadata" + str(e)}

        if not class_name or not server_list:
            return {"status": "FAILURE", "message" : "Key doesn't exist in system"}

        # Step2: Call the relevant protocol for the same.
        total_attempts = self.retry_attempts
        while total_attempts:
            output = self.class_name_to_object[class_name].put(key, value, server_list)
            print(output)
            if self.check_validity(output):
                return {"status" : "OK"}

            total_attempts -= 1

        return {"status": "FAILURE",
                "message": "Unable to put message after {0} attemps".format(self.retry_attempts)}


    def get(self, key):
        ######################
        # Get API for client
        # @ This API is for get key based on assigned class.
        # @ Returns dict with {"status" , "value" } keys
        ######################

        # Step1: Get the relevant metadata for the key
        try:
            class_name, server_list = self.look_up_metadata(key)
        except Exception as e:
            return {"status": "FAILURE", "message" : "Timeout or socket error for getting metadata" + str(e)}

        if not class_name or not server_list:
            return {"status": "FAILURE", "message": "Key doesn't exist in system"}

        total_attempts = self.retry_attempts

        # Step2: call the relevant protocol for the same
        while total_attempts:
            output = self.class_name_to_object[class_name].get(key, server_list)

            if self.check_validity(output):
                return {"status": "OK", "value": output["value"]}

            total_attempts -= 1

        return {"status": "FAILURE",
                "message": "Unable to put message after {0} attemps".format(
                    self.class_properties[class_name]["retry_attemps"])}


def call(key, value):
    print(json.dumps(client.put(key, str(value))))


if __name__ == "__main__":

    properties = json.load(open('client_config.json'))
    client = Client(properties)

    while 1:
        print("\n")
        data = input()
        method, key, value = data.split(",")
        if method == "insert":
            print(json.dumps(client.insert(key, value, "ABD")))
        elif method == "put":
            print(json.dumps(client.put(key, value)))
        elif method == "get":
            print(json.dumps(client.get(key)))
        else:
            print("Invalid Call")



    #
    # threadlist = []
    # for i in range(1,100):
    #     threadlist.append(threading.Thread(target=call, args=("chetan", i,)))
    #     threadlist[-1].deamon = True
    #     threadlist[-1].start()
    #
    # for i in range(1,100):
    #     threadlist[i].join()
