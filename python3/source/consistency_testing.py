import random
from multiprocessing import Process, Lock
from Client import Client
import json
import time

#import testing_server_wrapper

def run_client(key, file_name, lock, number_of_operations, _id):
    list_ = ["read", "write"]
    
    properties = json.load(open('client_config.json'))
    client = Client(properties, _id)

# {:process 3, :type :invoke, :f :read, :value nil}
# {:process 3, :type :ok, :f :read, :value 4}
# {:process 5, :type :invoke, :f :write, :value 2}
# {:process 5, :type :ok, :f :write, :value 2}

# {:type :invoke,
# :f :txn,
# :value [[:read :x nil]],
# :process 3,
# :time 7686455705}

    while number_of_operations:
        operation = random.choice(list_)
        if operation == "read":
            value = 'nil'
        else:
            value = str(random.uniform(1, 10000000))

        data = '{:type :invoke, :f :' + operation + ', :value ' + value + ', :process ' + client.id + ', :time ' + str(int(time.time() * 100000)) + '}\n'
#        data = '{:type :invoke, :f :txn, :value [[:' + operation + ' :x '+ value + ']], :process ' + client.id + ', :time ' + str(int(time.time())) + '}\n'
        lock.acquire()
        file_name = open("testing.edn", "a")
        file_name.write(data)
        file_name.close()

        lock.release()

        if operation == "write":
            result = client.put(key, value)
            if result["status"] == "OK":
                type_ = "ok"
            else:
                type_ = "fail"
        else:	
            result = client.get(key)
            if result["status"] == "OK":
                type_ = "ok"
                value = result["value"]
            else:
                type_ = "fail"
        
        data = '{:type :' + type_ + ', :f :' + operation + ', :value ' + value + ', :process ' + client.id + ', :time ' + str(int(time.time() * 100000 )) + '}\n'
        #data = '{:type :' + type_ + ', :f :txn, :value [[:' + operation + ':x '+ value + ']], :process ' + client.id + ', :time ' + str(int(time.time())) + '}\n'
            
        lock.acquire()

        file_name = open("testing.edn", "a")
        file_name.write(data)
        file_name.close()

        lock.release()
#        time.sleep(1)
        number_of_operations -=1 

    file_name.close()
    return



if __name__ == "__main__":

    # Configs = [(total_servers, quorum_servers, n, k)]
#    possible_configs = [(3, 2, 2, 1), (5, 3, 3, 2), (10, 6, 6, 4)]
#    
    properties = json.load(open('client_config.json'))
    datacenter_id = properties["local_datacenter"]
#    starting_server_address = 20000    
#
#    for total_servers, quorum_servers, n, k in possible_configs:  
#        data_server_process = Process(target=testing_server_wrapper.start_data_server, 
#                                      args=(starting_server_address, total_servers))
#
#        data_server_process.start()
#    
#        meta_data_process = Process(target=testing_server_wrapper.start_metadata_server, args=())
#        meta_data_process.start()
#
#        properties["datacenters"]["1"]["servers"] = {}
#        for i in ragne(starting_

    client = Client(properties)
    result = client.insert("data1", "nil", "Viveck_1")
    file_name = open("testing.edn", "w")
    process_list = []
    lock = Lock()

    for i in range(0,10):
        process_list.append(Process(target=run_client, args=("data1", file_name, lock, 100, datacenter_id + str(i))))

    for process in process_list:
        process.start()

    for process in process_list:
        process.join()

    file_name.close()
