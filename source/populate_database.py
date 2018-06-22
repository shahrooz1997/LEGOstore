import json
from Client import Client

import threading
import random
import logging
import string
from datetime import datetime
import copy


def get_logger(log_path):
    logger_ = logging.getLogger('log')
    logger_.setLevel(logging.INFO)
    handler = logging.FileHandler(log_path)
    # XXX: TODO: Check if needed
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger_.addHandler(handler)
    return logger_

def thread_wrapper(output_logger, start_index, end_index, value_size=100000):
    # We will assume fixed class for wrapper for now

    for i in range(int(start_index), int(end_index)):
        value = ''.join(random.choice(string.ascii_uppercase)
                        for _ in range(value_size))
        output_logger.info("key:"+str(i)+json.dumps(client.insert("key"+str(i), copy.deepcopy(value), "Viveck_1")))

    return


if __name__ == "__main__":
    properties = json.load(open('client_config.json'))
    client_id = "1"
    client = Client(properties, properties["local_datacenter"])
    output_logger = get_logger("insert.log")

    keys_to_write = 110000
    start_index = keys_to_write * (int(client_id) - 1)
    end_index = start_index + keys_to_write
    number_of_thread = 20

    thread_list = []
    for i in range(0, number_of_thread):
        # Please not we assume client id starts from 1
        s_index = start_index + i * (keys_to_write/number_of_thread)
        e_index = start_index + (i + 1) * (keys_to_write/number_of_thread)

        thread_list.append(threading.Thread(target=thread_wrapper, args=(output_logger, s_index, e_index)))
        thread_list[-1].start()

    for i in range(0, number_of_thread):
        thread_list[i].join()
