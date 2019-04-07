from adb_protocol import ABD
from viveck_1 import Viveck_1

class Protocol(object):
    """ Defines how many servers to ping and how many to wait for"""
    def __init__(self):
        pass

    @staticmethod
    def get_class_protocol(class_name, properties, local_data_center, data_center, id):
        if class_name == "ABD":
            return ABD(properties, local_data_center, data_center, id)
        if class_name == "CAS":
            return Viveck_1(properties, local_data_center, data_center, id)
