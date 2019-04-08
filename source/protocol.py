from ABD_Client import ABD_Client
from CAS_Client import CAS_Client

class Protocol(object):
    """ Defines how many servers to ping and how many to wait for"""
    def __init__(self):
        pass

    @staticmethod
    def get_class_protocol(class_name, properties, local_data_center, data_center, id):
        if class_name == "ABD":
            return ABD_Client(properties, local_data_center, data_center, id)
        if class_name == "CAS":
            return CAS_Client(properties, local_data_center, data_center, id)
