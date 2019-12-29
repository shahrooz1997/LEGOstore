#XXX: Depriciated now with new design not used anymore
import threading

class Key:
    def __init__(self, key_class, server_list):
        #XXX: Local server list is not used currently but I just kept it for future.
        self.local_server_list = []
        self.server_list = server_list
        self.key_class = key_class
        self.external_server_list_lock = threading.lock()
        self.internal_server_list_lock = threading.lock()


    def get_local_server_list(self):
        return self.local_server_list


    def get_dimensions(self):
        return self.dimension


    def get_server_list(self):
        return self.server_list


    def update_server_list(self, id):
        self.external_server_list_lock.acquire()
        #Todo: Make it thread safe per key wise, not sure if required but still marking for now
        if id in self.server_list:
            self.server_list[id] = self.server_list[id] + 1
        else:
            self.server_list[id] = 1

        self.external_server_list_lock.release()
        return


    def update_local_server_list(self, id):
        self.internal_server_list_lock.acquire()
        self.local_server_list.append(id)
        self.internal_server_list_lock.release()
        return


    def set_dimensions(self, dimensions):
        self.set_dimensions = dimensions
        return
