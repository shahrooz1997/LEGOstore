import threading

class Key:
    def __init__(self, dimension):
        self.local_server_list = []
        self.external_server_list = {}
        self.dimension = dimension
        self.external_server_list_lock = threading.lock()
        self.internal_server_list_lock = threading.lock()


    def get_local_server_list(self):
        return self.local_server_list


    def get_dimensions(self):
        return self.dimension


    def get_external_server_list(self):
        return self.external_server_list


    def update_external_server_list(self, id):
        self.external_server_list_lock.acquire()
        #Todo: Make it thread safe per key wise, not sure if required but still marking for now
        if id in self.external_server_list:
            self.external_server_list[id] = self.external_server_list[id] + 1
        else:
            self.external_server_list[id] = 1

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