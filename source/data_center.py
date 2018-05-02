import json
from mc_wrapper import MCWrapper

class Datacenter:
    # Each data center wil store the information about the servers it has and also periodically checks the health of servers in it.
    def __init__(self, datacenter_list):
        self.data_center_list = datacenter_list


    def add_server(self):
        pass


    def get_datacenter_list(self):
        return self.data_center_list


    def get_server_info(self, data_center_id, server_id):
        return self.data_center_list[data_center_id]["servers"][server_id]


    def remove_server(self):
        pass

    #
    # # Get the MC Router relevant to the server
    # def get_mc_router(self):
    #     pass
    #
    #
    # def get_current_data_center_id(self):
    #     return self.id
    #
    #
    # def get_data_center_with_id(self, id):
    #     return self.id_to_data_center_map[id]