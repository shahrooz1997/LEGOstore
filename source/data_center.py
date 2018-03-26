import json
from mc_wrapper import MCWrapper

class Datacenter:
    # Each data center wil store the information about the servers it has and also periodically checks the health of servers in it.
    def __init__(self):
        self.data_center_list = json.load(open('config.json'))["data_centers"]
        self.id_to_data_list = {}
        for data_center in self.data_center_list:
            self.id_to_data_center_map[data_center["id"]] = data_center
            if (data_center["host"] == "localhost"):
                self.id = data_center["id"]

        self.mc_wapper = MCWrapper(json.load(open('config.json')), self)


    def add_server(self):
        pass


    def get_mc_wapper(self):
        return self.mc_wapper


    def remove_server(self):
        pass

    # Get the MC Router relevant to the server
    def get_mc_router(self):
        pass

    def get_current_data_center_id(self):
        return self.id

    def get_data_center_with_id(self, id):
        return self.id_to_data_center_map[id]