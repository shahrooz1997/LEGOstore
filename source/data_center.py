class data_center:
    # Each data center wil store the information about the servers it has and also periodically checks the health of servers in it.
    def __init__(self):
        self.data_center_list = json.load(open('config.json'))["data_centers"]
        self.id_to_data_list = {}
        for data_center in self.data_center_list:
            self.id_to_data_center_map[data_center["id"]] = data_center
            if (data_center["host"] == "localhost"):
                self.id = data_center["id"]

    def add_server():

    def remove_server():

    # Get the MC Router relevant to the server
    def get_mc_router():


    def get_current_data_center_id():
        return self.id

    def get_data_center_with_next():
        return self.id_to_data_center_map[self.id]
