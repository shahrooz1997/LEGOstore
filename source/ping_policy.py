class PingPolicy(object):
    def __init__(self, number_of_servers, required_servers):
        self.required_servers = required_servers
        self.number_of_servers = number_of_servers


class PingMinPolicy(PingPolicy):

    def server_to_ping(self):
        servers_to_ping = self.required_servers
        servers_to_wait = self.required_servers

        return servers_to_ping, servers_to_wait


class PingAllPolicy(PingPolicy):

    def server_to_ping(self):
        servers_to_ping = self.number_of_servers
        servers_to_wait = self.required_servers

        return servers_to_ping, servers_to_wait
