class PingPolicy(object):
    """ Defines how many servers to ping and how many to wait for"""
    def __init__(self):
        self.servers_to_ping = None
        self.servers_to_wait = None

    def fetch_metrics(self, number_of_servers, required_servers):
        raise NotImplementedError


class PingMinPolicy(PingPolicy):

    def fetch_metrics(self, number_of_servers, required_servers):
        self.servers_to_ping = required_servers
        self.servers_to_wait = required_servers

        return self.servers_to_ping, self.servers_to_wait


class PingAllPolicy(PingPolicy):

    def fetch_metrics(self, number_of_servers, required_servers):
        self.servers_to_ping = number_of_servers
        self.servers_to_wait = required_servers

        return self.servers_to_ping, self.servers_to_wait
