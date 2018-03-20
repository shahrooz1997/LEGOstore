class PingAllPolicy:
    def servers_to_ping(number_of_servers, required_servers):
        servers_to_ping = number_of_servers
        servers_to_wait = required_servers

        return [servers_to_ping, servers_to_wait]
