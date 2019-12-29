from ping_policy import PingAllPolicy, PingMinPolicy


class Quorum(object):
    def __init__(self, config):
        self.config = config

    def get_ping_policy(self):
        if self.config["type"] == "PingAll":
            return PingAllPolicy()

        if self.config["type"] == "PingMin":
            return PingMinPolicy()

        raise Exception("Quorum Policy not found")