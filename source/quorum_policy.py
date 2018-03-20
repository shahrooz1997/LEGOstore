

class QuorumPolicy:
    def __init__(config):
        if config["type"] == "PingAll":
            return PingAllPolicy()

        if config["type"] == "PingMin":
            return PingMinPolicy()

        raise Exception("Quorum Policy not found")
