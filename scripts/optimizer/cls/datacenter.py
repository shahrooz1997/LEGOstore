class DataCenter:
    def __init__(self, **kwargs):
        self.id = None
        self.latencies = []
        self.network_cost = None
        self.provider = None
        self.details = {}
