class Group:
    def __init__(self, **kwargs):
        self.availability_target = None
        self.client_dist = []
        self.object_size = None
        self.metadata_size = None
        self.num_objects = None
        self.arrival_rate = None
        self.read_ratio = None
        self.write_ratio = None
        self.slo_read = None
        self.slo_write = None
        self.duration = None
        self.beta = None
        self.time_to_decode = None
