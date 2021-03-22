import json
from services import placement_service as ps
from constants import opt_consts as CONSTS

class PlacementOutput:
    def __init__(self, dcs=None, iq1=None, iq2=None, iq3=None, iq4=None, \
                 get_latency=None, put_latency=None, get_cost=None, put_cost=None, \
                 storage_cost=None, vm_cost=None, m=None, k=1):
        self.dcs = dcs
        self.iq1 = iq1
        self.iq2 = iq2
        self.iq3 = iq3
        self.iq4 = iq4
        self.get_latency = get_latency
        self.put_latency = put_latency
        self.get_cost = get_cost
        self.put_cost = put_cost
        self.storage_cost = storage_cost
        self.vm_cost = vm_cost
        self.m = m # code length
        self.k = k # code dimension

class PlacementBase:
    def __init__(self, **kwargs):
        self.outfile = kwargs['file_name']
        self.heuristic = kwargs['heuristic'] if kwargs['heuristic'] \
                                              else CONSTS.BRUTE_FORCE
        self.k = int(kwargs['k']) if kwargs['k'] is not None else None
        self.m = int(kwargs['m']) if kwargs['m'] is not None else None
        self.grp = kwargs['grp'] if kwargs['grp'] is not None else None
        self.groups = kwargs['groups']
        self.datacenters = kwargs['datacenters']
        self.verbose = True if kwargs['verbose'] else False
        self.placements = {}

    def find_placement(self):
        ps.get_placement(self, self.heuristic, self.m, self.k, self.verbose, self.grp, use_protocol_param=False)

    def write_output(self):
        with open(self.outfile, "w") as f:
            f.write(json.dumps(self.placements, sort_keys=False, indent=2))

class PlacementAbd(PlacementBase):
    def __init__(self, **kwargs):
        super(PlacementAbd, self).__init__(**kwargs)
        self.protocol = [CONSTS.ABD]
    
    def find_placement(self):
        ps.get_placement(self, self.heuristic, self.m, self.k, self.verbose, self.grp, use_protocol_param=True)

class PlacementCas(PlacementBase):
    def __init__(self, **kwargs):
        super(PlacementCas, self).__init__(**kwargs)
        self.protocol = [CONSTS.CAS]
    
    def find_placement(self):
        ps.get_placement(self, self.heuristic, self.m, self.k, self.verbose, self.grp, use_protocol_param=True)

class PlacementRep(PlacementBase):
    def __init__(self, **kwargs):
        super(PlacementRep, self).__init__(**kwargs)
        self.protocol = [CONSTS.ABD, CONSTS.REP]

    def find_placement(self):
        ps.get_placement(self, self.heuristic, self.m, self.k, self.verbose, self.grp, use_protocol_param=True)

class PlacementEC(PlacementBase):
    def __init__(self, **kwargs):
        super(PlacementEC, self).__init__(**kwargs)
        self.protocol = [CONSTS.CAS]

    def find_placement(self):
        ps.get_placement(self, self.heuristic, self.m, self.k, self.verbose, self.grp, use_protocol_param=True, only_EC=True)
