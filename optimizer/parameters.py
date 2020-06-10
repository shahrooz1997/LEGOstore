import copy

class single_group:
    def __init__(self,
      availability_target,
      client_dist,
      object_size,
      metadata_size,
      num_objects,
      arrival_rate,
      read_ratio,
      write_ratio,
      SLO_read,
      SLO_write,
      duration,
      time_to_decode):
        
      
      self.availability_target  = availability_target 
      self.client_dist          = client_dist 
      self.object_size          = object_size 
      self.metadata_size        = metadata_size
      self.num_objects          = num_objects
      self.arrival_rate         = arrival_rate
      self.read_ratio           = read_ratio
      self.write_ratio          = write_ratio
      self.SLO_read             = SLO_read
      self.SLO_write            = SLO_write
      self.duration             = duration
      self.time_to_decode       = time_to_decode


# vary arrival rate and return the input groups
def vary_arrival_rate(init_group, delta, start, end):
    input_groups = []
    for _lambda in range(start,end, delta):
        init_group.arrival_rate = _lambda
        input_groups.append(copy.deepcopy(init_group.__dict__))
    return input_groups

# vary object size and return the input groups
# note that start and end are in bytes,
#     and conversion to GB is needed to run on the
#     optimizer
def vary_object_size(init_group,delta, start, end):
    input_groups = []
    for _size in range(start, end, delta):
        init_group.object_size = _size/1e9
        input_groups.append(copy.deepcopy(init_group.__dict__))
    return input_groups

#vary object count and return the input groups
def vary_object_count(init_group,delta, start, end):
    input_groups = []
    for _count in range(start, end, delta):
        init_group.num_objects = _count
        input_groups.append(copy.deepcopy(init_group.__dict__))
    return input_groups