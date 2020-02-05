import json
import sys
import matplotlib.pyplot as plt
import os
import time

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

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("USAGE: python3 generate_input.py <path>")
        exit(1)
    
    filename = sys.argv[1]
   
    availability_target  = 2 
    client_dist          = [0.9, 0.025, 0.025, 0.0, 0.0, 0.0, 0.0, 0.025, 0.025]    
    object_size          = 0.000001
    metadata_size        = 0.000000001
    num_objects          = 1000
    arrival_rate         = 50
    read_ratio           = 0.968
    write_ratio          = 0.032
    SLO_read             = 1000
    SLO_write            = 1000
    duration             = 3600
    time_to_decode       = 0.00028

    granularity = 10

    input_groups = []
    for _lambda in range(450,651, granularity):
        arrival_rate = _lambda
        entry = single_group(
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
          time_to_decode)
        input_groups.append(entry.__dict__)

    json.dump({"input_groups": input_groups}, open("NSF_proposal.json","w"), indent=4)

    os.system("./NSF.sh")
 



